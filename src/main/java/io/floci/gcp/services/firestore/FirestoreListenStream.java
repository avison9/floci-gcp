package io.floci.gcp.services.firestore;

import com.google.firestore.v1.DocumentChange;
import com.google.firestore.v1.DocumentDelete;
import com.google.firestore.v1.DocumentRemove;
import com.google.firestore.v1.ListenResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.Target;
import com.google.firestore.v1.TargetChange;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.floci.gcp.core.common.GcpException;
import io.floci.gcp.services.firestore.model.StoredDocument;
import io.grpc.stub.StreamObserver;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * One {@code Firestore.Listen} call: its targets, and what each target last sent to the client.
 *
 * <p>Every method runs under {@link FirestoreService}'s write lock, which is what orders a target's
 * initial snapshot against concurrent commits and serializes all sends on the response observer.
 * The message sequence follows google/firestore/v1/firestore.proto ({@code TargetChange}): a target
 * is acknowledged with {@code ADD}, its documents follow, {@code CURRENT} marks it consistent, and a
 * {@code NO_CHANGE} with no target ids and a {@code read_time} marks a consistent snapshot of the
 * whole stream, which is what the SDKs raise a listener snapshot on.
 */
final class FirestoreListenStream {

    private final FirestoreService service;
    private final StreamObserver<ListenResponse> responseObserver;
    private final Map<Integer, ListenTarget> targets = new LinkedHashMap<>();
    private int nextAssignedId = 1;
    private boolean closed;

    FirestoreListenStream(FirestoreService service, StreamObserver<ListenResponse> responseObserver) {
        this.service = service;
        this.responseObserver = responseObserver;
    }

    boolean isClosed() {
        return closed;
    }

    /** Marks the stream closed and reports whether it was open. */
    boolean close() {
        boolean wasOpen = !closed;
        closed = true;
        targets.clear();
        return wasOpen;
    }

    void addTarget(Target target, Instant readTime) {
        if (target.getTargetId() < 0) {
            throw GcpException.invalidArgument("Target ID must be positive: " + target.getTargetId());
        }
        if (target.getTargetTypeCase() == Target.TargetTypeCase.TARGETTYPE_NOT_SET) {
            throw GcpException.invalidArgument("Target must specify a query or documents");
        }
        if (targets.containsKey(target.getTargetId())) {
            throw GcpException.invalidArgument("Target ID already exists: " + target.getTargetId());
        }
        int targetId = target.getTargetId() == 0 ? assignTargetId() : target.getTargetId();
        ListenTarget listenTarget = new ListenTarget(targetId, target);
        targets.put(targetId, listenTarget);

        send(targetChange(TargetChange.TargetChangeType.ADD, targetId));
        boolean resumed = !target.getResumeToken().isEmpty() || target.hasReadTime();
        if (resumed) {
            // Floci keeps no change history, so a resumed target starts over from its current state.
            send(targetChange(TargetChange.TargetChangeType.RESET, targetId)
                    .setResumeToken(resumeToken(readTime))
                    .setReadTime(toTimestamp(readTime)));
        }
        if (target.hasDocuments()) {
            for (String name : target.getDocuments().getDocumentsList()) {
                Optional<StoredDocument> doc = service.getDocument(name);
                if (doc.isPresent()) {
                    sendDocumentChange(listenTarget, doc.get());
                } else {
                    sendDocumentDelete(listenTarget, name, readTime);
                }
            }
        } else {
            for (StoredDocument doc : listenTarget.evaluate()) {
                sendDocumentChange(listenTarget, doc);
            }
        }
        send(targetChange(TargetChange.TargetChangeType.CURRENT, targetId)
                .setResumeToken(resumeToken(readTime))
                .setReadTime(toTimestamp(readTime)));
        sendGlobalSnapshot(readTime);
    }

    /** An unknown target ID is ignored, as Google's Firestore emulator does. */
    void removeTarget(int targetId) {
        if (targets.remove(targetId) != null) {
            send(targetChange(TargetChange.TargetChangeType.REMOVE, targetId));
        }
    }

    /**
     * Sends what changed for every target a committed write may have touched, then one consistent
     * snapshot for the stream. Targets are diffed against what they last sent, so documents that
     * left a query's results are reported whether they were deleted or merely stopped matching.
     */
    void publish(Collection<String> changedNames, Instant readTime) {
        boolean changed = false;
        for (ListenTarget target : targets.values()) {
            if (changedNames.stream().anyMatch(target.scope)) {
                changed |= publishTarget(target, readTime);
            }
        }
        if (changed) {
            sendGlobalSnapshot(readTime);
        }
    }

    private boolean publishTarget(ListenTarget target, Instant readTime) {
        boolean changed = false;
        Set<String> current = new HashSet<>();
        for (StoredDocument doc : target.evaluate()) {
            current.add(doc.getName());
            if (!doc.getUpdateTime().equals(target.sent.get(doc.getName()))) {
                sendDocumentChange(target, doc);
                changed = true;
            }
        }
        for (String name : new ArrayList<>(target.sent.keySet())) {
            if (current.contains(name)) {
                continue;
            }
            target.sent.remove(name);
            changed = true;
            if (service.getDocument(name).isPresent()) {
                send(ListenResponse.newBuilder()
                        .setDocumentRemove(DocumentRemove.newBuilder()
                                .setDocument(name)
                                .addRemovedTargetIds(target.id)
                                .setReadTime(toTimestamp(readTime)))
                        .build());
            } else {
                sendDocumentDelete(target, name, readTime);
            }
        }
        return changed;
    }

    private void sendDocumentChange(ListenTarget target, StoredDocument doc) {
        target.sent.put(doc.getName(), doc.getUpdateTime());
        send(ListenResponse.newBuilder()
                .setDocumentChange(DocumentChange.newBuilder()
                        .setDocument(FirestoreController.toProto(doc))
                        .addTargetIds(target.id))
                .build());
    }

    private void sendDocumentDelete(ListenTarget target, String name, Instant readTime) {
        send(ListenResponse.newBuilder()
                .setDocumentDelete(DocumentDelete.newBuilder()
                        .setDocument(name)
                        .addRemovedTargetIds(target.id)
                        .setReadTime(toTimestamp(readTime)))
                .build());
    }

    /** No target ids: the whole stream has reached a consistent snapshot at {@code readTime}. */
    private void sendGlobalSnapshot(Instant readTime) {
        send(TargetChange.newBuilder()
                .setTargetChangeType(TargetChange.TargetChangeType.NO_CHANGE)
                .setResumeToken(resumeToken(readTime))
                .setReadTime(toTimestamp(readTime)));
    }

    private void send(TargetChange.Builder change) {
        send(ListenResponse.newBuilder().setTargetChange(change).build());
    }

    private void send(ListenResponse response) {
        responseObserver.onNext(response);
    }

    private int assignTargetId() {
        while (targets.containsKey(nextAssignedId)) {
            nextAssignedId++;
        }
        return nextAssignedId++;
    }

    private static TargetChange.Builder targetChange(TargetChange.TargetChangeType type, int targetId) {
        return TargetChange.newBuilder().setTargetChangeType(type).addTargetIds(targetId);
    }

    /** An opaque token naming the snapshot; floci restarts a resumed target, so it only has to round-trip. */
    private static ByteString resumeToken(Instant readTime) {
        return ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES + Integer.BYTES)
                .putLong(readTime.getEpochSecond())
                .putInt(readTime.getNano())
                .array());
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    private final class ListenTarget {
        final int id;
        final Target target;
        final Predicate<String> scope;
        /** Document name to the update time last sent for it. */
        final Map<String, String> sent = new LinkedHashMap<>();

        ListenTarget(int id, Target target) {
            this.id = id;
            this.target = target;
            if (target.hasQuery()) {
                StructuredQuery query = target.getQuery().getStructuredQuery();
                this.scope = FirestoreService.inCollection(target.getQuery().getParent(),
                        query.getFromCount() > 0 ? query.getFrom(0)
                                : StructuredQuery.CollectionSelector.getDefaultInstance());
            } else {
                Set<String> names = new HashSet<>(target.getDocuments().getDocumentsList());
                this.scope = names::contains;
            }
        }

        List<StoredDocument> evaluate() {
            if (target.hasQuery()) {
                return service.runQuery(target.getQuery().getParent(), target.getQuery().getStructuredQuery());
            }
            List<StoredDocument> docs = new ArrayList<>();
            for (String name : target.getDocuments().getDocumentsList()) {
                service.getDocument(name).ifPresent(docs::add);
            }
            return docs;
        }
    }
}
