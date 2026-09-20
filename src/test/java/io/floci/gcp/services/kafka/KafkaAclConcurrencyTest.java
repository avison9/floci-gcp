package io.floci.gcp.services.kafka;

import io.floci.gcp.config.EmulatorConfig;
import io.floci.gcp.core.common.GcpException;
import io.floci.gcp.core.storage.InMemoryStorage;
import io.floci.gcp.services.kafka.model.AclEntry;
import io.floci.gcp.services.kafka.model.ClusterState;
import io.floci.gcp.services.kafka.model.StoredAcl;
import io.floci.gcp.services.kafka.model.StoredCluster;
import io.floci.gcp.services.kafka.model.StoredConsumerGroup;
import io.floci.gcp.services.kafka.model.StoredTopic;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Invariant: an ACL exists only under a cluster that exists, and no ACL mutation loses another's
 * entry. Both are enforced by the per-cluster lock in {@link KafkaService}; these tests pin the
 * interleavings deterministically (latches inside the ACL store, no sleeps).
 */
class KafkaAclConcurrencyTest {

    private static final String PROJECT = "p";
    private static final String LOCATION = "us-central1";
    private static final String CLUSTER = "projects/p/locations/us-central1/clusters/main";

    private static KafkaService service(InMemoryStorage<String, StoredAcl> acls) {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.services().kafka().mock()).thenReturn(true);
        InMemoryStorage<String, StoredCluster> clusters = new InMemoryStorage<>();
        StoredCluster cluster = new StoredCluster(CLUSTER);
        cluster.setState(ClusterState.ACTIVE);
        clusters.put(CLUSTER, cluster);
        return new KafkaService(clusters, new InMemoryStorage<String, StoredTopic>(),
                new InMemoryStorage<String, StoredConsumerGroup>(), acls, config, null, null);
    }

    private static AclEntry entry(String principal) {
        return new AclEntry(principal, "ALLOW", "READ", "*");
    }

    @Test
    void clusterDeletionSerializesEveryAclOperationAndLeavesNoOrphan() throws Exception {
        BlockingStorage<String, StoredAcl> acls = new BlockingStorage<>(BlockingStorage.Op.DELETE);
        KafkaService tested = service(acls);
        tested.addAclEntry(PROJECT, LOCATION, "main", "topic/orders", entry("User:a"));

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> delete = executor.submit(() -> tested.deleteCluster(PROJECT, LOCATION, "main"));
            assertTrue(acls.awaitEntered(), "delete never reached the acl cascade");
            // Every ACL operation, including the ones that create an ACL when it is absent.
            List<Future<?>> blocked = List.of(
                    executor.submit(() -> tested.addAclEntry(PROJECT, LOCATION, "main", "topic/late", entry("User:b"))),
                    executor.submit(() -> tested.addAclEntry(PROJECT, LOCATION, "main", "topic/orders", entry("User:b"))),
                    executor.submit(() -> tested.removeAclEntry(PROJECT, LOCATION, "main", "topic/orders", entry("User:a"))),
                    executor.submit(() -> tested.createAcl(PROJECT, LOCATION, "main", "consumerGroup/g",
                            aclWith(entry("User:c")))),
                    executor.submit(() -> tested.updateAcl(PROJECT, LOCATION, "main", "topic/orders",
                            aclWith(entry("User:d")), null)),
                    executor.submit(() -> tested.deleteAcl(PROJECT, LOCATION, "main", "topic/orders")),
                    executor.submit(() -> tested.getAcl(PROJECT, LOCATION, "main", "topic/orders")),
                    executor.submit(() -> tested.listAcls(PROJECT, LOCATION, "main", 0, null)));
            assertTimeoutPreemptively(Duration.ofMillis(200),
                    () -> assertTrue(blocked.stream().noneMatch(Future::isDone)));

            acls.release();
            delete.get(5, TimeUnit.SECONDS);
            for (Future<?> operation : blocked) {
                assertNotFound(operation);
            }
            // Nothing submitted during the window was stored: no ACL survives its cluster.
            assertTrue(acls.scan(k -> true).isEmpty(), "an acl was orphaned by the deletion race");
        }
    }

    @Test
    void concurrentEntryMutationsOnOneAclNeverLoseAWrite() throws Exception {
        BlockingStorage<String, StoredAcl> acls = new BlockingStorage<>(BlockingStorage.Op.PUT);
        KafkaService tested = service(acls);
        // The first put is the one the test blocks, so create the ACL through a path whose put
        // happens before the contended pair.
        acls.passThroughNextPut();
        tested.createAcl(PROJECT, LOCATION, "main", "topic/orders", aclWith(entry("User:a")));

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            // First add reads the ACL, appends, and blocks inside put while holding the cluster lock.
            Future<?> first = executor.submit(
                    () -> tested.addAclEntry(PROJECT, LOCATION, "main", "topic/orders", entry("User:b")));
            assertTrue(acls.awaitEntered(), "first add never reached put");
            // The second add must not have read the ACL yet; unlocked it would have, and its put
            // would overwrite the first one's entry.
            Future<?> second = executor.submit(
                    () -> tested.addAclEntry(PROJECT, LOCATION, "main", "topic/orders", entry("User:c")));
            Future<?> remove = executor.submit(
                    () -> tested.removeAclEntry(PROJECT, LOCATION, "main", "topic/orders", entry("User:a")));
            assertTimeoutPreemptively(Duration.ofMillis(200), () -> {
                assertFalse(second.isDone());
                assertFalse(remove.isDone());
            });

            acls.release();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            remove.get(5, TimeUnit.SECONDS);
        }

        StoredAcl acl = tested.getAcl(PROJECT, LOCATION, "main", "topic/orders");
        assertEquals(List.of("User:b", "User:c"),
                acl.getAclEntries().stream().map(AclEntry::getPrincipal).sorted().toList());
    }

    private static StoredAcl aclWith(AclEntry... entries) {
        StoredAcl acl = new StoredAcl();
        acl.setAclEntries(List.of(entries));
        return acl;
    }

    private static void assertNotFound(Future<?> operation) throws Exception {
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> operation.get(5, TimeUnit.SECONDS));
        assertInstanceOf(GcpException.class, failure.getCause());
        assertEquals("NOT_FOUND", ((GcpException) failure.getCause()).getGcpStatus());
    }

    /** Blocks the first {@code put} or {@code delete} until released, so a test can hold one operation open mid-way. */
    private static final class BlockingStorage<K, V> extends InMemoryStorage<K, V> {
        enum Op { PUT, DELETE }

        private final Op blockOn;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private boolean armed = true;
        private boolean passThrough;

        private BlockingStorage(Op blockOn) {
            this.blockOn = blockOn;
        }

        private void passThroughNextPut() {
            passThrough = true;
        }

        @Override
        public void put(K key, V value) {
            if (blockOn == Op.PUT) {
                if (passThrough) {
                    passThrough = false;
                } else {
                    blockOnce();
                }
            }
            super.put(key, value);
        }

        @Override
        public void delete(K key) {
            if (blockOn == Op.DELETE) {
                blockOnce();
            }
            super.delete(key);
        }

        private void blockOnce() {
            if (!armed) {
                return;
            }
            armed = false;
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting for release");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }

        private boolean awaitEntered() throws InterruptedException {
            return entered.await(5, TimeUnit.SECONDS);
        }

        private void release() {
            release.countDown();
        }
    }
}
