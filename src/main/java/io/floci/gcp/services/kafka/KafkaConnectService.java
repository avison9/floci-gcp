package io.floci.gcp.services.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import io.floci.gcp.core.common.GcpException;
import io.floci.gcp.core.common.PageToken;
import io.floci.gcp.core.storage.StorageBackend;
import io.floci.gcp.core.storage.StorageFactory;
import io.floci.gcp.services.kafka.model.ClusterState;
import io.floci.gcp.services.kafka.model.ConnectorState;
import io.floci.gcp.services.kafka.model.StoredConnectCluster;
import io.floci.gcp.services.kafka.model.StoredConnector;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Control plane for {@code google.cloud.managedkafka.v1.ManagedKafkaConnect}: Connect clusters
 * and their connectors as metadata, with the connector lifecycle methods flipping {@code state}.
 *
 * <p>No Connect runtime is started: a Connect cluster is {@code ACTIVE} as soon as it is created,
 * and connectors report {@code RUNNING}, {@code PAUSED} or {@code STOPPED} as the API moved them,
 * in mock and Docker mode alike. Attaching a Kafka Connect sidecar to the referenced cluster's
 * Redpanda container is a follow-up that changes none of this surface.
 *
 * <p>Invariant: a connector exists only under a Connect cluster that exists. Every operation on a
 * Connect cluster or one of its connectors runs under that cluster's lock (striped on the cluster
 * name), so a delete cannot interleave with a connector mutation and leave an orphan, and two
 * mutations of one connector cannot lose each other's write. The Kafka cluster a Connect cluster
 * references is checked at create only; a later {@code DeleteCluster} on the Kafka side is not
 * blocked in this phase, which is safe because nothing here dereferences that cluster afterwards
 * (see the class notes in the PR for the follow-up).
 */
@ApplicationScoped
public class KafkaConnectService {

    private static final Logger LOG = Logger.getLogger(KafkaConnectService.class);

    private static final Pattern KAFKA_CLUSTER_NAME =
            Pattern.compile("^projects/([^/]+)/locations/([^/]+)/clusters/([^/]+)$");
    /** {@code ConnectCluster} fields {@code update_mask} may name; {@code kafka_cluster} is IMMUTABLE. */
    private static final Set<String> CONNECT_CLUSTER_MUTABLE = Set.of("labels", "capacityConfig", "gcpConfig", "config");
    private static final Set<String> CONNECTOR_MUTABLE = Set.of("configs", "taskRestartPolicy");
    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int LOCK_STRIPES = 256;

    private final Object[] clusterLocks = createLocks();
    private final StorageBackend<String, StoredConnectCluster> connectClusterStore;
    private final StorageBackend<String, StoredConnector> connectorStore;
    private final Predicate<String> kafkaClusterExists;

    @Inject
    public KafkaConnectService(StorageFactory storageFactory, KafkaService kafkaService) {
        this(storageFactory.createGlobal("kafka", "kafka-connect-clusters.json",
                        new TypeReference<Map<String, StoredConnectCluster>>() {}),
                storageFactory.createGlobal("kafka", "kafka-connectors.json",
                        new TypeReference<Map<String, StoredConnector>>() {}),
                kafkaService::clusterExists);
    }

    KafkaConnectService(StorageBackend<String, StoredConnectCluster> connectClusterStore,
                        StorageBackend<String, StoredConnector> connectorStore,
                        Predicate<String> kafkaClusterExists) {
        this.connectClusterStore = connectClusterStore;
        this.connectorStore = connectorStore;
        this.kafkaClusterExists = kafkaClusterExists;
    }

    // ── Connect clusters ──────────────────────────────────────────────────────

    public StoredConnectCluster createConnectCluster(String project, String location, String connectClusterId,
                                                     Map<String, Object> body) {
        String name = connectClusterName(project, location, connectClusterId);
        if (body == null) {
            throw GcpException.invalidArgument("Missing connectCluster body");
        }
        synchronized (clusterLock(name)) {
            if (connectClusterStore.get(name).isPresent()) {
                throw GcpException.alreadyExists("ConnectCluster already exists: " + name);
            }
            String kafkaCluster = requireKafkaCluster(project, location, body.get("kafkaCluster"));
            StoredConnectCluster cluster = new StoredConnectCluster(name, kafkaCluster);
            cluster.setCapacityConfig(requireCapacityConfig(body.get("capacityConfig")));
            cluster.setGcpConfig(requireGcpConfig(body.get("gcpConfig")));
            cluster.setLabels(stringMap(body.get("labels"), "labels"));
            cluster.setConfig(stringMap(body.get("config"), "config"));
            // Nothing is provisioned in this phase, so there is no CREATING window to report.
            cluster.setState(ClusterState.ACTIVE);
            connectClusterStore.put(name, cluster);
            LOG.infov("Created Kafka Connect cluster {0} on {1}", name, kafkaCluster);
            return cluster;
        }
    }

    public StoredConnectCluster getConnectCluster(String project, String location, String connectClusterId) {
        String name = connectClusterName(project, location, connectClusterId);
        synchronized (clusterLock(name)) {
            return requireConnectCluster(name);
        }
    }

    public PageToken.Page<StoredConnectCluster> listConnectClusters(String project, String location,
                                                                    Integer pageSize, String pageToken) {
        String prefix = "projects/" + project + "/locations/" + location + "/connectClusters/";
        List<StoredConnectCluster> all = connectClusterStore.scan(k -> k.startsWith(prefix)).stream()
                .sorted(Comparator.comparing(StoredConnectCluster::getName))
                .toList();
        return PageToken.paginate(all, pageSize(pageSize), pageToken);
    }

    public StoredConnectCluster updateConnectCluster(String project, String location, String connectClusterId,
                                                     String updateMask, Map<String, Object> body) {
        String name = connectClusterName(project, location, connectClusterId);
        Map<String, Object> update = body == null ? Map.of() : body;
        Set<String> fields = fieldsToApply(updateMask, update, CONNECT_CLUSTER_MUTABLE, "ConnectCluster");
        synchronized (clusterLock(name)) {
            StoredConnectCluster cluster = requireConnectCluster(name);
            Object requestedKafkaCluster = update.get("kafkaCluster");
            if (requestedKafkaCluster != null && !requestedKafkaCluster.equals(cluster.getKafkaCluster())) {
                throw GcpException.invalidArgument("kafkaCluster is immutable");
            }
            // Validate everything before the first setter, so a rejected body leaves the stored
            // object (the live reference in memory mode) exactly as it was.
            Map<String, String> labels = fields.contains("labels") ? stringMap(update.get("labels"), "labels") : null;
            Map<String, Object> capacity = fields.contains("capacityConfig")
                    ? requireCapacityConfig(update.get("capacityConfig")) : null;
            Map<String, Object> gcpConfig = fields.contains("gcpConfig") ? requireGcpConfig(update.get("gcpConfig")) : null;
            Map<String, String> config = fields.contains("config") ? stringMap(update.get("config"), "config") : null;
            if (fields.contains("labels")) {
                cluster.setLabels(labels);
            }
            if (fields.contains("capacityConfig")) {
                cluster.setCapacityConfig(capacity);
            }
            if (fields.contains("gcpConfig")) {
                cluster.setGcpConfig(gcpConfig);
            }
            if (fields.contains("config")) {
                cluster.setConfig(config);
            }
            cluster.setUpdateTime(Instant.now());
            connectClusterStore.put(name, cluster);
            return cluster;
        }
    }

    /** Connectors go first and the cluster last, all under the cluster lock, so no interleaving can
     * observe a cluster without its connectors' parent or a connector whose parent is gone. */
    public void deleteConnectCluster(String project, String location, String connectClusterId) {
        String name = connectClusterName(project, location, connectClusterId);
        synchronized (clusterLock(name)) {
            StoredConnectCluster cluster = requireConnectCluster(name);
            cluster.setState(ClusterState.DELETING);
            String connectorPrefix = name + "/connectors/";
            connectorStore.scan(k -> k.startsWith(connectorPrefix))
                    .forEach(c -> connectorStore.delete(c.getName()));
            connectClusterStore.delete(name);
            LOG.infov("Deleted Kafka Connect cluster {0}", name);
        }
    }

    // ── Connectors ────────────────────────────────────────────────────────────

    public StoredConnector createConnector(String project, String location, String connectClusterId,
                                           String connectorId, Map<String, Object> body) {
        String clusterName = connectClusterName(project, location, connectClusterId);
        String name = clusterName + "/connectors/" + connectorId;
        Map<String, Object> request = body == null ? Map.of() : body;
        StoredConnector connector = new StoredConnector(name);
        connector.setConfigs(stringMap(request.get("configs"), "configs"));
        connector.setTaskRestartPolicy(taskRestartPolicy(request.get("taskRestartPolicy")));
        synchronized (clusterLock(clusterName)) {
            requireConnectCluster(clusterName);
            if (connectorStore.get(name).isPresent()) {
                throw GcpException.alreadyExists("Connector already exists: " + name);
            }
            connectorStore.put(name, connector);
            return connector;
        }
    }

    public StoredConnector getConnector(String project, String location, String connectClusterId, String connectorId) {
        String clusterName = connectClusterName(project, location, connectClusterId);
        synchronized (clusterLock(clusterName)) {
            return requireConnector(clusterName + "/connectors/" + connectorId);
        }
    }

    public PageToken.Page<StoredConnector> listConnectors(String project, String location, String connectClusterId,
                                                          Integer pageSize, String pageToken) {
        String clusterName = connectClusterName(project, location, connectClusterId);
        String prefix = clusterName + "/connectors/";
        synchronized (clusterLock(clusterName)) {
            requireConnectCluster(clusterName);
            List<StoredConnector> all = connectorStore.scan(k -> k.startsWith(prefix)).stream()
                    .sorted(Comparator.comparing(StoredConnector::getName))
                    .toList();
            return PageToken.paginate(all, pageSize(pageSize), pageToken);
        }
    }

    public StoredConnector updateConnector(String project, String location, String connectClusterId,
                                           String connectorId, String updateMask, Map<String, Object> body) {
        String clusterName = connectClusterName(project, location, connectClusterId);
        String name = clusterName + "/connectors/" + connectorId;
        Map<String, Object> update = body == null ? Map.of() : body;
        Set<String> fields = fieldsToApply(updateMask, update, CONNECTOR_MUTABLE, "Connector");
        Map<String, String> configs = fields.contains("configs") ? stringMap(update.get("configs"), "configs") : null;
        Map<String, Object> policy = fields.contains("taskRestartPolicy")
                ? taskRestartPolicy(update.get("taskRestartPolicy")) : null;
        synchronized (clusterLock(clusterName)) {
            StoredConnector connector = requireConnector(name);
            if (fields.contains("configs")) {
                connector.setConfigs(configs);
            }
            if (fields.contains("taskRestartPolicy")) {
                connector.setTaskRestartPolicy(policy);
            }
            connectorStore.put(name, connector);
            return connector;
        }
    }

    public void deleteConnector(String project, String location, String connectClusterId, String connectorId) {
        String clusterName = connectClusterName(project, location, connectClusterId);
        String name = clusterName + "/connectors/" + connectorId;
        synchronized (clusterLock(clusterName)) {
            requireConnector(name);
            connectorStore.delete(name);
        }
    }

    /**
     * {@code PauseConnector}, {@code ResumeConnector}, {@code RestartConnector}, {@code StopConnector}.
     * A restart on a real runtime passes through {@code RESTARTING}; with nothing to restart here
     * the connector is {@code RUNNING} again by the time the response is written.
     */
    public StoredConnector transitionConnector(String project, String location, String connectClusterId,
                                               String connectorId, ConnectorState target) {
        String clusterName = connectClusterName(project, location, connectClusterId);
        String name = clusterName + "/connectors/" + connectorId;
        synchronized (clusterLock(clusterName)) {
            StoredConnector connector = requireConnector(name);
            connector.setState(target);
            connectorStore.put(name, connector);
            return connector;
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private static String connectClusterName(String project, String location, String connectClusterId) {
        return "projects/" + project + "/locations/" + location + "/connectClusters/" + connectClusterId;
    }

    /** Callers hold {@link #clusterLock} for the cluster. */
    private StoredConnectCluster requireConnectCluster(String name) {
        return connectClusterStore.get(name)
                .orElseThrow(() -> GcpException.notFound("ConnectCluster not found: " + name));
    }

    /** Callers hold {@link #clusterLock} for the connector's cluster. */
    private StoredConnector requireConnector(String name) {
        return connectorStore.get(name)
                .orElseThrow(() -> GcpException.notFound("Connector not found: " + name));
    }

    /** One lock per Connect cluster (striped), the only lock this service takes, so there is no order to preserve. */
    private Object clusterLock(String connectClusterName) {
        return clusterLocks[Math.floorMod(connectClusterName.hashCode(), clusterLocks.length)];
    }

    private static Object[] createLocks() {
        Object[] locks = new Object[LOCK_STRIPES];
        Arrays.setAll(locks, ignored -> new Object());
        return locks;
    }

    /**
     * {@code kafka_cluster} is REQUIRED and must name a Kafka cluster in the same project and
     * location; the real service pins a Connect cluster to a Kafka cluster in its own region.
     */
    private String requireKafkaCluster(String project, String location, Object value) {
        if (!(value instanceof String kafkaCluster) || kafkaCluster.isBlank()) {
            throw GcpException.invalidArgument("kafkaCluster is required");
        }
        Matcher m = KAFKA_CLUSTER_NAME.matcher(kafkaCluster);
        if (!m.matches()) {
            throw GcpException.invalidArgument("kafkaCluster must be a Kafka cluster resource name "
                    + "(projects/{project}/locations/{location}/clusters/{cluster}): " + kafkaCluster);
        }
        if (!project.equals(m.group(1)) || !location.equals(m.group(2))) {
            throw GcpException.invalidArgument("kafkaCluster must be in the same project and location "
                    + "as the ConnectCluster: " + kafkaCluster);
        }
        if (!kafkaClusterExists.test(kafkaCluster)) {
            throw GcpException.notFound("Cluster not found: " + kafkaCluster);
        }
        return kafkaCluster;
    }

    /** {@code capacity_config} is REQUIRED with both {@code vcpu_count} and {@code memory_bytes}. */
    private static Map<String, Object> requireCapacityConfig(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            throw GcpException.invalidArgument("capacityConfig is required");
        }
        Map<String, Object> capacity = new LinkedHashMap<>();
        capacity.put("vcpuCount", requirePositiveLong(map.get("vcpuCount"), "capacityConfig.vcpuCount"));
        capacity.put("memoryBytes", requirePositiveLong(map.get("memoryBytes"), "capacityConfig.memoryBytes"));
        return capacity;
    }

    /**
     * {@code gcp_config.access_config.network_configs} is REQUIRED and each entry needs a
     * {@code primary_subnet}; the rest of the config is kept verbatim for read-back fidelity.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> requireGcpConfig(Object value) {
        if (!(value instanceof Map<?, ?> gcpConfig)) {
            throw GcpException.invalidArgument("gcpConfig is required");
        }
        Object accessConfig = gcpConfig.get("accessConfig");
        Object networkConfigs = accessConfig instanceof Map<?, ?> a ? a.get("networkConfigs") : null;
        if (!(networkConfigs instanceof List<?> configs) || configs.isEmpty()) {
            throw GcpException.invalidArgument("gcpConfig.accessConfig.networkConfigs must have at least one entry");
        }
        for (Object entry : configs) {
            Object primarySubnet = entry instanceof Map<?, ?> e ? e.get("primarySubnet") : null;
            if (!(primarySubnet instanceof String s) || s.isBlank()) {
                throw GcpException.invalidArgument("gcpConfig.accessConfig.networkConfigs[].primarySubnet is required");
            }
        }
        return new LinkedHashMap<>((Map<String, Object>) gcpConfig);
    }

    /** A proto {@code map<string, string>}: absent is {@code null}, anything but string values is a 400. */
    private static Map<String, String> stringMap(Object value, String field) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw GcpException.invalidArgument(field + " must be an object of string values");
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getValue() instanceof String s)) {
                throw GcpException.invalidArgument(field + "." + entry.getKey() + " must be a string");
            }
            result.put(String.valueOf(entry.getKey()), s);
        }
        return result;
    }

    /** {@code TaskRetryPolicy}: two optional {@code google.protobuf.Duration} fields, {@code "60s"} on the wire. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> taskRestartPolicy(Object value) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw GcpException.invalidArgument("taskRestartPolicy must be an object");
        }
        for (String field : List.of("minimumBackoff", "maximumBackoff")) {
            Object duration = map.get(field);
            if (duration != null && !(duration instanceof String s && s.matches("^-?\\d+(\\.\\d+)?s$"))) {
                throw GcpException.invalidArgument("taskRestartPolicy." + field
                        + " must be a duration such as \"60s\"");
            }
        }
        return new LinkedHashMap<>((Map<String, Object>) map);
    }

    private static long requirePositiveLong(Object value, String field) {
        long parsed;
        if (value instanceof Number n) {
            parsed = n.longValue();
        } else if (value instanceof String s && s.matches("^\\d+$")) {
            // int64 is a JSON string in proto3 JSON; the SDKs send it that way.
            parsed = Long.parseLong(s);
        } else {
            throw GcpException.invalidArgument(field + " is required");
        }
        if (parsed <= 0) {
            throw GcpException.invalidArgument(field + " must be positive");
        }
        return parsed;
    }

    /**
     * The fields an update applies: those named by {@code update_mask} when one is sent, otherwise
     * whichever mutable fields the body carries (the same leniency the Kafka cluster update has).
     * A mask naming a field outside the mutable set is rejected rather than silently ignored, so
     * a client asking to change {@code kafkaCluster} learns that it is immutable.
     */
    private static Set<String> fieldsToApply(String updateMask, Map<String, Object> body,
                                             Set<String> mutable, String resource) {
        if (updateMask == null || updateMask.isBlank()) {
            return body.keySet().stream().filter(mutable::contains).collect(java.util.stream.Collectors.toSet());
        }
        Set<String> fields = Arrays.stream(updateMask.split(","))
                .map(String::trim)
                .filter(f -> !f.isEmpty())
                .map(f -> f.contains(".") ? f.substring(0, f.indexOf('.')) : f)
                .map(KafkaConnectService::lowerCamel)
                .collect(java.util.stream.Collectors.toSet());
        for (String field : fields) {
            if (!mutable.contains(field)) {
                throw GcpException.invalidArgument("updateMask names a field that cannot be updated on a "
                        + resource + ": " + field);
            }
        }
        return fields;
    }

    /** FieldMask paths are lowerCamelCase in proto3 JSON, but gcloud sends the proto spelling ({@code capacity_config}). */
    private static String lowerCamel(String path) {
        StringBuilder out = new StringBuilder();
        boolean upper = false;
        for (char c : path.toCharArray()) {
            if (c == '_') {
                upper = true;
            } else {
                out.append(upper ? Character.toUpperCase(c) : c);
                upper = false;
            }
        }
        return out.toString();
    }

    private static int pageSize(Integer pageSize) {
        return pageSize == null || pageSize <= 0 ? DEFAULT_PAGE_SIZE : pageSize;
    }
}
