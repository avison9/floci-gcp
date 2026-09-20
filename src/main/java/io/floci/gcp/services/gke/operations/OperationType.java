package io.floci.gcp.services.gke.operations;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;

/**
 * The {@code container.v1.Operation.Type} members this emulator reports. The proto enum is closed
 * (cluster_service.proto {@code Operation.Type}, 19 values), and a generated client parses the
 * field into it, so every constant here must be one of them; {@link #RETIRED} is the only place
 * a name outside the proto may appear.
 */
public enum OperationType {
    CREATE_CLUSTER,
    DELETE_CLUSTER,
    UPGRADE_MASTER,
    UPGRADE_NODES,
    UPDATE_CLUSTER,
    CREATE_NODE_POOL,
    DELETE_NODE_POOL,
    SET_NODE_POOL_MANAGEMENT,
    SET_LABELS,
    SET_MASTER_AUTH,
    SET_NODE_POOL_SIZE,
    SET_NETWORK_POLICY,
    SET_MAINTENANCE_POLICY;

    /**
     * Names this emulator persisted before it was held to the proto enum (#228), read back as the
     * member the same RPC reports now. Without this a {@code gke-operations.json} holding one of
     * them fails to load and is quarantined, taking every persisted GKE operation with it.
     */
    private static final Map<String, OperationType> RETIRED = Map.ofEntries(
            Map.entry("UPDATE_NODE_POOL", UPGRADE_NODES),
            Map.entry("ROLLBACK_NODE_POOL_UPGRADE", UPGRADE_NODES),
            Map.entry("COMPLETE_NODE_POOL_UPGRADE", UPGRADE_NODES),
            Map.entry("SET_ADDONS_CONFIG", UPDATE_CLUSTER),
            Map.entry("SET_LOGGING_SERVICE", UPDATE_CLUSTER),
            Map.entry("SET_MONITORING_SERVICE", UPDATE_CLUSTER),
            Map.entry("SET_LOCATIONS", UPDATE_CLUSTER),
            Map.entry("SET_NODE_POOL_AUTOSCALING", UPDATE_CLUSTER),
            Map.entry("SET_LEGACY_ABAC", UPDATE_CLUSTER),
            Map.entry("START_IP_ROTATION", UPDATE_CLUSTER),
            Map.entry("COMPLETE_IP_ROTATION", UPDATE_CLUSTER));

    @JsonCreator
    public static OperationType fromJson(String name) {
        OperationType retired = RETIRED.get(name);
        return retired != null ? retired : valueOf(name);
    }
}
