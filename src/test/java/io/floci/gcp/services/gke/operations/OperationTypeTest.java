package io.floci.gcp.services.gke.operations;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationTypeTest {

    /**
     * {@code container.v1.Operation.Type}, cluster_service.proto at googleapis aa87617d67
     * (L3984-L4107). A closed enum: a generated client parses {@code operationType} into it, so a
     * name outside this set is a value no SDK can represent (#228).
     */
    private static final Set<String> PROTO_OPERATION_TYPES = Set.of(
            "TYPE_UNSPECIFIED", "CREATE_CLUSTER", "DELETE_CLUSTER", "UPGRADE_MASTER", "UPGRADE_NODES",
            "REPAIR_CLUSTER", "UPDATE_CLUSTER", "CREATE_NODE_POOL", "DELETE_NODE_POOL",
            "SET_NODE_POOL_MANAGEMENT", "AUTO_REPAIR_NODES", "AUTO_UPGRADE_NODES", "SET_LABELS",
            "SET_MASTER_AUTH", "SET_NODE_POOL_SIZE", "SET_NETWORK_POLICY", "SET_MAINTENANCE_POLICY",
            "RESIZE_CLUSTER", "FLEET_FEATURE_UPGRADE");

    private final ObjectMapper objectMapper = new ObjectMapper();

    @ParameterizedTest
    @EnumSource(OperationType.class)
    void everyConstantIsAnOperationTypeTheProtoDeclares(OperationType type) {
        assertTrue(PROTO_OPERATION_TYPES.contains(type.name()), type.name());
    }

    @ParameterizedTest
    @CsvSource({
            "UPDATE_NODE_POOL, UPGRADE_NODES",
            "ROLLBACK_NODE_POOL_UPGRADE, UPGRADE_NODES",
            "COMPLETE_NODE_POOL_UPGRADE, UPGRADE_NODES",
            "SET_ADDONS_CONFIG, UPDATE_CLUSTER",
            "SET_LOGGING_SERVICE, UPDATE_CLUSTER",
            "SET_MONITORING_SERVICE, UPDATE_CLUSTER",
            "SET_LOCATIONS, UPDATE_CLUSTER",
            "SET_NODE_POOL_AUTOSCALING, UPDATE_CLUSTER",
            "SET_LEGACY_ABAC, UPDATE_CLUSTER",
            "START_IP_ROTATION, UPDATE_CLUSTER",
            "COMPLETE_IP_ROTATION, UPDATE_CLUSTER",
    })
    void retiredNamesPersistedByEarlierBuildsStillLoad(String retired, OperationType expected) throws Exception {
        // The shape GkeOperationService persists as gke-operations.json. A name Jackson cannot map
        // would fail the whole file's load and quarantine it, losing every persisted operation.
        String json = "{\"operation-1\":{\"name\":\"operation-1\",\"operationType\":\"" + retired
                + "\",\"status\":\"DONE\",\"zone\":\"us-central1\",\"location\":\"us-central1\"}}";
        Map<String, StoredOperation> loaded = objectMapper.readValue(json,
                new TypeReference<Map<String, StoredOperation>>() {
                });

        assertEquals(expected, loaded.get("operation-1").getOperationType());
        // What clients read back is the member, never the retired spelling.
        assertEquals("\"" + expected.name() + "\"",
                objectMapper.writeValueAsString(loaded.get("operation-1").getOperationType()));
    }

    @Test
    void currentNamesRoundTripAndUnknownOnesStillFail() throws Exception {
        for (OperationType type : OperationType.values()) {
            assertEquals(type, objectMapper.readValue("\"" + type.name() + "\"", OperationType.class));
        }
        assertThrows(JsonMappingException.class,
                () -> objectMapper.readValue("\"NOT_A_TYPE\"", OperationType.class));
    }
}
