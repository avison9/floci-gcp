package io.floci.gcp.test;

import com.google.cloud.managedkafka.v1.CapacityConfig;
import com.google.cloud.managedkafka.v1.ConnectAccessConfig;
import com.google.cloud.managedkafka.v1.ConnectCluster;
import com.google.cloud.managedkafka.v1.ConnectGcpConfig;
import com.google.cloud.managedkafka.v1.ConnectNetworkConfig;
import com.google.cloud.managedkafka.v1.Connector;
import com.google.cloud.managedkafka.v1.ManagedKafkaConnectClient;
import com.google.cloud.managedkafka.v1.TaskRetryPolicy;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code ManagedKafkaConnect} (managedkafka.googleapis.com) compatibility via the official
 * google-cloud-managedkafka SDK over HttpJson: generated request serialization, routing of the
 * custom methods, LRO unpacking of the {@code ConnectCluster} response, and enum decoding. The
 * Kafka cluster the Connect cluster is attached to is created over HTTP, as {@link KafkaTest} does.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaConnectTest {

    private static final String PROJECT = TestFixtures.projectId();
    private static final String LOCATION = "us-central1";
    private static final String PARENT = "projects/" + PROJECT + "/locations/" + LOCATION;
    private static final String KAFKA_ID = TestFixtures.uniqueName("java-connect-kafka");
    private static final String KAFKA_CLUSTER = PARENT + "/clusters/" + KAFKA_ID;
    private static final String CONNECT_ID = TestFixtures.uniqueName("java-connect");
    private static final String CONNECT_CLUSTER = PARENT + "/connectClusters/" + CONNECT_ID;
    private static final String CONNECTOR = CONNECT_CLUSTER + "/connectors/gcs-sink";

    private static final HttpClient http = HttpClient.newHttpClient();
    private static ManagedKafkaConnectClient client;

    @BeforeAll
    static void setUp() throws Exception {
        HttpRequest create = HttpRequest.newBuilder()
                .uri(URI.create(TestFixtures.endpoint() + "/v1/" + PARENT + "/clusters?clusterId=" + KAFKA_ID))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"capacityConfig\":{\"vcpuCount\":3,\"memoryBytes\":3221225472}}"))
                .build();
        assertThat(http.send(create, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
        client = TestFixtures.kafkaConnectClient();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (client != null) {
            try {
                client.deleteConnectClusterAsync(CONNECT_CLUSTER).get();
            } catch (Exception ignored) {
            }
            client.close();
        }
        http.send(HttpRequest.newBuilder()
                .uri(URI.create(TestFixtures.endpoint() + "/v1/" + KAFKA_CLUSTER))
                .DELETE()
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    @Order(1)
    void createConnectClusterReturnsAnUnpackableDoneOperation() throws Exception {
        ConnectCluster requested = ConnectCluster.newBuilder()
                .setKafkaCluster(KAFKA_CLUSTER)
                .setCapacityConfig(CapacityConfig.newBuilder().setVcpuCount(12).setMemoryBytes(21474836480L))
                .setGcpConfig(ConnectGcpConfig.newBuilder().setAccessConfig(ConnectAccessConfig.newBuilder()
                        .addNetworkConfigs(ConnectNetworkConfig.newBuilder()
                                .setPrimarySubnet("projects/" + PROJECT + "/regions/us-central1/subnetworks/default"))))
                .putLabels("env", "compat")
                .build();

        ConnectCluster created = client.createConnectClusterAsync(PARENT, requested, CONNECT_ID).get();

        assertThat(created.getName()).isEqualTo(CONNECT_CLUSTER);
        assertThat(created.getKafkaCluster()).isEqualTo(KAFKA_CLUSTER);
        assertThat(created.getState()).isEqualTo(ConnectCluster.State.ACTIVE);
        assertThat(created.getCapacityConfig().getVcpuCount()).isEqualTo(12);
        assertThat(created.getCapacityConfig().getMemoryBytes()).isEqualTo(21474836480L);
        assertThat(created.getGcpConfig().getAccessConfig().getNetworkConfigs(0).getPrimarySubnet())
                .endsWith("/subnetworks/default");
        assertThat(created.getLabelsMap()).containsEntry("env", "compat");
        assertThat(created.hasCreateTime()).isTrue();
    }

    @Test
    @Order(2)
    void getAndListConnectClusters() {
        ConnectCluster cluster = client.getConnectCluster(CONNECT_CLUSTER);
        assertThat(cluster.getState()).isEqualTo(ConnectCluster.State.ACTIVE);

        assertThat(client.listConnectClusters(PARENT).iterateAll())
                .extracting(ConnectCluster::getName).contains(CONNECT_CLUSTER);
    }

    @Test
    @Order(3)
    void updateConnectClusterHonoursTheFieldMask() throws Exception {
        ConnectCluster update = ConnectCluster.newBuilder()
                .setName(CONNECT_CLUSTER)
                .putLabels("env", "compat")
                .putLabels("team", "data")
                .setCapacityConfig(CapacityConfig.newBuilder().setVcpuCount(24).setMemoryBytes(1))
                .build();

        ConnectCluster updated = client.updateConnectClusterAsync(update,
                FieldMask.newBuilder().addPaths("labels").build()).get();

        assertThat(updated.getLabelsMap()).containsEntry("team", "data");
        // capacityConfig was outside the mask and is unchanged.
        assertThat(updated.getCapacityConfig().getVcpuCount()).isEqualTo(12);
    }

    @Test
    @Order(4)
    void connectorLifecycleThroughTheGeneratedClient() {
        Connector requested = Connector.newBuilder()
                .putConfigs("connector.class", "io.aiven.kafka.connect.gcs.GcsSinkConnector")
                .putConfigs("topics", "orders")
                .putConfigs("tasks.max", "1")
                .setTaskRestartPolicy(TaskRetryPolicy.newBuilder()
                        .setMinimumBackoff(Duration.newBuilder().setSeconds(60))
                        .setMaximumBackoff(Duration.newBuilder().setSeconds(1800)))
                .build();

        Connector created = client.createConnector(CONNECT_CLUSTER, requested, "gcs-sink");
        assertThat(created.getName()).isEqualTo(CONNECTOR);
        assertThat(created.getState()).isEqualTo(Connector.State.RUNNING);
        assertThat(created.getConfigsMap()).containsEntry("topics", "orders");
        assertThat(created.getTaskRestartPolicy().getMinimumBackoff().getSeconds()).isEqualTo(60);

        // The custom methods: the SDK posts to `.../connectors/gcs-sink:pause` with an empty body.
        client.pauseConnector(CONNECTOR);
        assertThat(client.getConnector(CONNECTOR).getState()).isEqualTo(Connector.State.PAUSED);
        client.resumeConnector(CONNECTOR);
        assertThat(client.getConnector(CONNECTOR).getState()).isEqualTo(Connector.State.RUNNING);
        client.stopConnector(CONNECTOR);
        assertThat(client.getConnector(CONNECTOR).getState()).isEqualTo(Connector.State.STOPPED);
        client.restartConnector(CONNECTOR);
        assertThat(client.getConnector(CONNECTOR).getState()).isEqualTo(Connector.State.RUNNING);

        // A nested mask path touches one backoff and keeps the other.
        Connector updated = client.updateConnector(Connector.newBuilder()
                        .setName(CONNECTOR)
                        .setTaskRestartPolicy(TaskRetryPolicy.newBuilder()
                                .setMinimumBackoff(Duration.newBuilder().setSeconds(5)))
                        .build(),
                FieldMask.newBuilder().addPaths("task_restart_policy.minimum_backoff").build());
        assertThat(updated.getTaskRestartPolicy().getMinimumBackoff().getSeconds()).isEqualTo(5);
        assertThat(updated.getTaskRestartPolicy().getMaximumBackoff().getSeconds()).isEqualTo(1800);

        assertThat(client.listConnectors(CONNECT_CLUSTER).iterateAll())
                .extracting(Connector::getName).containsExactly(CONNECTOR);
        client.deleteConnector(CONNECTOR);
        assertThat(client.listConnectors(CONNECT_CLUSTER).iterateAll()).isEmpty();
    }

    @Test
    @Order(5)
    void deleteConnectClusterReturnsADoneOperation() throws Exception {
        client.deleteConnectClusterAsync(CONNECT_CLUSTER).get();

        assertThat(client.listConnectClusters(PARENT).iterateAll())
                .extracting(ConnectCluster::getName).doesNotContain(CONNECT_CLUSTER);
    }
}
