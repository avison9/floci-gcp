package io.floci.gcp.services.kafka;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * {@code ManagedKafkaConnect} over REST: the LRO envelope on Connect cluster mutations, the
 * resource shapes read back, {@code updateMask}, and the four connector lifecycle methods with
 * their empty responses.
 */
@QuarkusTest
class KafkaConnectRestIntegrationTest {

    private static final String PROJECT = "kafka-connect-it";
    private static final String LOCATION = "us-central1";
    private static final String BASE = "/v1/projects/" + PROJECT + "/locations/" + LOCATION;

    private static String kafkaCluster(String id) {
        given()
                .contentType("application/json")
                .queryParam("clusterId", id)
                .body("{\"capacityConfig\":{\"vcpuCount\":3,\"memoryBytes\":3221225472}}")
                .when().post(BASE + "/clusters")
                .then()
                .statusCode(200);
        return "projects/" + PROJECT + "/locations/" + LOCATION + "/clusters/" + id;
    }

    /** {@code urlEncodingEnabled(false)}: RestAssured would otherwise encode the {@code :} of the custom method. */
    private static io.restassured.response.ValidatableResponse lifecycle(String path) {
        return given().urlEncodingEnabled(false).contentType("application/json").body("{}").when().post(path).then();
    }

    private static String connectClusterBody(String kafkaCluster) {
        return "{\"kafkaCluster\":\"" + kafkaCluster + "\","
                + "\"capacityConfig\":{\"vcpuCount\":12,\"memoryBytes\":\"21474836480\"},"
                + "\"gcpConfig\":{\"accessConfig\":{\"networkConfigs\":[{\"primarySubnet\":"
                + "\"projects/" + PROJECT + "/regions/us-central1/subnetworks/default\"}]}},"
                + "\"labels\":{\"env\":\"dev\"}}";
    }

    @Test
    void connectClusterLifecycleFollowsTheManagedKafkaContract() {
        String kafka = kafkaCluster("main");
        String connectPath = BASE + "/connectClusters/cc";
        String connectName = "projects/" + PROJECT + "/locations/" + LOCATION + "/connectClusters/cc";

        // create: an already-done LRO whose response is the ConnectCluster, ACTIVE right away
        given()
                .contentType("application/json")
                .queryParam("connectClusterId", "cc")
                .body(connectClusterBody(kafka))
                .when().post(BASE + "/connectClusters")
                .then()
                .statusCode(200)
                .body("name", startsWith("projects/" + PROJECT + "/locations/" + LOCATION + "/operations/"))
                .body("done", equalTo(true))
                .body("response.name", equalTo(connectName))
                .body("response.kafkaCluster", equalTo(kafka))
                .body("response.state", equalTo("ACTIVE"))
                .body("response.capacityConfig.vcpuCount", equalTo(12))
                .body("response.gcpConfig.accessConfig.networkConfigs[0].primarySubnet",
                        equalTo("projects/" + PROJECT + "/regions/us-central1/subnetworks/default"))
                .body("response.labels.env", equalTo("dev"))
                .body("response.createTime", notNullValue());

        given()
                .when().get(connectPath)
                .then()
                .statusCode(200)
                .body("name", equalTo(connectName))
                .body("state", equalTo("ACTIVE"));

        given()
                .when().get(BASE + "/connectClusters")
                .then()
                .statusCode(200)
                .body("connectClusters", hasSize(1))
                .body("connectClusters[0].name", equalTo(connectName))
                .body("$", not(hasKey("nextPageToken")));

        // the referenced Kafka cluster must exist in this project and location
        given()
                .contentType("application/json")
                .queryParam("connectClusterId", "orphan")
                .body(connectClusterBody("projects/" + PROJECT + "/locations/" + LOCATION + "/clusters/nope"))
                .when().post(BASE + "/connectClusters")
                .then()
                .statusCode(404)
                .body("error.status", equalTo("NOT_FOUND"));
        given()
                .contentType("application/json")
                .queryParam("connectClusterId", "far")
                .body(connectClusterBody("projects/" + PROJECT + "/locations/europe-west1/clusters/main"))
                .when().post(BASE + "/connectClusters")
                .then()
                .statusCode(400)
                .body("error.status", equalTo("INVALID_ARGUMENT"));
        given()
                .contentType("application/json")
                .body(connectClusterBody(kafka))
                .when().post(BASE + "/connectClusters")
                .then()
                .statusCode(400)
                .body("error.message", equalTo("connectClusterId query parameter is required"));

        // update: PATCH with updateMask, wrapped in the same LRO envelope
        given()
                .contentType("application/json")
                .queryParam("updateMask", "labels,capacityConfig")
                .body("{\"labels\":{\"env\":\"prod\"},\"capacityConfig\":{\"vcpuCount\":24,\"memoryBytes\":42949672960},"
                        + "\"kafkaCluster\":\"" + kafka + "\"}")
                .when().patch(connectPath)
                .then()
                .statusCode(200)
                .body("done", equalTo(true))
                .body("response.labels.env", equalTo("prod"))
                .body("response.capacityConfig.vcpuCount", equalTo(24));
        given()
                .contentType("application/json")
                .queryParam("updateMask", "kafkaCluster")
                .body("{\"kafkaCluster\":\"" + kafka + "\"}")
                .when().patch(connectPath)
                .then()
                .statusCode(400)
                .body("error.status", equalTo("INVALID_ARGUMENT"));

        // connectors
        given()
                .contentType("application/json")
                .queryParam("connectorId", "gcs-sink")
                .body("{\"configs\":{\"connector.class\":\"io.aiven.kafka.connect.gcs.GcsSinkConnector\","
                        + "\"topics\":\"orders\",\"tasks.max\":\"1\"},"
                        + "\"taskRestartPolicy\":{\"minimumBackoff\":\"60s\",\"maximumBackoff\":\"1800s\"}}")
                .when().post(connectPath + "/connectors")
                .then()
                .statusCode(200)
                .body("name", equalTo(connectName + "/connectors/gcs-sink"))
                .body("state", equalTo("RUNNING"))
                .body("configs.topics", equalTo("orders"))
                .body("taskRestartPolicy.minimumBackoff", equalTo("60s"));

        given()
                .contentType("application/json")
                .queryParam("connectorId", "mm2")
                .body("{\"configs\":{\"connector.class\":\"MirrorSourceConnector\"}}")
                .when().post(connectPath + "/connectors")
                .then()
                .statusCode(200)
                .body("$", not(hasKey("taskRestartPolicy")));

        given()
                .when().get(connectPath + "/connectors")
                .then()
                .statusCode(200)
                .body("connectors.name", contains(connectName + "/connectors/gcs-sink", connectName + "/connectors/mm2"));

        // the lifecycle methods answer with their empty response messages and move state
        String connector = connectPath + "/connectors/gcs-sink";
        // (`body: "*"` on a request message holding only the name, so the SDK sends `{}`)
        lifecycle(connector + ":pause").statusCode(200).body("$", equalTo(java.util.Map.of()));
        given().when().get(connector).then().statusCode(200).body("state", equalTo("PAUSED"));
        lifecycle(connector + ":resume").statusCode(200);
        given().when().get(connector).then().statusCode(200).body("state", equalTo("RUNNING"));
        lifecycle(connector + ":stop").statusCode(200);
        given().when().get(connector).then().statusCode(200).body("state", equalTo("STOPPED"));
        lifecycle(connector + ":restart").statusCode(200);
        given().when().get(connector).then().statusCode(200).body("state", equalTo("RUNNING"));
        lifecycle(connectPath + "/connectors/nope:pause").statusCode(404).body("error.status", equalTo("NOT_FOUND"));

        given()
                .contentType("application/json")
                .queryParam("updateMask", "configs")
                .body("{\"configs\":{\"topics\":\"orders,returns\"}}")
                .when().patch(connector)
                .then()
                .statusCode(200)
                .body("configs.topics", equalTo("orders,returns"))
                .body("configs", not(hasKey("tasks.max")));

        // DeleteConnector returns Empty; deleting the Connect cluster cascades and returns an LRO
        given().when().delete(connectPath + "/connectors/mm2").then().statusCode(200).body("$", equalTo(java.util.Map.of()));
        given().when().get(connectPath + "/connectors").then().statusCode(200).body("connectors", hasSize(1));

        given()
                .when().delete(connectPath)
                .then()
                .statusCode(200)
                .body("done", equalTo(true))
                .body("response", equalTo(java.util.Map.of()));
        given().when().get(connectPath).then().statusCode(404);
        given().when().get(connector).then().statusCode(404);

        // the Kafka cluster itself is untouched
        given().when().get(BASE + "/clusters/main").then().statusCode(200).body("state", equalTo("ACTIVE"));
    }
}
