package io.floci.gcp.services.eventarc;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@QuarkusTest
class EventarcOperationsRestIntegrationTest {

    private static final String LOCATION = "us-central1";
    private static final String METADATA_TYPE = "type.googleapis.com/google.cloud.eventarc.v1.OperationMetadata";

    @Test
    void createTriggerOperationIsReadableAndListedUnderV1() {
        String project = "eventarc-ops-1";
        String operationName = createTrigger(project, LOCATION, "t1");

        given()
                .when().get("/v1/" + operationName)
                .then()
                .statusCode(200)
                .body("name", equalTo(operationName))
                .body("done", equalTo(true))
                .body("metadata.'@type'", equalTo(METADATA_TYPE))
                .body("metadata.target", equalTo(parent(project, LOCATION) + "/triggers/t1"))
                .body("response.name", equalTo(parent(project, LOCATION) + "/triggers/t1"));

        given()
                .when().get("/v1/" + parent(project, LOCATION) + "/operations")
                .then()
                .statusCode(200)
                .body("operations.name", hasItem(operationName));
    }

    @Test
    void listOnlyReturnsOperationsOfItsProjectAndLocation() {
        String operationName = createTrigger("eventarc-ops-2a", LOCATION, "t1");

        given()
                .when().get("/v1/" + parent("eventarc-ops-2b", LOCATION) + "/operations")
                .then()
                .statusCode(200)
                .body("operations", nullValue());

        given()
                .when().get("/v1/" + parent("eventarc-ops-2a", "europe-west1") + "/operations")
                .then()
                .statusCode(200)
                .body("operations", nullValue());

        given()
                .when().get("/v1/" + operationName.replace("eventarc-ops-2a", "eventarc-ops-2b"))
                .then()
                .statusCode(404)
                .body("error.status", equalTo("NOT_FOUND"));
    }

    @Test
    void listPagesThroughOperations() {
        String project = "eventarc-ops-3";
        createTrigger(project, LOCATION, "t1");
        createTrigger(project, LOCATION, "t2");

        String pageToken = given()
                .queryParam("pageSize", 1)
                .when().get("/v1/" + parent(project, LOCATION) + "/operations")
                .then()
                .statusCode(200)
                .body("operations", hasSize(1))
                .body("nextPageToken", notNullValue())
                .extract().path("nextPageToken");

        given()
                .queryParam("pageSize", 1)
                .queryParam("pageToken", pageToken)
                .when().get("/v1/" + parent(project, LOCATION) + "/operations")
                .then()
                .statusCode(200)
                .body("operations", hasSize(1))
                .body("nextPageToken", nullValue());
    }

    @Test
    void operationsOfOtherServicesAreNotServedAsEventarcOperations() {
        String project = "eventarc-ops-4";
        String functionOperation = given()
                .contentType("application/json")
                .queryParam("functionId", "fn")
                .body("{\"buildConfig\":{\"runtime\":\"java21\"}}")
                .when().post("/v2/" + parent(project, LOCATION) + "/functions")
                .then()
                .statusCode(200)
                .extract().path("name");
        String triggerOperation = createTrigger(project, LOCATION, "t1");

        given()
                .when().get("/v1/" + functionOperation)
                .then()
                .statusCode(404)
                .body("error.status", equalTo("NOT_FOUND"));

        given()
                .when().get("/v1/" + parent(project, LOCATION) + "/operations")
                .then()
                .statusCode(200)
                .body("operations.name", hasItem(triggerOperation))
                .body("operations.name", not(hasItem(functionOperation)));

        given()
                .when().delete("/v1/" + functionOperation)
                .then()
                .statusCode(404);

        given()
                .when().get("/v2/" + functionOperation)
                .then()
                .statusCode(200)
                .body("name", equalTo(functionOperation));
    }

    @Test
    void unknownOperationIsNotFoundInTheGcpErrorShape() {
        String name = parent("eventarc-ops-5", LOCATION) + "/operations/missing";

        given()
                .when().get("/v1/" + name)
                .then()
                .statusCode(404)
                .body("error.code", equalTo(404))
                .body("error.status", equalTo("NOT_FOUND"));

        given()
                .when().delete("/v1/" + name)
                .then()
                .statusCode(404)
                .body("error.status", equalTo("NOT_FOUND"));

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{}")
                .when().post("/v1/" + name + ":cancel")
                .then()
                .statusCode(404)
                .body("error.status", equalTo("NOT_FOUND"));
    }

    @Test
    void cancelLeavesADoneOperationUnchangedAndDeleteRemovesIt() {
        String project = "eventarc-ops-6";
        String operationName = createTrigger(project, LOCATION, "t1");

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body("{}")
                .when().post("/v1/" + operationName + ":cancel")
                .then()
                .statusCode(200)
                .body(equalTo("{}"));

        // gax-httpjson sends the empty CancelOperationRequest as an empty form body.
        given()
                .urlEncodingEnabled(false)
                .contentType("application/x-www-form-urlencoded")
                .when().post("/v1/" + operationName + ":cancel")
                .then()
                .statusCode(200)
                .body(equalTo("{}"));

        given()
                .when().get("/v1/" + operationName)
                .then()
                .statusCode(200)
                .body("done", equalTo(true))
                .body("error", nullValue())
                .body("response.name", equalTo(parent(project, LOCATION) + "/triggers/t1"));

        given()
                .when().delete("/v1/" + operationName)
                .then()
                .statusCode(200)
                .body(equalTo("{}"));

        given()
                .when().get("/v1/" + operationName)
                .then()
                .statusCode(404)
                .body("error.status", equalTo("NOT_FOUND"));

        given()
                .when().get("/v1/" + parent(project, LOCATION) + "/triggers/t1")
                .then()
                .statusCode(200);
    }

    private static String createTrigger(String project, String location, String triggerId) {
        return given()
                .contentType("application/json")
                .queryParam("triggerId", triggerId)
                .body("{\"destination\":{\"httpEndpoint\":{\"uri\":\"http://localhost:1/hook\"}},"
                        + "\"eventFilters\":[{\"attribute\":\"type\","
                        + "\"value\":\"google.cloud.pubsub.topic.v1.messagePublished\"}]}")
                .when().post("/v1/" + parent(project, location) + "/triggers")
                .then()
                .statusCode(200)
                .body("done", equalTo(true))
                .extract().path("name");
    }

    private static String parent(String project, String location) {
        return "projects/" + project + "/locations/" + location;
    }
}
