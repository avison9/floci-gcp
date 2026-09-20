package io.floci.gcp.services.gke;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * {@code ClusterManager.UpdateNodePool} over REST ({@code PUT .../nodePools/{id}}), with the
 * request shapes gcloud sends for {@code container clusters upgrade --node-pool} and
 * {@code container node-pools update}.
 */
@QuarkusTest
class GkeUpdateNodePoolRestIntegrationTest {

    private static final String PROJECT = "gke-update-node-pool-it";
    private static final String LOCATION = "us-central1";
    private static final String BASE = "/container/v1/projects/" + PROJECT + "/locations/" + LOCATION;

    @Test
    void versionAliasesResolveToARealVersionOnThePool() {
        String cluster = "pool-upgrade";
        String poolPath = BASE + "/clusters/" + cluster + "/nodePools/default-pool";

        given()
                .contentType("application/json")
                .body("{\"cluster\":{\"name\":\"" + cluster + "\",\"initialClusterVersion\":\"1.29.0-gke.1\"}}")
                .when().post(BASE + "/clusters")
                .then()
                .statusCode(200);

        String advertised = given()
                .when().get(BASE + "/serverConfig")
                .then()
                .statusCode(200)
                .extract().path("defaultClusterVersion");

        given()
                .contentType("application/json")
                .body("{\"nodeVersion\":\"latest\"}")
                .when().put(poolPath)
                .then()
                .statusCode(200)
                .body("status", equalTo("DONE"));

        given()
                .when().get(poolPath)
                .then()
                .statusCode(200)
                .body("version", equalTo(advertised))
                .body("version", not(equalTo("latest")));

        // `gcloud container clusters upgrade C --node-pool default-pool` with no --cluster-version:
        // "-" means the cluster's master version.
        given()
                .contentType("application/json")
                .body("{\"nodeVersion\":\"-\"}")
                .when().put(poolPath)
                .then()
                .statusCode(200)
                .body("status", equalTo("DONE"));

        given()
                .when().get(poolPath)
                .then()
                .statusCode(200)
                .body("version", equalTo("1.29.0-gke.1"));
    }

    @Test
    void aNonStringVersionIsABadRequestNotAServerError() {
        String cluster = "pool-shape";
        String poolPath = BASE + "/clusters/" + cluster + "/nodePools/default-pool";

        given()
                .contentType("application/json")
                .body("{\"cluster\":{\"name\":\"" + cluster + "\"}}")
                .when().post(BASE + "/clusters")
                .then()
                .statusCode(200);

        String versionBefore = given()
                .when().get(poolPath)
                .then()
                .statusCode(200)
                .extract().path("version");

        for (String body : new String[] {"{\"nodeVersion\":123}", "{\"nodeVersion\":{\"v\":\"1\"}}", "{\"nodeVersion\":\"1\"}"}) {
            given()
                    .contentType("application/json")
                    .body(body)
                    .when().put(poolPath)
                    .then()
                    .statusCode(400)
                    .body("error.status", equalTo("INVALID_ARGUMENT"));
        }

        given()
                .when().get(poolPath)
                .then()
                .statusCode(200)
                .body("version", equalTo(versionBefore));
    }
}
