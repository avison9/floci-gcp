package io.floci.gcp.services.cloudsql;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/** SQL Admin JSON shapes for a MySQL instance: system schemas, {@code root@%}, host-qualified users. */
@QuarkusTest
class CloudSqlMySqlRestIntegrationTest {

    @Test
    void mysqlInstanceDatabaseAndUserShapesFollowTheMysqlAdminApi() {
        String project = "sql-it-mysql";
        String instance = "my-main";
        String base = "/v1/projects/" + project;

        given()
                .contentType("application/json")
                .body("""
                        {
                          "name": "my-main",
                          "databaseVersion": "MYSQL_8_0",
                          "region": "us-central1",
                          "rootPassword": "hunter2",
                          "settings": {"tier": "db-custom-1-3840"}
                        }
                        """)
                .when().post(base + "/instances")
                .then()
                .statusCode(200)
                .body("kind", equalTo("sql#operation"))
                .body("status", equalTo("DONE"))
                .body("operationType", equalTo("CREATE"));

        given()
                .when().get(base + "/instances/" + instance)
                .then()
                .statusCode(200)
                .body("databaseVersion", equalTo("MYSQL_8_0"))
                .body("state", equalTo("RUNNABLE"))
                .body("connectionName", equalTo(project + ":us-central1:" + instance))
                // write-only in the real API
                .body("rootPassword", nullValue());

        given()
                .when().get(base + "/instances/" + instance + "/databases")
                .then()
                .statusCode(200)
                .body("kind", equalTo("sql#databasesList"))
                .body("items.name", containsInAnyOrder("information_schema", "mysql", "performance_schema", "sys"));

        given()
                .contentType("application/json")
                .body("{\"name\":\"appdb\"}")
                .when().post(base + "/instances/" + instance + "/databases")
                .then()
                .statusCode(200)
                .body("operationType", equalTo("CREATE_DATABASE"));

        given()
                .when().get(base + "/instances/" + instance + "/databases/appdb")
                .then()
                .statusCode(200)
                .body("kind", equalTo("sql#database"))
                .body("charset", equalTo("utf8mb4"))
                .body("collation", equalTo("utf8mb4_0900_ai_ci"));

        given()
                .when().delete(base + "/instances/" + instance + "/databases/mysql")
                .then()
                .statusCode(400)
                .body("error.status", equalTo("FAILED_PRECONDITION"));

        // root@% exists from provisioning; a user inserted without a host lands on '%'.
        given()
                .when().get(base + "/instances/" + instance + "/users")
                .then()
                .statusCode(200)
                .body("items", hasSize(1))
                .body("items[0].name", equalTo("root"))
                .body("items[0].host", equalTo("%"));

        given()
                .contentType("application/json")
                .body("{\"name\":\"app\",\"password\":\"secret\"}")
                .when().post(base + "/instances/" + instance + "/users")
                .then()
                .statusCode(200)
                .body("operationType", equalTo("CREATE_USER"));

        given()
                .contentType("application/json")
                .body("{\"name\":\"app\",\"host\":\"10.0.0.5\",\"password\":\"secret\"}")
                .when().post(base + "/instances/" + instance + "/users")
                .then()
                .statusCode(200);

        given()
                .when().get(base + "/instances/" + instance + "/users")
                .then()
                .statusCode(200)
                .body("items", hasSize(3))
                .body("items.host", hasItems("%", "10.0.0.5"))
                .body("items.password", not(hasItem("secret")));

        given()
                .queryParam("host", "%")
                .when().get(base + "/instances/" + instance + "/users/app")
                .then()
                .statusCode(200)
                .body("host", equalTo("%"));

        given()
                .queryParam("name", "app")
                .queryParam("host", "10.0.0.5")
                .when().delete(base + "/instances/" + instance + "/users")
                .then()
                .statusCode(200)
                .body("operationType", equalTo("DELETE_USER"));

        given()
                .queryParam("name", "root")
                .queryParam("host", "%")
                .when().delete(base + "/instances/" + instance + "/users")
                .then()
                .statusCode(400)
                .body("error.status", equalTo("FAILED_PRECONDITION"));

        given()
                .when().get("/v1/flags")
                .then()
                .statusCode(200)
                .body("items.find { it.name == 'sql_mode' }.appliesTo", hasItems("MYSQL_8_0", "MYSQL_8_4"));
    }
}
