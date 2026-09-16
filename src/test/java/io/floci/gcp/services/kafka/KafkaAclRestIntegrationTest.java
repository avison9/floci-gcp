package io.floci.gcp.services.kafka;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * {@code ManagedKafka} ACL RPCs over REST: the multi-segment {@code acls/**} id, the derived
 * resource pattern fields, entry validation, the etag on update, and the incremental
 * {@code :addAclEntry} / {@code :removeAclEntry} methods that create and delete the ACL at the
 * edges.
 */
@QuarkusTest
class KafkaAclRestIntegrationTest {

    private static final String PROJECT = "kafka-acl-it";
    private static final String LOCATION = "us-central1";
    private static final String BASE = "/v1/projects/" + PROJECT + "/locations/" + LOCATION;

    private static String cluster(String id) {
        given()
                .contentType("application/json")
                .queryParam("clusterId", id)
                .body("{\"capacityConfig\":{\"vcpuCount\":3,\"memoryBytes\":3221225472}}")
                .when().post(BASE + "/clusters")
                .then()
                .statusCode(200);
        return BASE + "/clusters/" + id;
    }

    private static String entry(String principal, String permission, String operation) {
        return "{\"principal\":\"" + principal + "\",\"permissionType\":\"" + permission
                + "\",\"operation\":\"" + operation + "\",\"host\":\"*\"}";
    }

    @Test
    void aclLifecycleFollowsTheManagedKafkaContract() {
        String cluster = cluster("acl-main");

        // create: the id is a query parameter and encodes the Kafka resource pattern
        String etag = given()
                .contentType("application/json")
                .queryParam("aclId", "topic/orders")
                .body("{\"aclEntries\":[" + entry("User:svc@p.iam.gserviceaccount.com", "allow", "read") + "]}")
                .when().post(cluster + "/acls")
                .then()
                .statusCode(200)
                .body("name", equalTo("projects/" + PROJECT + "/locations/" + LOCATION
                        + "/clusters/acl-main/acls/topic/orders"))
                .body("resourceType", equalTo("TOPIC"))
                .body("resourceName", equalTo("orders"))
                .body("patternType", equalTo("LITERAL"))
                .body("aclEntries", hasSize(1))
                // accepted case-insensitively, reported in the canonical spelling
                .body("aclEntries[0].permissionType", equalTo("ALLOW"))
                .body("aclEntries[0].operation", equalTo("READ"))
                .body("etag", notNullValue())
                .extract().path("etag");

        // get and list through the multi-segment id
        given()
                .when().get(cluster + "/acls/topic/orders")
                .then()
                .statusCode(200)
                .body("resourceName", equalTo("orders"))
                .body("etag", equalTo(etag));

        given()
                .contentType("application/json")
                .queryParam("aclId", "consumerGroupPrefixed/billing-")
                .body("{\"aclEntries\":[" + entry("User:*", "ALLOW", "DESCRIBE") + "]}")
                .when().post(cluster + "/acls")
                .then()
                .statusCode(200)
                .body("resourceType", equalTo("GROUP"))
                .body("resourceName", equalTo("billing-"))
                .body("patternType", equalTo("PREFIXED"));

        given()
                .when().get(cluster + "/acls")
                .then()
                .statusCode(200)
                .body("acls.resourceName", containsInAnyOrder("orders", "billing-"))
                .body("nextPageToken", nullValue());

        // update replaces the entries and rotates the etag; a stale etag is refused
        String rotated = given()
                .contentType("application/json")
                .queryParam("updateMask", "aclEntries")
                .body("{\"etag\":\"" + etag + "\",\"aclEntries\":["
                        + entry("User:svc@p.iam.gserviceaccount.com", "ALLOW", "WRITE") + "]}")
                .when().patch(cluster + "/acls/topic/orders")
                .then()
                .statusCode(200)
                .body("aclEntries", hasSize(1))
                .body("aclEntries[0].operation", equalTo("WRITE"))
                .body("etag", not(equalTo(etag)))
                .extract().path("etag");

        given()
                .contentType("application/json")
                .body("{\"etag\":\"" + etag + "\",\"aclEntries\":[" + entry("User:*", "DENY", "ALL") + "]}")
                .when().patch(cluster + "/acls/topic/orders")
                .then()
                .statusCode(409)
                .body("error.status", equalTo("ABORTED"));

        // addAclEntry on an existing ACL appends; on a missing one it creates (aclCreated)
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body(entry("User:other@p.iam.gserviceaccount.com", "ALLOW", "DESCRIBE"))
                .when().post(cluster + "/acls/topic/orders:addAclEntry")
                .then()
                .statusCode(200)
                .body("aclCreated", equalTo(false))
                .body("acl.aclEntries", hasSize(2))
                .body("acl.etag", not(equalTo(rotated)));

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body(entry("User:*", "ALLOW", "IDEMPOTENT_WRITE"))
                .when().post(cluster + "/acls/cluster:addAclEntry")
                .then()
                .statusCode(200)
                .body("aclCreated", equalTo(true))
                .body("acl.resourceType", equalTo("CLUSTER"))
                .body("acl.resourceName", equalTo("kafka-cluster"))
                .body("acl.aclEntries", hasSize(1));

        // removeAclEntry returns the remaining ACL, or aclDeleted when the last entry goes
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body(entry("User:other@p.iam.gserviceaccount.com", "allow", "describe"))
                .when().post(cluster + "/acls/topic/orders:removeAclEntry")
                .then()
                .statusCode(200)
                .body("acl.aclEntries", hasSize(1))
                .body("aclDeleted", nullValue());

        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body(entry("User:*", "ALLOW", "IDEMPOTENT_WRITE"))
                .when().post(cluster + "/acls/cluster:removeAclEntry")
                .then()
                .statusCode(200)
                .body("aclDeleted", equalTo(true))
                .body("acl", nullValue());

        given()
                .when().get(cluster + "/acls/cluster")
                .then()
                .statusCode(404)
                .body("error.status", equalTo("NOT_FOUND"));

        // delete, then the cluster cascade
        given()
                .when().delete(cluster + "/acls/topic/orders")
                .then()
                .statusCode(200);
        given()
                .when().get(cluster + "/acls")
                .then()
                .statusCode(200)
                .body("acls", hasSize(1));

        given().when().delete(cluster).then().statusCode(200);
        cluster("acl-main");
        given()
                .when().get(cluster + "/acls")
                .then()
                .statusCode(200)
                .body("acls", hasSize(0));
    }

    @Test
    void aclIdsAndEntriesAreValidatedAgainstTheGrammar() {
        String cluster = cluster("acl-validate");
        String good = entry("User:svc@p.iam.gserviceaccount.com", "ALLOW", "READ");

        for (String badId : new String[] {"topics/x", "topic/", "cluster/x", "Topic/x", "allTopic", "group/x", "topic/a/b"}) {
            given()
                    .contentType("application/json")
                    .queryParam("aclId", badId)
                    .body("{\"aclEntries\":[" + good + "]}")
                    .when().post(cluster + "/acls")
                    .then()
                    .statusCode(400)
                    .body("error.status", equalTo("INVALID_ARGUMENT"));
        }
        for (String goodId : new String[] {"cluster", "allTopics", "allConsumerGroups", "allTransactionalIds",
                "topic/t", "topicPrefixed/t", "consumerGroup/g", "consumerGroupPrefixed/g",
                "transactionalId/x", "transactionalIdPrefixed/x"}) {
            given()
                    .contentType("application/json")
                    .queryParam("aclId", goodId)
                    .body("{\"aclEntries\":[" + good + "]}")
                    .when().post(cluster + "/acls")
                    .then()
                    .statusCode(200);
        }

        // entries: principal prefix, permission and operation vocabularies, host wildcard, required
        for (String bad : new String[] {
                entry("svc@p.iam.gserviceaccount.com", "ALLOW", "READ"),
                entry("User:x", "PERMIT", "READ"),
                entry("User:x", "ALLOW", "SUBSCRIBE"),
                "{\"principal\":\"User:x\",\"permissionType\":\"ALLOW\",\"operation\":\"READ\",\"host\":\"10.0.0.1\"}",
                "{\"principal\":\"User:x\",\"permissionType\":\"ALLOW\",\"operation\":\"READ\"}"}) {
            given()
                    .contentType("application/json")
                    .queryParam("aclId", "topic/validate-" + bad.hashCode())
                    .body("{\"aclEntries\":[" + bad + "]}")
                    .when().post(cluster + "/acls")
                    .then()
                    .statusCode(400)
                    .body("error.status", equalTo("INVALID_ARGUMENT"));
        }
        given()
                .contentType("application/json")
                .queryParam("aclId", "topic/no-entries")
                .body("{\"aclEntries\":[]}")
                .when().post(cluster + "/acls")
                .then()
                .statusCode(400);

        // duplicate create is a conflict; unknown cluster is 404
        given()
                .contentType("application/json")
                .queryParam("aclId", "cluster")
                .body("{\"aclEntries\":[" + good + "]}")
                .when().post(cluster + "/acls")
                .then()
                .statusCode(409)
                .body("error.status", equalTo("ALREADY_EXISTS"));
        given()
                .when().get(BASE + "/clusters/ghost/acls")
                .then()
                .statusCode(404);
    }

    @Test
    void aclEntriesAreCappedAtOneHundred() {
        String cluster = cluster("acl-cap");
        StringBuilder entries = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            if (i > 0) {
                entries.append(',');
            }
            entries.append(entry("User:u" + i + "@p.iam.gserviceaccount.com", "ALLOW", "READ"));
        }
        given()
                .contentType("application/json")
                .queryParam("aclId", "allTopics")
                .body("{\"aclEntries\":[" + entries + "]}")
                .when().post(cluster + "/acls")
                .then()
                .statusCode(200)
                .body("aclEntries", hasSize(100));
        given()
                .urlEncodingEnabled(false)
                .contentType("application/json")
                .body(entry("User:one-too-many@p.iam.gserviceaccount.com", "ALLOW", "READ"))
                .when().post(cluster + "/acls/allTopics:addAclEntry")
                .then()
                .statusCode(400)
                .body("error.status", equalTo("INVALID_ARGUMENT"));
    }
}
