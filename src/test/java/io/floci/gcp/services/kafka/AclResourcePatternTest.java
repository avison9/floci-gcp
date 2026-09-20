package io.floci.gcp.services.kafka;

import io.floci.gcp.core.common.GcpException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The {@code acl_id} grammar from resources.proto, spelling for spelling. */
class AclResourcePatternTest {

    @Test
    void everyDocumentedSpellingDerivesItsPattern() {
        Map<String, AclResourcePattern> expected = Map.ofEntries(
                Map.entry("cluster", new AclResourcePattern("CLUSTER", "kafka-cluster", "LITERAL")),
                Map.entry("topic/orders", new AclResourcePattern("TOPIC", "orders", "LITERAL")),
                Map.entry("topicPrefixed/ord", new AclResourcePattern("TOPIC", "ord", "PREFIXED")),
                Map.entry("allTopics", new AclResourcePattern("TOPIC", "*", "LITERAL")),
                Map.entry("consumerGroup/billing", new AclResourcePattern("GROUP", "billing", "LITERAL")),
                Map.entry("consumerGroupPrefixed/bill", new AclResourcePattern("GROUP", "bill", "PREFIXED")),
                Map.entry("allConsumerGroups", new AclResourcePattern("GROUP", "*", "LITERAL")),
                Map.entry("transactionalId/tx-1", new AclResourcePattern("TRANSACTIONAL_ID", "tx-1", "LITERAL")),
                Map.entry("transactionalIdPrefixed/tx-", new AclResourcePattern("TRANSACTIONAL_ID", "tx-", "PREFIXED")),
                Map.entry("allTransactionalIds", new AclResourcePattern("TRANSACTIONAL_ID", "*", "LITERAL")));
        expected.forEach((id, pattern) -> assertEquals(pattern, AclResourcePattern.parse(id), id));
    }

    @Test
    void anythingElseIsInvalidArgument() {
        for (String bad : List.of("", " ", "topics/x", "topic/", "topic", "/x", "cluster/x", "Topic/x", "TOPIC/x",
                "allTopic", "allTopics/x", "group/x", "topic/a/b", "topicPrefixed/", "transactionalid/x")) {
            GcpException error = assertThrows(GcpException.class, () -> AclResourcePattern.parse(bad), "[" + bad + "]");
            assertEquals("INVALID_ARGUMENT", error.getGcpStatus(), bad);
        }
        assertEquals("INVALID_ARGUMENT", assertThrows(GcpException.class, () -> AclResourcePattern.parse(null)).getGcpStatus());
    }

    @Test
    void resourceNamesKeepTheirCharactersExceptASecondSlash() {
        // Kafka topic and group names allow dots, dashes and underscores; the wildcard literal
        // is only produced by the all* spellings, but a literal "*" name is passed through.
        assertEquals("a.b-c_d", AclResourcePattern.parse("topic/a.b-c_d").resourceName());
        assertEquals("*", AclResourcePattern.parse("consumerGroup/*").resourceName());
    }
}
