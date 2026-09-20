package io.floci.gcp.services.kafka;

import io.floci.gcp.core.common.GcpException;

/**
 * The Kafka resource pattern an {@code acl_id} encodes (resources.proto, {@code Acl.name}).
 * The id is one of:
 * <ul>
 *   <li>{@code cluster}: the cluster itself ({@code CLUSTER}, {@code kafka-cluster}, {@code LITERAL})</li>
 *   <li>{@code topic/{name}}, {@code consumerGroup/{name}}, {@code transactionalId/{name}}: one resource</li>
 *   <li>{@code topicPrefixed/{name}} and friends: every resource whose name starts with {@code name}</li>
 *   <li>{@code allTopics}, {@code allConsumerGroups}, {@code allTransactionalIds}: the wildcard {@code *}</li>
 * </ul>
 * Anything else is {@code 400 INVALID_ARGUMENT}. Spelling is case-sensitive, as the API's is.
 */
record AclResourcePattern(String resourceType, String resourceName, String patternType) {

    private static final String LITERAL = "LITERAL";
    private static final String PREFIXED = "PREFIXED";

    static AclResourcePattern parse(String aclId) {
        if (aclId == null || aclId.isBlank()) {
            throw GcpException.invalidArgument("aclId is required");
        }
        switch (aclId) {
            case "cluster": return new AclResourcePattern("CLUSTER", "kafka-cluster", LITERAL);
            case "allTopics": return new AclResourcePattern("TOPIC", "*", LITERAL);
            case "allConsumerGroups": return new AclResourcePattern("GROUP", "*", LITERAL);
            case "allTransactionalIds": return new AclResourcePattern("TRANSACTIONAL_ID", "*", LITERAL);
            default: break;
        }
        int slash = aclId.indexOf('/');
        if (slash > 0 && slash < aclId.length() - 1 && aclId.indexOf('/', slash + 1) < 0) {
            String kind = aclId.substring(0, slash);
            String name = aclId.substring(slash + 1);
            switch (kind) {
                case "topic": return new AclResourcePattern("TOPIC", name, LITERAL);
                case "topicPrefixed": return new AclResourcePattern("TOPIC", name, PREFIXED);
                case "consumerGroup": return new AclResourcePattern("GROUP", name, LITERAL);
                case "consumerGroupPrefixed": return new AclResourcePattern("GROUP", name, PREFIXED);
                case "transactionalId": return new AclResourcePattern("TRANSACTIONAL_ID", name, LITERAL);
                case "transactionalIdPrefixed": return new AclResourcePattern("TRANSACTIONAL_ID", name, PREFIXED);
                default: break;
            }
        }
        throw GcpException.invalidArgument("Invalid aclId \"" + aclId + "\": expected cluster, allTopics, "
                + "allConsumerGroups, allTransactionalIds, or {topic|topicPrefixed|consumerGroup|"
                + "consumerGroupPrefixed|transactionalId|transactionalIdPrefixed}/{resource_name}");
    }
}
