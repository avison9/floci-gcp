# Managed Kafka

floci-gcp emulates Google Cloud Managed Service for Apache Kafka (MSK) over REST JSON using the real GCP Managed Kafka API.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `FLOCI_GCP_SERVICES_KAFKA_ENABLED` | `true` | Enable/disable Managed Kafka |
| `FLOCI_GCP_SERVICES_KAFKA_MOCK` | `false` | Use mock mode (no Docker; cluster state returns `ACTIVE` immediately) |

## Quick Start

=== "REST API"

    ```bash
    # Create a cluster
    curl -X POST \
      "http://localhost:4588/v1/projects/floci-local/locations/us-central1/clusters" \
      -H "Content-Type: application/json" \
      -d '{
        "clusterId": "my-cluster",
        "cluster": {
          "capacityConfig": { "vcpuCount": 3, "memoryBytes": 3221225472 },
          "gcpConfig": { "accessConfig": { "networkConfigs": [{ "subnet": "projects/floci-local/regions/us-central1/subnetworks/default" }] } }
        }
      }'

    # List clusters
    curl "http://localhost:4588/v1/projects/floci-local/locations/us-central1/clusters"

    # Create a topic
    curl -X POST \
      "http://localhost:4588/v1/projects/floci-local/locations/us-central1/clusters/my-cluster/topics" \
      -H "Content-Type: application/json" \
      -d '{"topicId":"my-topic","topic":{"partitionCount":3,"replicationFactor":1}}'

    # List topics
    curl "http://localhost:4588/v1/projects/floci-local/locations/us-central1/clusters/my-cluster/topics"
    ```

## Mock Mode

Set `FLOCI_GCP_SERVICES_KAFKA_MOCK=true` to use mock mode. In mock mode, clusters are created in memory and return `ACTIVE` state immediately without requiring a backing Redpanda container. Useful for testing Terraform or SDK code that provisions Kafka resources but does not produce or consume messages.

## Consumer Groups

```bash
# List consumer groups
curl "http://localhost:4588/v1/projects/floci-local/locations/us-central1/clusters/my-cluster/consumerGroups"

# Get a specific consumer group
curl "http://localhost:4588/v1/projects/floci-local/locations/us-central1/clusters/my-cluster/consumerGroups/my-group"

# Delete a consumer group
curl -X DELETE \
  "http://localhost:4588/v1/projects/floci-local/locations/us-central1/clusters/my-cluster/consumerGroups/my-group"
```

## Supported Operations

**Clusters:**

- `CreateCluster`
- `GetCluster`
- `ListClusters`
- `UpdateCluster`
- `DeleteCluster`

**Topics:**

- `CreateTopic`
- `GetTopic`
- `ListTopics`
- `UpdateTopic`
- `DeleteTopic`

**Consumer Groups:**

- `GetConsumerGroup`
- `ListConsumerGroups`
- `UpdateConsumerGroup`
- `DeleteConsumerGroup`

**ACLs:**

- `CreateAcl`, `GetAcl`, `ListAcls`, `UpdateAcl`, `DeleteAcl`
- `AddAclEntry`, `RemoveAclEntry`

This is the full `ManagedKafka` v1 RPC surface. The sibling `ManagedKafkaConnect` and
Schema Registry services are not served.

## ACLs

An ACL is addressed by an `acl_id` that encodes the Kafka resource pattern, exactly as the real
API spells it: `cluster`; `topic/{name}`, `consumerGroup/{name}`, `transactionalId/{name}`;
`topicPrefixed/{name}`, `consumerGroupPrefixed/{name}`, `transactionalIdPrefixed/{name}`; and
`allTopics`, `allConsumerGroups`, `allTransactionalIds`. Anything else is `400 INVALID_ARGUMENT`.
The output-only `resourceType`, `resourceName` and `patternType` fields are derived from the id.

```bash
B=http://localhost:4588/v1/projects/p/locations/us-central1/clusters/c
curl -s -X POST "$B/acls?aclId=topic/orders" -H 'Content-Type: application/json' \
  -d '{"aclEntries":[{"principal":"User:svc@p.iam.gserviceaccount.com","permissionType":"ALLOW","operation":"READ","host":"*"}]}'
curl -s -X POST "$B/acls/topic/orders:addAclEntry" -H 'Content-Type: application/json' \
  -d '{"principal":"User:svc@p.iam.gserviceaccount.com","permissionType":"ALLOW","operation":"WRITE","host":"*"}'
curl -s "$B/acls/topic/orders"
```

- Entries follow the proto's field rules: `principal` carries the `User:` prefix (or is `User:*`),
  `permissionType` is `ALLOW` or `DENY`, `operation` is one of the Kafka operations (`ALL`, `READ`,
  `WRITE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE`, `CLUSTER_ACTION`, `DESCRIBE_CONFIGS`,
  `ALTER_CONFIGS`, `IDEMPOTENT_WRITE`), matched case-insensitively and stored upper-case, and
  `host` must be `*`. At most 100 entries per ACL.
- `addAclEntry` creates the ACL if it does not exist (`aclCreated: true`); `removeAclEntry`
  deletes it when the last entry goes (`aclDeleted: true`). Adding an identical entry twice is
  a no-op.
- `etag` changes on every write. `UpdateAcl` with a stale `etag` is `409 ABORTED`; without one it
  is unconditional. `updateMask` may only name `aclEntries`.
- ACLs are control-plane metadata, like topics in this emulator: the Redpanda container runs
  without an authorizer, so entries are recorded and read back but do not gate produce or consume.
