package io.floci.gcp.services.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.time.Instant;
import java.util.Map;

/**
 * {@code google.cloud.managedkafka.v1.ConnectCluster}, serialized as the wire shape:
 * {@code capacityConfig}, {@code gcpConfig}, {@code labels} and {@code config} nest as the proto
 * declares them. {@code kafkaCluster} is the full resource name of the Kafka cluster the Connect
 * cluster is attached to, immutable after create. {@code ConnectCluster.State} has the same
 * members as {@code Cluster.State}, so {@link ClusterState} is reused.
 */
@RegisterForReflection
public class StoredConnectCluster {

    private String name;
    private String kafkaCluster;
    private Instant createTime;
    private Instant updateTime;
    private Map<String, String> labels;
    private Map<String, Object> capacityConfig;
    private Map<String, Object> gcpConfig;
    private ClusterState state;
    private Map<String, String> config;

    public StoredConnectCluster() {}

    public StoredConnectCluster(String name, String kafkaCluster) {
        this.name = name;
        this.kafkaCluster = kafkaCluster;
        this.state = ClusterState.CREATING;
        this.createTime = Instant.now();
        this.updateTime = this.createTime;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getKafkaCluster() { return kafkaCluster; }
    public void setKafkaCluster(String kafkaCluster) { this.kafkaCluster = kafkaCluster; }

    public Instant getCreateTime() { return createTime; }
    public void setCreateTime(Instant createTime) { this.createTime = createTime; }

    public Instant getUpdateTime() { return updateTime; }
    public void setUpdateTime(Instant updateTime) { this.updateTime = updateTime; }

    public Map<String, String> getLabels() { return labels; }
    public void setLabels(Map<String, String> labels) { this.labels = labels; }

    public Map<String, Object> getCapacityConfig() { return capacityConfig; }
    public void setCapacityConfig(Map<String, Object> capacityConfig) { this.capacityConfig = capacityConfig; }

    public Map<String, Object> getGcpConfig() { return gcpConfig; }
    public void setGcpConfig(Map<String, Object> gcpConfig) { this.gcpConfig = gcpConfig; }

    public ClusterState getState() { return state; }
    public void setState(ClusterState state) { this.state = state; }

    public Map<String, String> getConfig() { return config; }
    public void setConfig(Map<String, String> config) { this.config = config; }
}
