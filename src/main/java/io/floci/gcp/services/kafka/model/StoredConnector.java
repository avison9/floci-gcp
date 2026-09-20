package io.floci.gcp.services.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.Map;

/**
 * {@code google.cloud.managedkafka.v1.Connector}. {@code taskRestartPolicy} is the one member of
 * the {@code restart_policy} oneof, so it is omitted from the JSON when unset rather than sent as
 * {@code null}, which is how a proto3 oneof serializes.
 */
@RegisterForReflection
public class StoredConnector {

    private String name;
    private Map<String, String> configs;
    private ConnectorState state;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> taskRestartPolicy;

    public StoredConnector() {}

    public StoredConnector(String name) {
        this.name = name;
        this.state = ConnectorState.RUNNING;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Map<String, String> getConfigs() { return configs; }
    public void setConfigs(Map<String, String> configs) { this.configs = configs; }

    public ConnectorState getState() { return state; }
    public void setState(ConnectorState state) { this.state = state; }

    public Map<String, Object> getTaskRestartPolicy() { return taskRestartPolicy; }
    public void setTaskRestartPolicy(Map<String, Object> taskRestartPolicy) { this.taskRestartPolicy = taskRestartPolicy; }
}
