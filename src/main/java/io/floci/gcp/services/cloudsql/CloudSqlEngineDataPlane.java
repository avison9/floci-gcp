package io.floci.gcp.services.cloudsql;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Map;

/**
 * The data plane {@link CloudSqlService} talks to: picks the engine's plane from the
 * instance's {@code databaseVersion} on every call, so the control plane never needs to know
 * which engine an instance runs.
 */
@ApplicationScoped
public class CloudSqlEngineDataPlane implements CloudSqlDataPlane {

    private final CloudSqlPostgresDataPlane postgres;
    private final CloudSqlMySqlDataPlane mysql;

    @Inject
    public CloudSqlEngineDataPlane(CloudSqlPostgresDataPlane postgres, CloudSqlMySqlDataPlane mysql) {
        this.postgres = postgres;
        this.mysql = mysql;
    }

    private CloudSqlDataPlane select(Map<String, Object> metadata) {
        Object version = metadata == null ? null : metadata.get("databaseVersion");
        return switch (CloudSqlEngine.fromDatabaseVersion(version == null ? null : version.toString())) {
            case POSTGRES -> postgres;
            case MYSQL -> mysql;
        };
    }

    @Override
    public Map<String, Object> startInstance(String project, String instance, Map<String, Object> metadata) {
        return select(metadata).startInstance(project, instance, metadata);
    }

    @Override
    public Map<String, Object> ensureInstance(String project, String instance, Map<String, Object> metadata) {
        return select(metadata).ensureInstance(project, instance, metadata);
    }

    @Override
    public void stopInstance(String project, String instance, Map<String, Object> metadata, boolean removeStorage) {
        select(metadata).stopInstance(project, instance, metadata, removeStorage);
    }

    @Override
    public void createDatabase(Map<String, Object> instanceMetadata, String database) {
        select(instanceMetadata).createDatabase(instanceMetadata, database);
    }

    @Override
    public void deleteDatabase(Map<String, Object> instanceMetadata, String database) {
        select(instanceMetadata).deleteDatabase(instanceMetadata, database);
    }

    @Override
    public void createOrUpdateUser(Map<String, Object> instanceMetadata, String user, String host, String password) {
        select(instanceMetadata).createOrUpdateUser(instanceMetadata, user, host, password);
    }

    @Override
    public void deleteUser(Map<String, Object> instanceMetadata, String user, String host, Iterable<String> databases) {
        select(instanceMetadata).deleteUser(instanceMetadata, user, host, databases);
    }

    @Override
    public void grantDatabaseAccess(Map<String, Object> instanceMetadata, String database, String user, String host) {
        select(instanceMetadata).grantDatabaseAccess(instanceMetadata, database, user, host);
    }
}
