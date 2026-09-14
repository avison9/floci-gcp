package io.floci.gcp.services.cloudsql;

import io.floci.gcp.config.EmulatorConfig;
import io.floci.gcp.core.common.GcpException;
import io.floci.gcp.core.common.docker.ContainerBuilder;
import io.floci.gcp.core.common.docker.ContainerDetector;
import io.floci.gcp.core.common.docker.ContainerLifecycleManager;
import io.floci.gcp.core.common.docker.ContainerLifecycleManager.ExecResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class CloudSqlPostgresDataPlane extends CloudSqlContainerDataPlane {

    private static final Logger LOG = Logger.getLogger(CloudSqlPostgresDataPlane.class);
    private static final int POSTGRES_PORT = 5432;
    private static final String ADMIN_USER = "postgres";
    private static final String ADMIN_PASSWORD = "postgres";
    private static final String POSTGRES_DATA_PARENT_V18 = "/var/lib/postgresql";
    private static final String POSTGRES_DATA_PARENT_LEGACY = "/var/lib/postgresql/data";

    @Inject
    public CloudSqlPostgresDataPlane(ContainerBuilder containerBuilder,
                                     ContainerLifecycleManager lifecycleManager,
                                     ContainerDetector containerDetector,
                                     EmulatorConfig config) {
        super(containerBuilder, lifecycleManager, containerDetector, config, LOG);
    }

    @Override
    protected String engineName() {
        return "postgres";
    }

    @Override
    protected String displayName() {
        return "PostgreSQL";
    }

    @Override
    protected int port() {
        return POSTGRES_PORT;
    }

    @Override
    protected String imageFor(String databaseVersion) {
        return switch (databaseVersion) {
            case "POSTGRES_15" -> config.services().cloudsql().postgres15Image();
            case "POSTGRES_16" -> config.services().cloudsql().postgres16Image();
            case "POSTGRES_17" -> config.services().cloudsql().postgres17Image();
            case "POSTGRES_18" -> config.services().cloudsql().postgres18Image();
            default -> throw GcpException.invalidArgument("Unsupported PostgreSQL databaseVersion: " + databaseVersion);
        };
    }

    @Override
    protected Map<String, String> containerEnv() {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("POSTGRES_USER", ADMIN_USER);
        env.put("POSTGRES_PASSWORD", ADMIN_PASSWORD);
        env.put("POSTGRES_DB", "postgres");
        return env;
    }

    @Override
    protected String dataMountPath(String databaseVersion) {
        if ("POSTGRES_18".equals(databaseVersion)) {
            return POSTGRES_DATA_PARENT_V18;
        }
        return POSTGRES_DATA_PARENT_LEGACY;
    }

    @Override
    protected List<String> readinessCommand() {
        return List.of("pg_isready", "-h", "127.0.0.1", "-p", String.valueOf(POSTGRES_PORT),
                "-U", ADMIN_USER, "-d", "postgres");
    }

    @Override
    public void createDatabase(Map<String, Object> instanceMetadata, String database) {
        if ("postgres".equals(database)) {
            return;
        }
        if (databaseExists(instanceMetadata, database)) {
            return;
        }
        runSql(instanceMetadata, "postgres", "CREATE DATABASE " + quoteIdentifier(database),
                "Could not create PostgreSQL database " + database);
    }

    @Override
    public void deleteDatabase(Map<String, Object> instanceMetadata, String database) {
        if ("postgres".equals(database)) {
            return;
        }
        runSql(instanceMetadata, "postgres", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        + "WHERE datname = " + quoteLiteral(database) + " AND pid <> pg_backend_pid()",
                "Could not delete PostgreSQL database " + database);
        runSql(instanceMetadata, "postgres", "DROP DATABASE IF EXISTS " + quoteIdentifier(database),
                "Could not delete PostgreSQL database " + database);
    }

    /** PostgreSQL roles carry no host; the control plane never passes one for this engine. */
    @Override
    public void createOrUpdateUser(Map<String, Object> instanceMetadata, String user, String host, String password) {
        String secret = password == null || password.isBlank() ? ADMIN_PASSWORD : password;
        String verb = roleExists(instanceMetadata, user) ? "ALTER ROLE " : "CREATE ROLE ";
        runSql(instanceMetadata, "postgres",
                verb + quoteIdentifier(user) + " WITH LOGIN PASSWORD " + quoteLiteral(secret),
                "Could not create PostgreSQL user " + user);
    }

    @Override
    public void deleteUser(Map<String, Object> instanceMetadata, String user, String host, Iterable<String> databases) {
        for (String database : databases) {
            ExecResult result = psql(instanceMetadata, database, "DROP OWNED BY " + quoteIdentifier(user));
            if (result.exitCode() != 0) {
                LOG.debugv("Could not drop objects owned by {0} in database {1}: {2}",
                        user, database, errorOf(result));
            }
        }
        runSql(instanceMetadata, "postgres", "DROP ROLE IF EXISTS " + quoteIdentifier(user),
                "Could not delete PostgreSQL user " + user);
    }

    @Override
    public void grantDatabaseAccess(Map<String, Object> instanceMetadata, String database, String user, String host) {
        runSql(instanceMetadata, "postgres",
                "GRANT CONNECT, CREATE ON DATABASE " + quoteIdentifier(database) + " TO " + quoteIdentifier(user),
                "Could not grant PostgreSQL database access");
        runSql(instanceMetadata, database,
                "GRANT USAGE, CREATE ON SCHEMA public TO " + quoteIdentifier(user),
                "Could not grant PostgreSQL schema access");
    }

    /** Runs a SQL statement via {@code psql} inside the instance container; throws on failure. */
    private void runSql(Map<String, Object> instanceMetadata, String database, String sql, String errorMessage) {
        ExecResult result = psql(instanceMetadata, database, sql);
        if (result.exitCode() != 0) {
            throw GcpException.unavailable(errorMessage + ": " + errorOf(result));
        }
    }

    /** Runs a scalar query via {@code psql}; returns the trimmed output (empty when no rows match). */
    private String querySql(Map<String, Object> instanceMetadata, String database, String sql) {
        ExecResult result = psql(instanceMetadata, database, sql);
        if (result.exitCode() != 0) {
            throw GcpException.unavailable("PostgreSQL query failed: " + errorOf(result));
        }
        return result.stdout().strip();
    }

    private ExecResult psql(Map<String, Object> instanceMetadata, String database, String sql) {
        return lifecycleManager.exec(requireContainerId(instanceMetadata),
                List.of("PGPASSWORD=" + ADMIN_PASSWORD),
                List.of("psql", "-h", "127.0.0.1", "-p", String.valueOf(POSTGRES_PORT),
                        "-U", ADMIN_USER, "-d", database, "-v", "ON_ERROR_STOP=1", "-tAqc", sql));
    }

    private boolean databaseExists(Map<String, Object> instanceMetadata, String database) {
        return !querySql(instanceMetadata, "postgres",
                "SELECT 1 FROM pg_database WHERE datname = " + quoteLiteral(database)).isEmpty();
    }

    private boolean roleExists(Map<String, Object> instanceMetadata, String user) {
        return !querySql(instanceMetadata, "postgres",
                "SELECT 1 FROM pg_roles WHERE rolname = " + quoteLiteral(user)).isEmpty();
    }

    private static String quoteIdentifier(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String quoteLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
