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

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * MySQL data plane: the official {@code mysql} image, one container per instance, DDL issued
 * through the {@code mysql} client inside the container as {@code root}.
 *
 * <p>Users are {@code 'name'@'host'} identities, so every user operation takes the host the
 * control plane resolved ({@code %} unless the request set one). The {@code root@%} account the
 * image provisions is the emulator's own admin login: its password stays {@code root} and the
 * control plane does not let it be dropped, so a {@code users.update} on it is acknowledged
 * without touching the server.
 */
@ApplicationScoped
public class CloudSqlMySqlDataPlane extends CloudSqlContainerDataPlane {

    private static final Logger LOG = Logger.getLogger(CloudSqlMySqlDataPlane.class);
    private static final int MYSQL_PORT = 3306;
    private static final String ADMIN_USER = "root";
    private static final String ADMIN_PASSWORD = "root";
    private static final String MYSQL_DATA_DIR = "/var/lib/mysql";
    /** {@code MYSQL_8_0} or a minor-pinned {@code MYSQL_8_0_NN}; nothing looser. */
    private static final Pattern MYSQL_8_0_VERSION = Pattern.compile("^MYSQL_8_0(?:_\\d+)?$");
    /** Charset and collation names are bare identifiers in MySQL ({@code utf8mb4}, {@code utf8mb4_bin}). */
    private static final Pattern CHARSET_OR_COLLATION = Pattern.compile("^[A-Za-z0-9_]+$");

    @Inject
    public CloudSqlMySqlDataPlane(ContainerBuilder containerBuilder,
                                  ContainerLifecycleManager lifecycleManager,
                                  ContainerDetector containerDetector,
                                  EmulatorConfig config) {
        super(containerBuilder, lifecycleManager, containerDetector, config, LOG);
    }

    @Override
    protected String engineName() {
        return "mysql";
    }

    @Override
    protected String displayName() {
        return "MySQL";
    }

    @Override
    protected int port() {
        return MYSQL_PORT;
    }

    /**
     * Cloud SQL spells a minor-pinned 8.0 as {@code MYSQL_8_0_NN}; the image is the same 8.0 line
     * either way, since the emulator tracks one patch per major.
     */
    @Override
    protected String imageFor(String databaseVersion) {
        if (databaseVersion != null && MYSQL_8_0_VERSION.matcher(databaseVersion).matches()) {
            return config.services().cloudsql().mysql80Image();
        }
        if ("MYSQL_8_4".equals(databaseVersion)) {
            return config.services().cloudsql().mysql84Image();
        }
        throw GcpException.invalidArgument("Unsupported MySQL databaseVersion: " + databaseVersion);
    }

    @Override
    protected Map<String, String> containerEnv() {
        return Map.of("MYSQL_ROOT_PASSWORD", ADMIN_PASSWORD);
    }

    @Override
    protected String dataMountPath(String databaseVersion) {
        return MYSQL_DATA_DIR;
    }

    /**
     * Over TCP on purpose: the image's entrypoint runs a socket-only temporary server while it
     * initialises the data directory, and a socket ping would report ready before the real
     * server listens on {@value MYSQL_PORT}.
     */
    @Override
    protected List<String> readinessCommand() {
        return List.of("mysqladmin", "ping", "-h", "127.0.0.1", "-P", String.valueOf(MYSQL_PORT),
                "-u", ADMIN_USER, "-p" + ADMIN_PASSWORD, "--silent");
    }

    /** Applies the requested charset and collation so the server matches what the resource reports. */
    @Override
    public void createDatabase(Map<String, Object> instanceMetadata, String database, String charset, String collation) {
        if (CloudSqlEngine.MYSQL.isSystemDatabase(database)) {
            return;
        }
        StringBuilder sql = new StringBuilder("CREATE DATABASE IF NOT EXISTS ").append(quoteIdentifier(database));
        if (charset != null && !charset.isBlank()) {
            sql.append(" CHARACTER SET ").append(requireName("charset", charset));
        }
        if (collation != null && !collation.isBlank()) {
            sql.append(" COLLATE ").append(requireName("collation", collation));
        }
        runSql(instanceMetadata, sql.toString(), "Could not create MySQL database " + database);
    }

    private static String requireName(String field, String value) {
        if (!CHARSET_OR_COLLATION.matcher(value).matches()) {
            throw GcpException.invalidArgument("Invalid MySQL " + field + ": " + value);
        }
        return value;
    }

    @Override
    public void deleteDatabase(Map<String, Object> instanceMetadata, String database) {
        if (CloudSqlEngine.MYSQL.isSystemDatabase(database)) {
            return;
        }
        runSql(instanceMetadata, "DROP DATABASE IF EXISTS " + quoteIdentifier(database),
                "Could not delete MySQL database " + database);
    }

    @Override
    public void createOrUpdateUser(Map<String, Object> instanceMetadata, String user, String host, String password) {
        if (isAdminAccount(user, host)) {
            LOG.debugv("Leaving the MySQL admin account {0} unchanged; its password is fixed in the emulator", user);
            return;
        }
        String secret = password == null || password.isBlank() ? ADMIN_PASSWORD : password;
        String account = account(user, host);
        // CREATE ... IF NOT EXISTS then ALTER: one round trip that both creates a new user and
        // rotates the password of an existing one, which is what users.insert / users.update need.
        runSql(instanceMetadata,
                "CREATE USER IF NOT EXISTS " + account + " IDENTIFIED BY " + quoteLiteral(secret) + "; "
                        + "ALTER USER " + account + " IDENTIFIED BY " + quoteLiteral(secret),
                "Could not create MySQL user " + user);
    }

    @Override
    public void deleteUser(Map<String, Object> instanceMetadata, String user, String host, Iterable<String> databases) {
        runSql(instanceMetadata, "DROP USER IF EXISTS " + account(user, host),
                "Could not delete MySQL user " + user);
    }

    /**
     * A Cloud SQL MySQL user created through the Admin API can use every user database, so
     * each database the control plane knows about is granted in full. System schemas are
     * skipped: MySQL refuses grants on {@code information_schema} and the others are not the
     * user's to own.
     */
    @Override
    public void grantDatabaseAccess(Map<String, Object> instanceMetadata, String database, String user, String host) {
        if (CloudSqlEngine.MYSQL.isSystemDatabase(database) || isAdminAccount(user, host)) {
            return;
        }
        runSql(instanceMetadata,
                "GRANT ALL PRIVILEGES ON " + quoteIdentifier(database) + ".* TO " + account(user, host),
                "Could not grant MySQL database access");
    }

    /** Runs SQL via the {@code mysql} client inside the instance container; throws on failure. */
    private void runSql(Map<String, Object> instanceMetadata, String sql, String errorMessage) {
        ExecResult result = mysql(instanceMetadata, sql);
        if (result.exitCode() != 0) {
            throw GcpException.unavailable(errorMessage + ": " + errorOf(result));
        }
    }

    private ExecResult mysql(Map<String, Object> instanceMetadata, String sql) {
        // MYSQL_PWD rather than -p so the client does not print its insecure-password warning
        // on stderr, which errorOf would otherwise surface as the failure text.
        return lifecycleManager.exec(requireContainerId(instanceMetadata),
                List.of("MYSQL_PWD=" + ADMIN_PASSWORD),
                List.of("mysql", "-h", "127.0.0.1", "-P", String.valueOf(MYSQL_PORT),
                        "-u", ADMIN_USER, "--batch", "--skip-column-names", "-e", sql));
    }

    /** Only the provisioned {@code root@%}; a {@code root} at another host is an ordinary account. */
    private static boolean isAdminAccount(String user, String host) {
        return ADMIN_USER.equals(user) && (host == null || host.isBlank() || "%".equals(host));
    }

    private static String account(String user, String host) {
        return quoteLiteral(user) + "@" + quoteLiteral(host == null || host.isBlank() ? "%" : host);
    }

    private static String quoteIdentifier(String value) {
        return "`" + value.replace("`", "``") + "`";
    }

    private static String quoteLiteral(String value) {
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }
}
