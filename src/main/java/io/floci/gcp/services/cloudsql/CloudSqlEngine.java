package io.floci.gcp.services.cloudsql;

import io.floci.gcp.core.common.GcpException;

import java.util.List;

/**
 * The database engine behind a Cloud SQL instance, derived from {@code databaseVersion}
 * ({@code POSTGRES_16}, {@code MYSQL_8_0}, ...). Everything that differs between engines on
 * the control plane lives here so {@link CloudSqlService} stays engine-agnostic: which
 * databases exist on a fresh instance and cannot be dropped, the default charset/collation a
 * new database reports, and whether user identities carry a host part.
 */
enum CloudSqlEngine {

    /** PostgreSQL: one {@code postgres} database, roles have no host. */
    POSTGRES("PostgreSQL", "POSTGRES_", List.of("postgres"), "UTF8", "en_US.UTF8", null, null),

    /**
     * MySQL: the four system schemas a fresh Cloud SQL MySQL instance lists, {@code utf8mb4}
     * defaults (8.0 and 8.4), host-qualified users defaulting to {@code %}, and the
     * {@code root@%} account the instance is provisioned with.
     */
    MYSQL("MySQL", "MYSQL_", List.of("information_schema", "mysql", "performance_schema", "sys"),
            "utf8mb4", "utf8mb4_0900_ai_ci", "%", "root");

    private final String displayName;
    private final String versionPrefix;
    private final List<String> systemDatabases;
    private final String defaultCharset;
    private final String defaultCollation;
    private final String defaultHost;
    private final String builtInUser;

    CloudSqlEngine(String displayName, String versionPrefix, List<String> systemDatabases, String defaultCharset,
                   String defaultCollation, String defaultHost, String builtInUser) {
        this.displayName = displayName;
        this.versionPrefix = versionPrefix;
        this.systemDatabases = systemDatabases;
        this.defaultCharset = defaultCharset;
        this.defaultCollation = defaultCollation;
        this.defaultHost = defaultHost;
        this.builtInUser = builtInUser;
    }

    /** Resolves the engine for a {@code databaseVersion}; {@code 400} for anything else (SQL Server, blank). */
    static CloudSqlEngine fromDatabaseVersion(String databaseVersion) {
        if (databaseVersion != null) {
            for (CloudSqlEngine engine : values()) {
                if (databaseVersion.startsWith(engine.versionPrefix)) {
                    return engine;
                }
            }
        }
        throw GcpException.invalidArgument("Only PostgreSQL and MySQL Cloud SQL instances are supported");
    }

    @Override
    public String toString() {
        return displayName;
    }

    /** Databases present on a fresh instance; listed by {@code databases.list} and never dropped. */
    List<String> systemDatabases() {
        return systemDatabases;
    }

    boolean isSystemDatabase(String database) {
        return systemDatabases.contains(database);
    }

    String defaultCharset() {
        return defaultCharset;
    }

    String defaultCollation() {
        return defaultCollation;
    }

    /** Whether user identities are {@code 'name'@'host'} (MySQL) rather than a bare role name. */
    boolean hostQualifiedUsers() {
        return defaultHost != null;
    }

    /**
     * Normalises the {@code host} of a user request: PostgreSQL rejects one, MySQL defaults a
     * missing one to {@code %} so {@code users.insert} and a later {@code users.get?host=%}
     * address the same stored user.
     */
    String normalizeHost(String host) {
        boolean given = host != null && !host.isBlank();
        if (!hostQualifiedUsers()) {
            if (given) {
                throw GcpException.invalidArgument("PostgreSQL Cloud SQL users do not support host-qualified identities");
            }
            return null;
        }
        return given ? host : defaultHost;
    }

    /** The account the data plane is provisioned with, or {@code null} when none is surfaced as a user. */
    String builtInUser() {
        return builtInUser;
    }

    /**
     * Whether {@code user@host} is the provisioned admin identity itself. Only that identity is
     * protected: a MySQL {@code root@10.0.0.5} is an ordinary, separately created account.
     */
    boolean isBuiltInUser(String user, String host) {
        return builtInUser != null && builtInUser.equals(user)
                && java.util.Objects.equals(normalizeHost(null), host);
    }
}
