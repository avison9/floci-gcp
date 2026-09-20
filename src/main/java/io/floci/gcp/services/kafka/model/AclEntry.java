package io.floci.gcp.services.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.Objects;

/** {@code google.cloud.managedkafka.v1.AclEntry}: one principal, permission, operation, host. */
@RegisterForReflection
public class AclEntry {

    private String principal;
    private String permissionType;
    private String operation;
    private String host;

    public AclEntry() {}

    public AclEntry(String principal, String permissionType, String operation, String host) {
        this.principal = principal;
        this.permissionType = permissionType;
        this.operation = operation;
        this.host = host;
    }

    public String getPrincipal() { return principal; }
    public void setPrincipal(String principal) { this.principal = principal; }

    public String getPermissionType() { return permissionType; }
    public void setPermissionType(String permissionType) { this.permissionType = permissionType; }

    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    /** Two entries are the same ACL rule when all four fields match; this is the identity
     * {@code addAclEntry} deduplicates on and {@code removeAclEntry} matches on. */
    @Override
    public boolean equals(Object o) {
        return o instanceof AclEntry e
                && Objects.equals(principal, e.principal)
                && Objects.equals(permissionType, e.permissionType)
                && Objects.equals(operation, e.operation)
                && Objects.equals(host, e.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, permissionType, operation, host);
    }
}
