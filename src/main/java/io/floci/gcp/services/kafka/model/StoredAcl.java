package io.floci.gcp.services.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code google.cloud.managedkafka.v1.Acl}: the entries that apply to one Kafka resource
 * pattern. {@code resourceType}, {@code resourceName} and {@code patternType} are derived from
 * the {@code acl_id} at creation and never taken from a request.
 */
@RegisterForReflection
public class StoredAcl {

    private String name;
    private List<AclEntry> aclEntries = new ArrayList<>();
    private String etag;
    private String resourceType;
    private String resourceName;
    private String patternType;

    public StoredAcl() {}

    public StoredAcl(String name, String resourceType, String resourceName, String patternType) {
        this.name = name;
        this.resourceType = resourceType;
        this.resourceName = resourceName;
        this.patternType = patternType;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public List<AclEntry> getAclEntries() { return aclEntries; }
    public void setAclEntries(List<AclEntry> aclEntries) { this.aclEntries = aclEntries; }

    public String getEtag() { return etag; }
    public void setEtag(String etag) { this.etag = etag; }

    public String getResourceType() { return resourceType; }
    public void setResourceType(String resourceType) { this.resourceType = resourceType; }

    public String getResourceName() { return resourceName; }
    public void setResourceName(String resourceName) { this.resourceName = resourceName; }

    public String getPatternType() { return patternType; }
    public void setPatternType(String patternType) { this.patternType = patternType; }
}
