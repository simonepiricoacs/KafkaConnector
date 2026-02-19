package it.water.connectors.kafka.model;
import it.water.core.validation.annotations.NoMalitiusCode;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
public class KafkaPermission {
    @NoMalitiusCode
    private String topic;
    private PatternType patternType;
    private AclOperation aclOperation;
    private AclPermissionType aclPermissionType;
    public KafkaPermission(String topic, PatternType patternType, AclOperation aclOperation, AclPermissionType aclPermissionType) {
        this.topic = topic;
        this.patternType = patternType;
        this.aclOperation = aclOperation;
        this.aclPermissionType = aclPermissionType;
    }
    public String getTopic() {
        return topic;
    }
    public PatternType getPatternType() {
        return patternType;
    }
    public AclOperation getAclOperation() {
        return aclOperation;
    }
    public AclPermissionType getAclPermissionType() {
        return aclPermissionType;
    }
}
