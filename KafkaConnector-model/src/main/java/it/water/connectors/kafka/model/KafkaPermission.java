package it.water.connectors.kafka.model;

import it.water.core.validation.annotations.NoMalitiusCode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;

@Getter
@AllArgsConstructor
public class KafkaPermission {
    @NoMalitiusCode
    private String topic;
    private PatternType patternType;
    private AclOperation aclOperation;
    private AclPermissionType aclPermissionType;
}
