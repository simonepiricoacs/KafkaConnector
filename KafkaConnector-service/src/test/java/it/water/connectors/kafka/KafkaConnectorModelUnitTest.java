package it.water.connectors.kafka;

import it.water.connectors.kafka.model.ACLConfig;
import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.ConnectorTask;
import it.water.connectors.kafka.model.KafkaMessage;
import it.water.connectors.kafka.model.KafkaPermission;
import it.water.connectors.kafka.model.KafkaRestMessage;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.connectors.kafka.model.TopicConfig;
import it.water.connectors.kafka.model.messages.types.MqttMessageType;
import it.water.connectors.kafka.model.messages.types.SystemMessageType;
import it.water.connectors.kafka.util.KafkaConnectorConstants;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConnectorModelUnitTest {

    @Test
    void connectorConfigGettersSettersAndDefaults() {
        ConnectorConfig config = new ConnectorConfig();
        assertEquals(500, config.getMaxPollIntervalMs());
        assertEquals(1, config.getTaskMax());

        config.setName("connector-1");
        config.setConnectorClass("my.class");
        config.setMaxPollIntervalMs(1000);
        config.setTaskMax(5);

        assertEquals("connector-1", config.getName());
        assertEquals("my.class", config.getConnectorClass());
        assertEquals(1000, config.getMaxPollIntervalMs());
        assertEquals(5, config.getTaskMax());
    }

    @Test
    void topicAndAclConfigsImplementEqualsAndHashCode() {
        TopicConfig topicOne = new TopicConfig();
        topicOne.setTopic("topic");
        topicOne.setNumPartition(3);
        topicOne.setReplicationFactor((short) 1);

        TopicConfig topicTwo = new TopicConfig();
        topicTwo.setTopic("topic");
        topicTwo.setNumPartition(3);
        topicTwo.setReplicationFactor((short) 1);

        assertEquals(topicOne, topicTwo);
        assertEquals(topicOne.hashCode(), topicTwo.hashCode());

        KafkaPermission permission = new KafkaPermission("topic", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW);
        HashMap<String, KafkaPermission> permissions = new HashMap<>();
        permissions.put("topic", permission);

        ACLConfig aclOne = new ACLConfig();
        aclOne.setUsername("user");
        aclOne.setPermissions(permissions);

        ACLConfig aclTwo = new ACLConfig();
        aclTwo.setUsername("user");
        aclTwo.setPermissions(permissions);

        assertEquals(aclOne, aclTwo);
        assertEquals(aclOne.hashCode(), aclTwo.hashCode());
    }

    @Test
    void kafkaConnectorAndTasksGettersWork() {
        KafkaConnector connector = new KafkaConnector("connector-1");
        connector.setType("source");

        ConnectorConfig config = new ConnectorConfig("connector-1", "my.class");
        connector.setConfig(config);

        ConnectorTask task = new ConnectorTask();
        task.setConnector("connector-1");
        connector.setTasks(new ConnectorTask[]{task});
        connector.setOwnerUserId(12L);

        assertEquals("connector-1", connector.getName());
        assertEquals("source", connector.getType());
        assertEquals(config, connector.getConfig());
        assertEquals(1, connector.getTasks().length);
        assertEquals("connector-1", connector.getTasks()[0].getConnector());
        assertEquals(0, connector.getTasks()[0].getTask());
        assertEquals(12L, connector.getOwnerUserId());
    }

    @Test
    void kafkaMessageAndPermissionFactoriesWork() {
        KafkaMessage message = KafkaMessage.from("topic-a", "k".getBytes(), "v".getBytes(), 5);
        assertEquals("topic-a", message.getTopic());
        assertArrayEquals("k".getBytes(), message.getKey());
        assertArrayEquals("v".getBytes(), message.getPayload());
        assertEquals(5, message.getPartition());
        assertTrue(message.toString().contains("topic-a"));

        KafkaPermission permission = new KafkaPermission("topic-a", PatternType.PREFIXED, AclOperation.WRITE, AclPermissionType.DENY);
        assertEquals("topic-a", permission.getTopic());
        assertEquals(PatternType.PREFIXED, permission.getPatternType());
        assertEquals(AclOperation.WRITE, permission.getAclOperation());
        assertEquals(AclPermissionType.DENY, permission.getAclPermissionType());
    }

    @Test
    void restMessageEnumsAndConstantsAreAvailable() {
        KafkaRestMessage restMessage = new KafkaRestMessage("k", "payload");
        assertEquals("k", restMessage.getKey());
        assertEquals("payload", restMessage.getContent());

        assertEquals("DROP_CONNECTION", MqttMessageType.DROP_CONNECTION.name());
        assertEquals("PROCESS_EVENT", SystemMessageType.PROCESS_EVENT.name());

        assertNotNull(KafkaConnectorConstants.HYPERIOT_KAFKA_SYSTEM_CONSUMER_MAX_THREADS);
        assertNotNull(KafkaConnectorConstants.HYPERIOT_KAFKA_OSGI_CONNECT_URL);
    }
}
