package it.water.connectors.kafka.api;

import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.KafkaPermission;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.core.api.service.BaseEntityApi;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public interface KafkaConnectorApi extends BaseEntityApi<KafkaConnector> {
    @SuppressWarnings("rawtypes")
    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, List<String> topics, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, String topic, long pollTime, int partition, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    void consumeReactiveAsSystem(String kafkaGroupId, Pattern topicPattern, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    CreateTopicsResult adminCreateTopic(String topic, int numPartitions, short numReplicas);

    CreateTopicsResult adminCreateTopic(String[] topics, int[] numPartitions, short[] numReplicas);

    DeleteTopicsResult adminDropTopic(List<String> topics);

    CreateAclsResult adminAddACLs(String username, Map<String, KafkaPermission> permissions);

    DeleteAclsResult adminDeleteACLs(String username, Map<String, KafkaPermission> permissions);

    KafkaConnector addNewConnector(String instanceName, ConnectorConfig config) throws IOException;

    void deleteConnector(String instanceName, boolean deleteKafkaTopic) throws IOException;

    KafkaConnector getConnector(String instanceName) throws IOException;

    List<String> listConnectors() throws IOException;

    KafkaConnector updateConnector(String instanceName, ConnectorConfig config) throws IOException;
}
