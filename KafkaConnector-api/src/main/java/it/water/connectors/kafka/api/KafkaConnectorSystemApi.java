package it.water.connectors.kafka.api;

import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.KafkaMessage;
import it.water.connectors.kafka.model.KafkaPermission;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.core.api.service.BaseEntitySystemApi;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public interface KafkaConnectorSystemApi extends BaseEntitySystemApi<KafkaConnector> {
    void registerMessageReceiver(KafkaMessageReceiver receiver, String topic);

    void unregisterMessageReceiver(KafkaMessageReceiver receiver, String topic);

    void stopConsumingFromKafka();

    void startConsumingFromKafka(List<String> topics);

    void produceMessage(KafkaMessage message, Callback callback);

    void produceMessage(KafkaMessage message);

    void produceMessage(KafkaMessage message, Producer<byte[], byte[]> producer, Callback callback);

    KafkaProducer<byte[], byte[]> getNewProducer();

    KafkaProducerPool getNewProducerPool(int poolSize);

    KafkaProducer<byte[], byte[]> getNewProducer(String clientId);

    @SuppressWarnings("rawtypes")
    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, List<String> topics, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, List<String> topics, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass, KafkaPartitionAssignListener assignListener, KafkaPartitionRevokeListener revokeListener) throws ClassNotFoundException;

    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, List<String> topics, List<Integer> partitions, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass, KafkaPartitionAssignListener assignListener, KafkaPartitionRevokeListener revokeListener) throws ClassNotFoundException;

    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, String topic, int partition, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    void consumeReactiveAsSystem(String kafkaGroupId, Pattern topicPattern, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    void consumeReactiveAsSystem(String kafkaGroupId, String topic, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

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

    String getClusterSystemTopic();
}
