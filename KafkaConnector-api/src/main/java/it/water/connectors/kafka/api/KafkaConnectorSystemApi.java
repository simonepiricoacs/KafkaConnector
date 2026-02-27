package it.water.connectors.kafka.api;

import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.connectors.kafka.model.KafkaMessage;
import it.water.connectors.kafka.model.KafkaPermission;
import it.water.core.api.service.BaseSystemApi;
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

/**
 * System-level API for the Kafka connector.
 * Exposes internal integration operations such as system consumers/producers,
 * receiver registration, Kafka admin operations and Kafka Connect management.
 *
 * Management operations (topics, ACLs, connectors) are inherited from KafkaConnectorManagementApi.
 */
public interface KafkaConnectorSystemApi extends BaseSystemApi, KafkaConnectorManagementApi {
    /**
     * Registers a message receiver for a specific topic (or wildcard topic, e.g. {@code *}).
     *
     * @param receiver receiver to register
     * @param topic topic name (or wildcard)
     */
    void registerMessageReceiver(KafkaMessageReceiver receiver, String topic);

    /**
     * Unregisters a previously registered message receiver for a topic.
     *
     * @param receiver receiver to unregister
     * @param topic topic name (or wildcard)
     */
    void unregisterMessageReceiver(KafkaMessageReceiver receiver, String topic);

    /**
     * Stops internal Kafka system consumers started by the connector.
     */
    void stopConsumingFromKafka();

    /**
     * Starts internal Kafka system consumers on the provided topics.
     * Typically invoked during component activation.
     *
     * @param topics topics used for system-level communication
     */
    void startConsumingFromKafka(List<String> topics);

    /**
     * Produces a Kafka message using the default producer and a custom callback.
     *
     * @param message Kafka message to produce
     * @param callback Kafka producer callback
     */
    void produceMessage(KafkaMessage message, Callback callback);

    /**
     * Produces a Kafka message using the default producer and default callback handling.
     *
     * @param message Kafka message to produce
     */
    void produceMessage(KafkaMessage message);

    /**
     * Produces a Kafka message using the provided producer instance.
     *
     * @param message Kafka message to produce
     * @param producer producer to use
     * @param callback Kafka producer callback (nullable)
     */
    void produceMessage(KafkaMessage message, Producer<byte[], byte[]> producer, Callback callback);

    /**
     * Creates a new Kafka producer using connector configuration.
     *
     * @return configured Kafka producer, or {@code null} if configuration is missing/invalid
     */
    KafkaProducer<byte[], byte[]> getNewProducer();

    /**
     * Creates a producer pool with the specified number of producers.
     *
     * @param poolSize number of pooled producers
     * @return producer pool instance
     */
    KafkaProducerPool getNewProducerPool(int poolSize);

    /**
     * Creates a new Kafka producer using connector configuration and a custom client id.
     *
     * @param clientId Kafka producer client id
     * @return configured Kafka producer, or {@code null} if configuration is missing/invalid
     */
    KafkaProducer<byte[], byte[]> getNewProducer(String clientId);

    /**
     * Starts reactive consumption (Reactor Flux) from one or more Kafka topics.
     *
     * @param kafkaGroupId Kafka consumer group id
     * @param topics list of Kafka topics
     * @param pollTime poll timeout in milliseconds
     * @param keyDeserializerClass Kafka key deserializer class
     * @param valueDeserializerClass Kafka value deserializer class
     * @return a Flux of Kafka receiver records
     * @throws ClassNotFoundException if a deserializer class cannot be resolved
     */
    @SuppressWarnings("rawtypes")
    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, List<String> topics, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    /**
     * Starts reactive consumption from one or more topics with partition assign/revoke listeners.
     *
     * @param kafkaGroupId Kafka consumer group id
     * @param topics list of Kafka topics
     * @param pollTime poll timeout in milliseconds
     * @param keyDeserializerClass Kafka key deserializer class
     * @param valueDeserializerClass Kafka value deserializer class
     * @param assignListener partition assignment listener
     * @param revokeListener partition revoke listener
     * @return a Flux of Kafka receiver records
     * @throws ClassNotFoundException if a deserializer class cannot be resolved
     */
    @SuppressWarnings("rawtypes")
    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, List<String> topics, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass, KafkaPartitionAssignListener assignListener, KafkaPartitionRevokeListener revokeListener) throws ClassNotFoundException;

    /**
     * Starts reactive consumption with explicit topic/partition assignment.
     *
     * @param kafkaGroupId Kafka consumer group id
     * @param topics list of Kafka topics
     * @param partitions partitions to assign
     * @param pollTime poll timeout in milliseconds
     * @param keyDeserializerClass Kafka key deserializer class
     * @param valueDeserializerClass Kafka value deserializer class
     * @param assignListener partition assignment listener
     * @param revokeListener partition revoke listener
     * @return a Flux of Kafka receiver records
     * @throws ClassNotFoundException if a deserializer class cannot be resolved
     */
    @SuppressWarnings("rawtypes")
    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, List<String> topics, List<Integer> partitions, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass, KafkaPartitionAssignListener assignListener, KafkaPartitionRevokeListener revokeListener) throws ClassNotFoundException;

    /**
     * Starts reactive consumption from a specific topic partition.
     *
     * @param kafkaGroupId Kafka consumer group id
     * @param topic Kafka topic name
     * @param partition Kafka partition id
     * @param pollTime poll timeout in milliseconds
     * @param keyDeserializerClass Kafka key deserializer class
     * @param valueDeserializerClass Kafka value deserializer class
     * @return a Flux of Kafka receiver records
     * @throws ClassNotFoundException if a deserializer class cannot be resolved
     */
    @SuppressWarnings("rawtypes")
    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, String topic, int partition, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    /**
     * Starts reactive consumption for system usage by subscribing to topics matching a pattern.
     *
     * @param kafkaGroupId Kafka consumer group id
     * @param topicPattern Kafka topic regex pattern
     * @param pollTime poll timeout in milliseconds
     * @param keyDeserializerClass Kafka key deserializer class
     * @param valueDeserializerClass Kafka value deserializer class
     * @throws ClassNotFoundException if a deserializer class cannot be resolved
     */
    @SuppressWarnings("rawtypes")
    void consumeReactiveAsSystem(String kafkaGroupId, Pattern topicPattern, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    /**
     * Starts reactive consumption for system usage on a single topic.
     *
     * @param kafkaGroupId Kafka consumer group id
     * @param topic Kafka topic name
     * @param pollTime poll timeout in milliseconds
     * @param keyDeserializerClass Kafka key deserializer class
     * @param valueDeserializerClass Kafka value deserializer class
     * @throws ClassNotFoundException if a deserializer class cannot be resolved
     */
    @SuppressWarnings("rawtypes")
    void consumeReactiveAsSystem(String kafkaGroupId, String topic, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    /**
     * Returns the Kafka topic used for cluster/system communication.
     *
     * @return cluster system topic name
     */
    String getClusterSystemTopic();
}
