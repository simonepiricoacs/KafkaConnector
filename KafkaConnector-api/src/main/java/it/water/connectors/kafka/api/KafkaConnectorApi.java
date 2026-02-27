package it.water.connectors.kafka.api;

import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.connectors.kafka.model.KafkaPermission;
import it.water.core.api.service.BaseApi;
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

/**
 * Public API exposed by the Kafka connector module.
 * Provides reactive consume operations (public variants).
 *
 * Management operations (topics, ACLs, connectors) are inherited from KafkaConnectorManagementApi,
 * ensuring authorization checks are applied before delegation to system API.
 */
public interface KafkaConnectorApi extends BaseApi, KafkaConnectorManagementApi {

    /**
     * Starts reactive consumption (Reactor Flux) from one or more Kafka topics.
     *
     * @param kafkaGroupId Kafka consumer group id
     * @param topics list of Kafka topics to subscribe to
     * @param pollTime poll timeout in milliseconds
     * @param keyDeserializerClass Kafka key deserializer class
     * @param valueDeserializerClass Kafka value deserializer class
     * @return a Flux of Kafka receiver records
     * @throws ClassNotFoundException if a deserializer class cannot be resolved
     */
    @SuppressWarnings("rawtypes")
    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, List<String> topics, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

    /**
     * Starts reactive consumption (Reactor Flux) from a specific topic partition.
     *
     * @param kafkaGroupId Kafka consumer group id
     * @param topic Kafka topic name
     * @param pollTime poll timeout in milliseconds
     * @param partition Kafka partition id
     * @param keyDeserializerClass Kafka key deserializer class
     * @param valueDeserializerClass Kafka value deserializer class
     * @return a Flux of Kafka receiver records
     * @throws ClassNotFoundException if a deserializer class cannot be resolved
     */
    @SuppressWarnings("rawtypes")
    Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, String topic, long pollTime, int partition, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException;

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
}
