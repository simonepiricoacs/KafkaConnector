package it.water.connectors.kafka.service;

import it.water.connectors.kafka.api.KafkaConnectorApi;
import it.water.connectors.kafka.api.KafkaConnectorSystemApi;
import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.KafkaPermission;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.core.api.registry.ComponentRegistry;
import it.water.core.interceptors.annotations.FrameworkComponent;
import it.water.core.interceptors.annotations.Inject;
import it.water.repository.service.BaseEntityServiceImpl;
import lombok.Getter;
import lombok.Setter;
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

@FrameworkComponent
public class KafkaConnectorServiceImpl extends BaseEntityServiceImpl<KafkaConnector> implements KafkaConnectorApi {

    @Inject
    @Getter
    @Setter
    private KafkaConnectorSystemApi systemService;

    @Inject
    @Getter
    @Setter
    private ComponentRegistry componentRegistry;

    public KafkaConnectorServiceImpl() {
        super(KafkaConnector.class);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, List<String> topics, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException {
        return systemService.consumeReactive(kafkaGroupId, topics, pollTime, keyDeserializerClass, valueDeserializerClass);
    }

    @Override
    public Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, String topic, long pollTime, int partition, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException {
        return systemService.consumeReactive(kafkaGroupId, topic, partition, pollTime, keyDeserializerClass, valueDeserializerClass);
    }

    @Override
    public void consumeReactiveAsSystem(String kafkaGroupId, Pattern topicPattern, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException {
        systemService.consumeReactiveAsSystem(kafkaGroupId, topicPattern, pollTime, keyDeserializerClass, valueDeserializerClass);
    }

    @Override
    public CreateTopicsResult adminCreateTopic(String topic, int numPartitions, short numReplicas) {
        return systemService.adminCreateTopic(topic, numPartitions, numReplicas);
    }

    @Override
    public CreateTopicsResult adminCreateTopic(String[] topics, int[] numPartitions, short[] numReplicas) {
        return systemService.adminCreateTopic(topics, numPartitions, numReplicas);
    }

    @Override
    public DeleteTopicsResult adminDropTopic(List<String> topics) {
        return systemService.adminDropTopic(topics);
    }

    @Override
    public CreateAclsResult adminAddACLs(String username, Map<String, KafkaPermission> permissions) {
        return systemService.adminAddACLs(username, permissions);
    }

    @Override
    public DeleteAclsResult adminDeleteACLs(String username, Map<String, KafkaPermission> permissions) {
        return systemService.adminDeleteACLs(username, permissions);
    }

    @Override
    public KafkaConnector addNewConnector(String instanceName, ConnectorConfig config) throws IOException {
        return systemService.addNewConnector(instanceName, config);
    }

    @Override
    public void deleteConnector(String instanceName, boolean deleteKafkaTopic) throws IOException {
        systemService.deleteConnector(instanceName, deleteKafkaTopic);
    }

    @Override
    public KafkaConnector getConnector(String instanceName) throws IOException {
        return systemService.getConnector(instanceName);
    }

    @Override
    public List<String> listConnectors() throws IOException {
        return systemService.listConnectors();
    }

    @Override
    public KafkaConnector updateConnector(String instanceName, ConnectorConfig config) throws IOException {
        return systemService.updateConnector(instanceName, config);
    }
}
