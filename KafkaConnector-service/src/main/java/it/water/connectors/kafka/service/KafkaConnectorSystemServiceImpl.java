package it.water.connectors.kafka.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.water.connectors.kafka.api.*;
import it.water.connectors.kafka.consumer.KafkaConsumerThread;
import it.water.connectors.kafka.consumer.KafkaGlobalNotifier;
import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.connectors.kafka.model.KafkaMessage;
import it.water.connectors.kafka.model.KafkaPermission;
import it.water.connectors.kafka.producer.KafkaProducerPoolImpl;
import it.water.connectors.kafka.util.KafkaConnectorConstants;
import it.water.core.api.bundle.ApplicationProperties;
import it.water.core.api.interceptors.OnActivate;
import it.water.core.api.interceptors.OnDeactivate;
import it.water.core.api.registry.filter.ComponentFilterBuilder;
import it.water.core.interceptors.annotations.FrameworkComponent;
import it.water.core.interceptors.annotations.Inject;
import it.water.core.service.BaseSystemServiceImpl;
import lombok.Setter;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

@FrameworkComponent
public class KafkaConnectorSystemServiceImpl extends BaseSystemServiceImpl implements KafkaConnectorSystemApi {

    public static final String CONSUMER_REACTIVE_ON_PARTITIONS_ASSIGNED = "Consumer Reactive onPartitionsAssigned {}";
    public static final String CONSUMER_REACTIVE_ON_PARTITIONS_REVOKED = "Consumer Reactive onPartitionsRevoked {}";
    public static final String BOOTSTRAP_SERVERS = ".bootstrap.servers";
    public static final String CONTENT_TYPE = "application/json";

    @Inject
    @Setter
    private ComponentFilterBuilder componentFilterBuilder;

    private static final String KAFKA_CONNECT_SERVICE_PATH = "/connectors";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private final List<KafkaConsumerThread> kcts;
    private final Map<String, List<KafkaMessageReceiver>> messageReceivers;

    private Producer<byte[], byte[]> producer;
    private AdminClient adminClient;

    private ExecutorService executor;
    private ExecutorService notifierExecutor;

    private final Properties consumerProperties;
    private final Properties systemConsumerProperties;
    private final Properties producerProperties;
    private final Properties adminProperties;
    private final Properties connectorProperties;

    private String kafkaConnectUrl;
    private String layerName;
    private String nodeId;
    private int kafkaSystemConsumerMaxThreads;

    private Scheduler reactorScheduler;

    public KafkaConnectorSystemServiceImpl() {
        this.kcts = new CopyOnWriteArrayList<>();
        this.messageReceivers = new ConcurrentHashMap<>();
        this.consumerProperties = new Properties();
        this.systemConsumerProperties = new Properties();
        this.producerProperties = new Properties();
        this.adminProperties = new Properties();
        this.connectorProperties = new Properties();
        this.layerName = "water";
        this.nodeId = "default";
        this.kafkaSystemConsumerMaxThreads = 5;
    }

    @OnActivate
    public void activate(ApplicationProperties applicationProperties) {
        getLog().info("Activating Kafka Connector");
        try {
            this.layerName = applicationProperties.getPropertyOrDefault("it.water.layer", "water");
            this.nodeId = applicationProperties.getPropertyOrDefault("it.water.node.id", "default");
            this.loadKafkaConfiguration(applicationProperties);
            if (!adminProperties.isEmpty()) {
                this.adminClient = AdminClient.create(adminProperties);
            }
            this.kafkaSystemConsumerMaxThreads = Integer.parseInt(
                connectorProperties.getProperty(
                    KafkaConnectorConstants.WATER_KAFKA_SYSTEM_CONSUMER_MAX_THREADS,
                    "5"
                )
            );
            int reactorMaxThreads = Integer.parseInt(
                connectorProperties.getProperty(
                    KafkaConnectorConstants.WATER_KAFKA_REACTOR_PROP_MAX_CONSUMER_THREADS,
                    "1"
                )
            );
            this.reactorScheduler = Schedulers.fromExecutor(Executors.newFixedThreadPool(reactorMaxThreads));
            this.startConsumingFromKafka(this.createBasicTopics());
        } catch (Throwable t) {
            getLog().error(t.getMessage(), t);
        }
    }

    @OnDeactivate
    public void deactivate() {
        getLog().info("Deactivating Kafka Connector");
        this.stopConsumingFromKafka();
        if (this.reactorScheduler != null) {
            this.reactorScheduler.dispose();
        }
        if (this.notifierExecutor != null) {
            this.notifierExecutor.shutdown();
        }
    }

    @Override
    public void registerMessageReceiver(KafkaMessageReceiver receiver, String topic) {
        this.messageReceivers.computeIfAbsent(topic, key -> new CopyOnWriteArrayList<>()).add(receiver);
    }

    @Override
    public void unregisterMessageReceiver(KafkaMessageReceiver receiver, String topic) {
        List<KafkaMessageReceiver> receivers = this.messageReceivers.get(topic);
        if (receivers != null) {
            receivers.remove(receiver);
        }
    }

    @Override
    public void stopConsumingFromKafka() {
        this.kcts.forEach(KafkaConsumerThread::stop);
        this.kcts.clear();
        if (this.executor != null) {
            this.executor.shutdown();
            try {
                this.executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        if (this.adminClient != null) {
            this.adminClient.close(Duration.ofSeconds(10));
        }
    }

    @Override
    public void startConsumingFromKafka(List<String> basicTopics) {
        if (this.consumerProperties.isEmpty()) {
            getLog().warn("No kafka consumer properties found, system consumer not started");
            return;
        }
        try {
            this.consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            this.consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            this.consumerProperties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "water_" + this.layerName + "_" + this.nodeId
            );

            this.executor = Executors.newFixedThreadPool(this.kafkaSystemConsumerMaxThreads);
            this.notifierExecutor = Executors.newFixedThreadPool(this.kafkaSystemConsumerMaxThreads);
            for (int i = 0; i < this.kafkaSystemConsumerMaxThreads; i++) {
                Properties threadProperties = new Properties();
                threadProperties.putAll(this.consumerProperties);
                threadProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, this.layerName + "_" + this.nodeId + "-" + i);
                KafkaConsumerThread thread = new KafkaConsumerThread(
                    threadProperties,
                    this.systemConsumerProperties,
                    basicTopics,
                    this.notifierExecutor,
                    this::notifyMessageReceivers
                );
                this.kcts.add(thread);
                this.executor.submit(thread);
            }
        } catch (Exception e) {
            getLog().error(e.getMessage(), e);
        }
    }

    @Override
    public void produceMessage(KafkaMessage message, Callback callback) {
        if (this.producerProperties.isEmpty()) {
            return;
        }
        if (this.producer == null) {
            this.producer = this.getNewProducer();
        }
        this.produceMessage(message, this.producer, callback);
    }

    @Override
    public void produceMessage(KafkaMessage message, Producer<byte[], byte[]> producer, Callback callback) {
        ProducerRecord<byte[], byte[]> recordP;
        if (message.getPartition() >= 0) {
            recordP = new ProducerRecord<>(message.getTopic(), message.getPartition(), message.getKey(), message.getPayload());
        } else {
            recordP = new ProducerRecord<>(message.getTopic(), message.getKey(), message.getPayload());
        }
        final long beforeSendTime = System.currentTimeMillis();
        if (callback != null) {
            producer.send(recordP, callback);
        } else {
            producer.send(recordP, (RecordMetadata metadata, Exception exception) -> {
                if (exception != null) {
                    getLog().error(exception.getMessage(), exception);
                } else if (getLog().isDebugEnabled()) {
                    getLog().debug("Produced message in {}", (System.currentTimeMillis() - beforeSendTime));
                }
            });
        }
    }

    @Override
    public void produceMessage(KafkaMessage message) {
        this.produceMessage(message, (Callback) null);
    }

    @Override
    public KafkaProducer<byte[], byte[]> getNewProducer() {
        return this.getNewProducer(null);
    }

    @Override
    public KafkaProducerPool getNewProducerPool(int poolSize) {
        return new KafkaProducerPoolImpl(poolSize, this);
    }

    @Override
    public KafkaProducer<byte[], byte[]> getNewProducer(String clientId) {
        if (this.producerProperties.isEmpty()) {
            return null;
        }
        Properties props = new Properties();
        props.putAll(this.producerProperties);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        if (clientId != null) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }
        try {
            return new KafkaProducer<>(props);
        } catch (Exception e) {
            getLog().error(e.getMessage(), e);
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(
        String kafkaGroupId,
        List<String> topics,
        List<Integer> partitions,
        long pollTime,
        Class keyDeserializerClass,
        Class valueDeserializerClass,
        KafkaPartitionAssignListener assignListener,
        KafkaPartitionRevokeListener revokeListener
    ) {
        ReceiverOptions<byte[], byte[]> options = createReceiverOptionsForTopics(
            kafkaGroupId,
            topics,
            partitions,
            pollTime,
            keyDeserializerClass,
            valueDeserializerClass,
            assignListener,
            revokeListener
        );
        if (options == null) {
            return null;
        }
        return KafkaReceiver.create(options).receive().subscribeOn(reactorScheduler);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, List<String> topics, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) {
        return this.consumeReactive(
            kafkaGroupId,
            topics,
            null,
            pollTime,
            keyDeserializerClass,
            valueDeserializerClass,
            partitions -> getLog().debug(CONSUMER_REACTIVE_ON_PARTITIONS_ASSIGNED, partitions),
            partitions -> getLog().debug(CONSUMER_REACTIVE_ON_PARTITIONS_REVOKED, partitions)
        );
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(
        String kafkaGroupId,
        List<String> topics,
        long pollTime,
        Class keyDeserializerClass,
        Class valueDeserializerClass,
        KafkaPartitionAssignListener assignListener,
        KafkaPartitionRevokeListener revokeListener
    ) {
        return this.consumeReactive(kafkaGroupId, topics, null, pollTime, keyDeserializerClass, valueDeserializerClass, assignListener, revokeListener);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Flux<ReceiverRecord<byte[], byte[]>> consumeReactive(String kafkaGroupId, String topic, int partition, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException {
        return this.consumeReactive(
            kafkaGroupId,
            Collections.singletonList(topic),
            Collections.singletonList(partition),
            pollTime,
            keyDeserializerClass,
            valueDeserializerClass,
            partitions -> getLog().debug(CONSUMER_REACTIVE_ON_PARTITIONS_ASSIGNED, partitions),
            partitions -> getLog().debug(CONSUMER_REACTIVE_ON_PARTITIONS_REVOKED, partitions)
        );
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void consumeReactiveAsSystem(String kafkaGroupId, Pattern topicPattern, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException {
        ReceiverOptions<byte[], byte[]> options = createReceiverOptionsForPattern(
            kafkaGroupId,
            topicPattern,
            pollTime,
            keyDeserializerClass,
            valueDeserializerClass
        );
        if (options != null) {
            Scheduler processingScheduler = this.reactorScheduler != null ? this.reactorScheduler : Schedulers.boundedElastic();
            KafkaReceiver.create(options)
                .receive()
                .subscribeOn(processingScheduler)
                .parallel()
                .runOn(processingScheduler)
                .subscribe(m -> {
                    String topic = m.topic();
                    byte[] key = m.key();
                    int partition = m.partition();
                    KafkaMessage message = KafkaMessage.from(topic, key, m.value(), partition);
                    this.notifyMessageReceivers(message);
                });
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void consumeReactiveAsSystem(String kafkaGroupId, String topic, long pollTime, Class keyDeserializerClass, Class valueDeserializerClass) throws ClassNotFoundException {
        ReceiverOptions<byte[], byte[]> options = createReceiverOptionsForTopics(
            kafkaGroupId,
            Collections.singletonList(topic),
            null,
            pollTime,
            keyDeserializerClass,
            valueDeserializerClass,
            partitions -> getLog().debug(CONSUMER_REACTIVE_ON_PARTITIONS_ASSIGNED, partitions),
            partitions -> getLog().debug(CONSUMER_REACTIVE_ON_PARTITIONS_REVOKED, partitions)
        );
        if (options != null) {
            Scheduler processingScheduler = this.reactorScheduler != null ? this.reactorScheduler : Schedulers.boundedElastic();
            KafkaReceiver.create(options)
                .receive()
                .subscribeOn(processingScheduler)
                .parallel()
                .runOn(processingScheduler)
                .subscribe(m -> {
                    String currentTopic = m.topic();
                    byte[] key = m.key();
                    int partition = m.partition();
                    KafkaMessage message = KafkaMessage.from(currentTopic, key, m.value(), partition);
                    this.notifyMessageReceivers(message);
                });
        }
    }

    @Override
    public CreateTopicsResult adminCreateTopic(String topic, int numPartitions, short numReplicas) {
        if (this.adminClient == null) {
            return null;
        }
        return this.adminClient.createTopics(Collections.singletonList(new NewTopic(topic, numPartitions, numReplicas)));
    }

    @Override
    public CreateTopicsResult adminCreateTopic(String[] topics, int[] numPartitions, short[] numReplicas) {
        if (this.adminClient == null) {
            return null;
        }
        if (topics.length != numPartitions.length || topics.length != numReplicas.length) {
            return null;
        }
        List<NewTopic> kafkaTopics = new ArrayList<>();
        for (int i = 0; i < topics.length; i++) {
            kafkaTopics.add(new NewTopic(topics[i], numPartitions[i], numReplicas[i]));
        }
        return this.adminClient.createTopics(kafkaTopics);
    }

    @Override
    public DeleteTopicsResult adminDropTopic(List<String> topics) {
        if (this.adminClient == null) {
            return null;
        }
        return this.adminClient.deleteTopics(topics);
    }

    @Override
    public CreateAclsResult adminAddACLs(String username, Map<String, KafkaPermission> permissions) {
        if (this.adminClient == null) {
            return null;
        }
        List<AclBinding> acls = new ArrayList<>();
        String principal = this.resolveAclPrincipal(username);
        for (KafkaPermission permission : permissions.values()) {
            ResourcePattern topicPattern = new ResourcePattern(ResourceType.TOPIC, permission.getTopic(), permission.getPatternType());
            AccessControlEntry aclEntry = new AccessControlEntry(principal, "*", permission.getAclOperation(), permission.getAclPermissionType());
            acls.add(new AclBinding(topicPattern, aclEntry));
        }
        return this.adminClient.createAcls(acls);
    }

    @Override
    public DeleteAclsResult adminDeleteACLs(String username, Map<String, KafkaPermission> permissions) {
        if (this.adminClient == null) {
            return null;
        }
        List<AclBindingFilter> acls = new ArrayList<>();
        String principal = this.resolveAclPrincipal(username);
        for (KafkaPermission permission : permissions.values()) {
            ResourcePatternFilter topicPattern = new ResourcePatternFilter(ResourceType.TOPIC, permission.getTopic(), permission.getPatternType());
            AccessControlEntryFilter aclEntry = new AccessControlEntryFilter(principal, "*", permission.getAclOperation(), permission.getAclPermissionType());
            acls.add(new AclBindingFilter(topicPattern, aclEntry));
        }
        return this.adminClient.deleteAcls(acls);
    }

    @Override
    public KafkaConnector addNewConnector(String instanceName, ConnectorConfig config) throws IOException {
        HttpPost addConnectorRequest = new HttpPost(connectorUrl());
        Map<String, Object> payload = new HashMap<>();
        payload.put("name", instanceName);
        payload.put("config", config);
        String json = new ObjectMapper().writeValueAsString(payload);
        String response = executeRequest(addConnectorRequest, json);
        if (response == null || response.isEmpty()) {
            return null;
        }
        return new ObjectMapper().readValue(response, KafkaConnector.class);
    }

    @Override
    public void deleteConnector(String instanceName, boolean deleteKafkaTopic) throws IOException {
        HttpDelete deleteRequest = new HttpDelete(connectorUrl() + "/" + instanceName);
        executeRequest(deleteRequest, null);
        if (deleteKafkaTopic) {
            this.adminDropTopic(Collections.singletonList(instanceName));
        }
    }

    @Override
    public KafkaConnector getConnector(String instanceName) throws IOException {
        HttpGet getRequest = new HttpGet(connectorUrl() + "/" + instanceName);
        String response = executeRequest(getRequest, null);
        if (response == null || response.isEmpty()) {
            return null;
        }
        return new ObjectMapper().readValue(response, KafkaConnector.class);
    }

    @Override
    public List<String> listConnectors() throws IOException {
        HttpGet listRequest = new HttpGet(connectorUrl());
        String response = executeRequest(listRequest, null);
        if (response == null || response.isEmpty()) {
            return Collections.emptyList();
        }
        return new ObjectMapper().readValue(response, new TypeReference<List<String>>() { });
    }

    @Override
    public KafkaConnector updateConnector(String instanceName, ConnectorConfig config) throws IOException {
        HttpPut updateConnectorRequest = new HttpPut(connectorUrl() + "/" + instanceName + "/config");
        String json = new ObjectMapper().writeValueAsString(config);
        String response = executeRequest(updateConnectorRequest, json);
        if (response == null || response.isEmpty()) {
            return null;
        }
        return new ObjectMapper().readValue(response, KafkaConnector.class);
    }

    @Override
    public String getClusterSystemTopic() {
        return KafkaConnectorConstants.WATER_KAFKA_OSGI_BASIC_TOPIC + "_" + this.layerName;
    }

    private List<String> createBasicTopics() {
        String[] topics = new String[2];
        int[] numPartitions = new int[2];
        short[] numReplicas = new short[2];
        topics[0] = getClusterSystemTopic();
        topics[1] = getClusterSystemTopic() + "_" + this.nodeId;
        int partitions = Math.max(1, this.kafkaSystemConsumerMaxThreads);
        numPartitions[0] = partitions;
        numPartitions[1] = partitions;
        numReplicas[0] = (short) 1;
        numReplicas[1] = (short) 1;
        this.adminCreateTopic(topics, numPartitions, numReplicas);
        return Arrays.asList(topics);
    }

    private void notifyMessageReceivers(KafkaMessage message) {
        List<KafkaMessageReceiver> receivers = new ArrayList<>();
        List<KafkaMessageReceiver> topicReceivers = this.messageReceivers.get(message.getTopic());
        if (topicReceivers != null) {
            receivers.addAll(topicReceivers);
        }
        List<KafkaMessageReceiver> wildcardReceivers = this.messageReceivers.get("*");
        if (wildcardReceivers != null) {
            receivers.addAll(wildcardReceivers);
        }
        for (KafkaMessageReceiver receiver : receivers) {
            try {
                receiver.receive(message);
            } catch (Throwable t) {
                getLog().error(t.getMessage(), t);
            }
        }
        KafkaGlobalNotifier.notifyKafkaMessage(message);
    }

    private ReceiverOptions<byte[], byte[]> createReceiverOptionsForPattern(
        String kafkaGroupId,
        Pattern topicPattern,
        long pollTime,
        Class<?> keyDeserializerClass,
        Class<?> valueDeserializerClass
    ) {
        if (this.consumerProperties.isEmpty()) {
            return null;
        }

        Properties props = new Properties();
        props.putAll(this.consumerProperties);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());

        return ReceiverOptions.<byte[], byte[]>create((Map<String, Object>) (Map<?, ?>) props)
            .pollTimeout(Duration.ofMillis(pollTime))
            .subscription(topicPattern);
    }

    private ReceiverOptions<byte[], byte[]> createReceiverOptionsForTopics(
        String kafkaGroupId,
        List<String> topics,
        List<Integer> partitions,
        long pollTime,
        Class<?> keyDeserializerClass,
        Class<?> valueDeserializerClass,
        KafkaPartitionAssignListener assignListener,
        KafkaPartitionRevokeListener revokeListener
    ) {
        if (this.consumerProperties.isEmpty()) {
            return null;
        }

        Properties props = new Properties();
        props.putAll(this.consumerProperties);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());

        ReceiverOptions<byte[], byte[]> options = ReceiverOptions.<byte[], byte[]>create((Map<String, Object>) (Map<?, ?>) props)
            .pollTimeout(Duration.ofMillis(pollTime));
        if (assignListener != null) {
            options = options.addAssignListener(assignListener);
        }
        if (revokeListener != null) {
            options = options.addRevokeListener(revokeListener);
        }

        if (partitions != null && !partitions.isEmpty()) {
            if (topics == null || topics.size() != partitions.size()) {
                getLog().warn(
                    "Cannot create receiver assignment: topics size {} differs from partitions size {}",
                    topics != null ? topics.size() : null,
                    partitions.size()
                );
                return null;
            }
            List<TopicPartition> topicPartitions = new ArrayList<>();
            for (int i = 0; i < topics.size(); i++) {
                topicPartitions.add(new TopicPartition(topics.get(i), partitions.get(i)));
            }
            return options.assignment(topicPartitions);
        }
        return options.subscription(topics);
    }

    private void loadKafkaConfiguration(ApplicationProperties applicationProperties) {
        copyKafkaProperty(
            consumerProperties,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            applicationProperties,
                KafkaConnectorConstants.WATER_KAFKA_BOOTSTRAP_SERVERS,
            KafkaConnectorConstants.WATER_KAFKA_PROPS_GLOBAL_PREFIX + BOOTSTRAP_SERVERS,
            KafkaConnectorConstants.WATER_KAFKA_PROPS_CONSUMER_PREFIX + BOOTSTRAP_SERVERS
        );
        copyKafkaProperty(
            producerProperties,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            applicationProperties,
                KafkaConnectorConstants.WATER_KAFKA_BOOTSTRAP_SERVERS,
            KafkaConnectorConstants.WATER_KAFKA_PROPS_GLOBAL_PREFIX + BOOTSTRAP_SERVERS,
            KafkaConnectorConstants.WATER_KAFKA_PROPS_PRODUCER_PREFIX + BOOTSTRAP_SERVERS
        );
        copyKafkaProperty(
            adminProperties,
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            applicationProperties,
                KafkaConnectorConstants.WATER_KAFKA_BOOTSTRAP_SERVERS,
            KafkaConnectorConstants.WATER_KAFKA_PROPS_GLOBAL_PREFIX + BOOTSTRAP_SERVERS,
            KafkaConnectorConstants.WATER_KAFKA_PROPS_ADMIN_PREFIX + BOOTSTRAP_SERVERS
        );

        if (!consumerProperties.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        }
        if (!producerProperties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        }
        if (!adminProperties.containsKey(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        }

        // shared optional properties
        List<String> optionalKeys = Arrays.asList(
            "security.protocol",
            "sasl.mechanism",
            "sasl.jaas.config",
            "ssl.truststore.location",
            "ssl.truststore.password",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.key.password"
        );
        for (String key : optionalKeys) {
            copyKafkaProperty(
                consumerProperties,
                key,
                applicationProperties,
                "it.water.connectors.kafka.consumer." + key,
                KafkaConnectorConstants.WATER_KAFKA_PROPS_CONSUMER_PREFIX + "." + key,
                KafkaConnectorConstants.WATER_KAFKA_PROPS_GLOBAL_PREFIX + "." + key
            );
            copyKafkaProperty(
                producerProperties,
                key,
                applicationProperties,
                "it.water.connectors.kafka.producer." + key,
                KafkaConnectorConstants.WATER_KAFKA_PROPS_PRODUCER_PREFIX + "." + key,
                KafkaConnectorConstants.WATER_KAFKA_PROPS_GLOBAL_PREFIX + "." + key
            );
            copyKafkaProperty(
                adminProperties,
                key,
                applicationProperties,
                "it.water.connectors.kafka.admin." + key,
                KafkaConnectorConstants.WATER_KAFKA_PROPS_ADMIN_PREFIX + "." + key,
                KafkaConnectorConstants.WATER_KAFKA_PROPS_GLOBAL_PREFIX + "." + key
            );
        }

        this.kafkaConnectUrl = firstNonBlank(
            readProperty(applicationProperties, "it.water.connectors.kafka.connect.url"),
            readProperty(applicationProperties, KafkaConnectorConstants.WATER_KAFKA_OSGI_CONNECT_URL),
            "http://localhost:8083"
        );
        if (this.kafkaConnectUrl != null && !this.kafkaConnectUrl.startsWith("http://") && !this.kafkaConnectUrl.startsWith("https://")) {
            this.kafkaConnectUrl = "http://" + this.kafkaConnectUrl;
        }

        connectorProperties.put(
            KafkaConnectorConstants.WATER_KAFKA_SYSTEM_CONSUMER_MAX_THREADS,
            firstNonBlank(
                readProperty(applicationProperties, "it.water.connectors.kafka.system.max.consumer.threads"),
                readProperty(applicationProperties, KafkaConnectorConstants.WATER_KAFKA_SYSTEM_CONSUMER_MAX_THREADS),
                "5"
            )
        );
        connectorProperties.put(
            KafkaConnectorConstants.WATER_KAFKA_REACTOR_PROP_MAX_CONSUMER_THREADS,
            firstNonBlank(
                readProperty(applicationProperties, "it.water.connectors.kafka.reactor.max.consumer.threads"),
                readProperty(applicationProperties, KafkaConnectorConstants.WATER_KAFKA_REACTOR_PROP_MAX_CONSUMER_THREADS),
                "1"
            )
        );
        systemConsumerProperties.put(
            KafkaConnectorConstants.WATER_KAFKA_SYSTEM_CONSUMER_POLL_MS,
            firstNonBlank(
                readProperty(applicationProperties, "it.water.connectors.kafka.system.consumer.poll.ms"),
                readProperty(applicationProperties, KafkaConnectorConstants.WATER_KAFKA_SYSTEM_CONSUMER_POLL_MS),
                "500"
            )
        );
    }

    private void copyKafkaProperty(Properties target, String targetKey, ApplicationProperties ap, String... candidateKeys) {
        for (String key : candidateKeys) {
            String value = readProperty(ap, key);
            if (value != null && !value.isBlank()) {
                target.put(targetKey, value);
                return;
            }
        }
    }

    private String readProperty(ApplicationProperties ap, String key) {
        if (ap.containsKey(key)) {
            Object value = ap.getProperty(key);
            if (value != null) {
                return String.valueOf(value);
            }
        }
        return null;
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private String connectorUrl() {
        return this.kafkaConnectUrl + KAFKA_CONNECT_SERVICE_PATH;
    }

    private String executeRequest(HttpUriRequest request, String requestBody) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            request.addHeader("Accept", CONTENT_TYPE);
            if (request instanceof HttpPost httpPost && requestBody != null) {
                httpPost.setEntity(new StringEntity(requestBody, StandardCharsets.UTF_8));
                request.addHeader("Content-Type", CONTENT_TYPE);
            }
            if (request instanceof HttpPut httpPut && requestBody != null) {
                httpPut.setEntity(new StringEntity(requestBody, StandardCharsets.UTF_8));
                request.addHeader("Content-Type", CONTENT_TYPE);
            }
            try (CloseableHttpResponse response = client.execute(request)) {
                int statusCode = response.getStatusLine() != null ? response.getStatusLine().getStatusCode() : 0;
                String responseBody = readResponseBody(response);
                if (statusCode >= 200 && statusCode < 300) {
                    return responseBody;
                }
                StringBuilder errorMessage = new StringBuilder();
                errorMessage
                    .append("Kafka Connect request failed [")
                    .append(statusCode)
                    .append("] ")
                    .append(request.getMethod())
                    .append(" ")
                    .append(request.getURI());
                if (responseBody != null && !responseBody.isBlank()) {
                    errorMessage.append(" - ").append(responseBody);
                }
                throw new IOException(errorMessage.toString());
            }
        }
    }

    private String readResponseBody(CloseableHttpResponse response) throws IOException {
        if (response.getEntity() == null) {
            return "";
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
            return result.toString();
        }
    }

    private String resolveAclPrincipal(String username) {
        if (username == null) {
            return null;
        }
        return username.trim();
    }
}
