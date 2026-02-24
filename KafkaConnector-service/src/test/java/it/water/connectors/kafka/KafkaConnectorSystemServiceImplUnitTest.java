package it.water.connectors.kafka;

import it.water.connectors.kafka.api.KafkaMessageReceiver;
import it.water.connectors.kafka.consumer.KafkaGlobalNotifier;
import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.connectors.kafka.model.KafkaMessage;
import it.water.connectors.kafka.model.KafkaPermission;
import it.water.connectors.kafka.service.KafkaConnectorSystemServiceImpl;
import it.water.connectors.kafka.util.KafkaConnectorConstants;
import it.water.core.api.bundle.ApplicationProperties;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class KafkaConnectorSystemServiceImplUnitTest {

    private KafkaConnectorSystemServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new KafkaConnectorSystemServiceImpl();
        KafkaGlobalNotifier.getRegisteredNotifiers().clear();
    }

    @AfterEach
    void tearDown() {
        service.stopConsumingFromKafka();
        KafkaGlobalNotifier.getRegisteredNotifiers().clear();
    }

    @Test
    void registerAndNotifyReceiversUsesTopicAndWildcard() throws Exception {
        KafkaMessageReceiver topicReceiver = mock(KafkaMessageReceiver.class);
        KafkaMessageReceiver wildcardReceiver = mock(KafkaMessageReceiver.class);

        service.registerMessageReceiver(topicReceiver, "topic-a");
        service.registerMessageReceiver(wildcardReceiver, "*");

        invokeNotify(KafkaMessage.from("topic-a", "k".getBytes(), "v".getBytes(), 1));

        verify(topicReceiver, times(1)).receive(any(KafkaMessage.class));
        verify(wildcardReceiver, times(1)).receive(any(KafkaMessage.class));

        service.unregisterMessageReceiver(topicReceiver, "topic-a");
        invokeNotify(KafkaMessage.from("topic-a", null, "v2".getBytes(), 2));
        verify(topicReceiver, times(1)).receive(any(KafkaMessage.class));
        verify(wildcardReceiver, times(2)).receive(any(KafkaMessage.class));
    }

    @Test
    void stopConsumingFromKafkaStopsThreadsExecutorAndAdmin() throws Exception {
        it.water.connectors.kafka.consumer.KafkaConsumerThread thread = mock(it.water.connectors.kafka.consumer.KafkaConsumerThread.class);
        List<it.water.connectors.kafka.consumer.KafkaConsumerThread> threads = getField(service, "kcts", List.class);
        threads.add(thread);

        ExecutorService executor = mock(ExecutorService.class);
        when(executor.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        setField(service, "executor", executor);

        AdminClient adminClient = mock(AdminClient.class);
        setField(service, "adminClient", adminClient);

        service.stopConsumingFromKafka();

        verify(thread).stop();
        verify(executor).shutdown();
        verify(executor).awaitTermination(10L, TimeUnit.SECONDS);
        verify(adminClient).close(Duration.ofSeconds(10));
        assertTrue(threads.isEmpty());
    }

    @Test
    void startConsumingFromKafkaReturnsWhenNoConsumerConfiguration() throws Exception {
        service.startConsumingFromKafka(Collections.singletonList("topic-a"));
        ExecutorService executor = getField(service, "executor", ExecutorService.class);
        assertNull(executor);
    }

    @Test
    void startConsumingFromKafkaCreatesConfiguredThreads() throws Exception {
        Properties consumerProps = getField(service, "consumerProperties", Properties.class);
        consumerProps.put("bootstrap.servers", "localhost:9092");
        setField(service, "kafkaSystemConsumerMaxThreads", 2);
        setField(service, "layerName", "layer");
        setField(service, "nodeId", "node");

        ExecutorService consumerExecutor = mock(ExecutorService.class);
        ExecutorService notifierExecutor = mock(ExecutorService.class);

        try (MockedStatic<java.util.concurrent.Executors> mockedExecutors = org.mockito.Mockito.mockStatic(java.util.concurrent.Executors.class);
             MockedConstruction<org.apache.kafka.clients.consumer.KafkaConsumer> ignored = org.mockito.Mockito.mockConstruction(org.apache.kafka.clients.consumer.KafkaConsumer.class)) {
            mockedExecutors.when(() -> java.util.concurrent.Executors.newFixedThreadPool(2)).thenReturn(consumerExecutor, notifierExecutor);
            when(consumerExecutor.submit(any(Runnable.class))).thenReturn(null);

            service.startConsumingFromKafka(Arrays.asList("topic-a", "topic-b"));

            List<?> threads = getField(service, "kcts", List.class);
            assertEquals(2, threads.size());
            verify(consumerExecutor, times(2)).submit(any(Runnable.class));
        }
    }

    @Test
    void produceMessageWithExistingProducerAndCallback() throws Exception {
        Producer<byte[], byte[]> producer = mock(Producer.class);
        setField(service, "producer", producer);
        Properties producerProps = getField(service, "producerProperties", Properties.class);
        producerProps.put("bootstrap.servers", "localhost:9092");

        KafkaMessage message = KafkaMessage.from("topic-a", "k".getBytes(), "v".getBytes(), 3);
        Callback callback = (metadata, exception) -> { };
        service.produceMessage(message, callback);

        ArgumentCaptor<ProducerRecord<byte[], byte[]>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(recordCaptor.capture(), eq(callback));
        assertEquals("topic-a", recordCaptor.getValue().topic());
        assertEquals(3, recordCaptor.getValue().partition());
    }

    @Test
    void produceMessageWithNullCallbackUsesDefaultCallback() {
        Producer<byte[], byte[]> producer = mock(Producer.class);
        KafkaMessage message = KafkaMessage.from("topic-a", "k".getBytes(), "v".getBytes());

        service.produceMessage(message, producer, null);

        verify(producer).send(any(ProducerRecord.class), any(Callback.class));
    }

    @Test
    void produceMessageReturnsWhenProducerPropertiesEmpty() {
        KafkaMessage message = KafkaMessage.from("topic-a", null, "v".getBytes());
        service.produceMessage(message);
        Producer<byte[], byte[]> producer = getFieldQuietly(service, "producer", Producer.class);
        assertNull(producer);
    }

    @Test
    void getNewProducerHandlesConfiguredAndMissingProperties() throws Exception {
        assertNull(service.getNewProducer());

        Properties producerProps = getField(service, "producerProperties", Properties.class);
        producerProps.put("bootstrap.servers", "localhost:9092");

        AtomicReference<Properties> capturedProps = new AtomicReference<>();
        try (MockedConstruction<KafkaProducer> construction = org.mockito.Mockito.mockConstruction(KafkaProducer.class, (mock, context) -> {
            capturedProps.set((Properties) context.arguments().get(0));
        })) {
            KafkaProducer<byte[], byte[]> created = service.getNewProducer("client-1");
            assertNotNull(created);
            assertEquals(1, construction.constructed().size());
            Properties constructorProps = capturedProps.get();
            assertEquals("client-1", constructorProps.getProperty("client.id"));
            assertNotNull(constructorProps.getProperty("key.serializer"));
            assertNotNull(constructorProps.getProperty("value.serializer"));
        }
    }

    @Test
    void consumeReactiveReturnsNullWithoutConfigAndFluxWithConfig() throws Exception {
        Flux<ReceiverRecord<byte[], byte[]>> missingConfig = service.consumeReactive(
            "group",
            Collections.singletonList("topic-a"),
            500L,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class
        );
        assertNull(missingConfig);

        Properties consumerProps = getField(service, "consumerProperties", Properties.class);
        consumerProps.put("bootstrap.servers", "localhost:9092");
        setField(service, "reactorScheduler", Schedulers.immediate());

        KafkaReceiver<byte[], byte[]> receiver = mock(KafkaReceiver.class);
        when(receiver.receive()).thenReturn(Flux.empty());

        try (MockedStatic<KafkaReceiver> mockedReceiver = org.mockito.Mockito.mockStatic(KafkaReceiver.class)) {
            mockedReceiver.when(() -> KafkaReceiver.create(any())).thenReturn(receiver);
            Flux<ReceiverRecord<byte[], byte[]>> flux = service.consumeReactive(
                "group",
                Collections.singletonList("topic-a"),
                Collections.singletonList(0),
                500L,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class,
                partitions -> { },
                partitions -> { }
            );

            assertNotNull(flux);
            mockedReceiver.verify(() -> KafkaReceiver.create(any()), atLeastOnce());
        }
    }

    @Test
    void consumeReactiveAsSystemDeliversMessagesToRegisteredReceivers() throws Exception {
        Properties consumerProps = getField(service, "consumerProperties", Properties.class);
        consumerProps.put("bootstrap.servers", "localhost:9092");

        KafkaMessageReceiver messageReceiver = mock(KafkaMessageReceiver.class);
        CountDownLatch deliveredLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            deliveredLatch.countDown();
            return null;
        }).when(messageReceiver).receive(any(KafkaMessage.class));
        service.registerMessageReceiver(messageReceiver, "topic-a");

        ReceiverRecord<byte[], byte[]> receiverRecord = mock(ReceiverRecord.class);
        when(receiverRecord.topic()).thenReturn("topic-a");
        when(receiverRecord.key()).thenReturn("k".getBytes());
        when(receiverRecord.value()).thenReturn("v".getBytes());
        when(receiverRecord.partition()).thenReturn(2);

        KafkaReceiver<byte[], byte[]> receiver = mock(KafkaReceiver.class);
        when(receiver.receive()).thenReturn(Flux.just(receiverRecord));

        try (MockedStatic<KafkaReceiver> mockedReceiver = org.mockito.Mockito.mockStatic(KafkaReceiver.class)) {
            mockedReceiver.when(() -> KafkaReceiver.create(any())).thenReturn(receiver);

            service.consumeReactiveAsSystem(
                "group",
                "topic-a",
                500L,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class
            );

            assertTrue(deliveredLatch.await(2, TimeUnit.SECONDS));
            verify(messageReceiver, atLeastOnce()).receive(any(KafkaMessage.class));
        }
    }

    @Test
    void adminOperationsReturnNullWithoutAdminAndDelegateWithAdmin() throws Exception {
        assertNull(service.adminCreateTopic("topic-a", 1, (short) 1));
        assertNull(service.adminDropTopic(Collections.singletonList("topic-a")));

        HashMap<String, KafkaPermission> permissions = new HashMap<>();
        permissions.put("topic-a", new KafkaPermission("topic-a", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW));
        assertNull(service.adminAddACLs("user", permissions));
        assertNull(service.adminDeleteACLs("user", permissions));

        AdminClient adminClient = mock(AdminClient.class);
        setField(service, "adminClient", adminClient);

        CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
        CreateAclsResult createAclsResult = mock(CreateAclsResult.class);
        DeleteAclsResult deleteAclsResult = mock(DeleteAclsResult.class);

        when(adminClient.createTopics(anyList())).thenReturn(createTopicsResult);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(adminClient.createAcls(anyList())).thenReturn(createAclsResult);
        when(adminClient.deleteAcls(anyList())).thenReturn(deleteAclsResult);

        assertSame(createTopicsResult, service.adminCreateTopic("topic-a", 1, (short) 1));
        assertSame(createTopicsResult, service.adminCreateTopic(new String[]{"topic-a"}, new int[]{1}, new short[]{1}));
        assertSame(deleteTopicsResult, service.adminDropTopic(Collections.singletonList("topic-a")));
        assertSame(createAclsResult, service.adminAddACLs("user", permissions));
        assertSame(deleteAclsResult, service.adminDeleteACLs("user", permissions));

        ArgumentCaptor<List<AclBinding>> aclCaptor = ArgumentCaptor.forClass(List.class);
        verify(adminClient, atLeastOnce()).createAcls(aclCaptor.capture());
        assertEquals("user", aclCaptor.getValue().get(0).entry().principal());
    }

    @Test
    void connectorHttpOperationsHandlePayloadsAndNullResponses() throws Exception {
        setField(service, "kafkaConnectUrl", "http://localhost:8083");

        AdminClient adminClient = mock(AdminClient.class);
        setField(service, "adminClient", adminClient);
        when(adminClient.deleteTopics(anyList())).thenReturn(mock(DeleteTopicsResult.class));

        try (MockedStatic<HttpClients> mockedHttpClients = org.mockito.Mockito.mockStatic(HttpClients.class)) {
            CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
            mockedHttpClients.when(HttpClients::createDefault).thenReturn(httpClient);

            CloseableHttpResponse postResponse = httpResponse("{\"name\":\"c1\"}");
            CloseableHttpResponse putResponse = httpResponse("{\"name\":\"c1\"}");
            CloseableHttpResponse listResponse = httpResponse("[\"c1\",\"c2\"]");
            CloseableHttpResponse emptyListResponse = httpResponse("");
            when(httpClient.execute(any(HttpPost.class))).thenReturn(postResponse);
            when(httpClient.execute(any(HttpPut.class))).thenReturn(putResponse);
            when(httpClient.execute(any(HttpGet.class))).thenReturn(listResponse, emptyListResponse);
            CloseableHttpResponse deleteResponse = mock(CloseableHttpResponse.class);
            when(deleteResponse.getEntity()).thenReturn(null);
            StatusLine deleteStatusLine = mock(StatusLine.class);
            when(deleteStatusLine.getStatusCode()).thenReturn(204);
            when(deleteResponse.getStatusLine()).thenReturn(deleteStatusLine);
            when(httpClient.execute(any(HttpDelete.class))).thenReturn(deleteResponse);

            ConnectorConfig config = new ConnectorConfig();
            config.setName("c1");

            KafkaConnector created = service.addNewConnector("c1", config);
            KafkaConnector updated = service.updateConnector("c1", config);
            List<String> connectors = service.listConnectors();
            List<String> emptyConnectors = service.listConnectors();
            service.deleteConnector("c1", true);

            assertEquals("c1", created.getName());
            assertEquals("c1", updated.getName());
            assertEquals(Arrays.asList("c1", "c2"), connectors);
            assertTrue(emptyConnectors.isEmpty());
            verify(adminClient).deleteTopics(Collections.singletonList("c1"));
        }
    }

    @Test
    void deleteConnectorThrowsIOExceptionOnNon2xxResponse() throws Exception {
        setField(service, "kafkaConnectUrl", "http://localhost:8083");
        AdminClient adminClient = mock(AdminClient.class);
        setField(service, "adminClient", adminClient);

        try (MockedStatic<HttpClients> mockedHttpClients = org.mockito.Mockito.mockStatic(HttpClients.class)) {
            CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
            mockedHttpClients.when(HttpClients::createDefault).thenReturn(httpClient);
            CloseableHttpResponse errorResponse = httpResponse("{\"error\":\"failed\"}", 500);
            when(httpClient.execute(any(HttpDelete.class))).thenReturn(errorResponse);

            assertThrows(IOException.class, () -> service.deleteConnector("c1", true));
            verify(adminClient, never()).deleteTopics(anyList());
        }
    }

    @Test
    void getConnectorReturnsNullForEmptyPayload() throws Exception {
        setField(service, "kafkaConnectUrl", "http://localhost:8083");

        try (MockedStatic<HttpClients> mockedHttpClients = org.mockito.Mockito.mockStatic(HttpClients.class)) {
            CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
            mockedHttpClients.when(HttpClients::createDefault).thenReturn(httpClient);
            CloseableHttpResponse emptyResponse = httpResponse("");
            when(httpClient.execute(any(HttpGet.class))).thenReturn(emptyResponse);

            KafkaConnector connector = service.getConnector("c1");
            assertNull(connector);
        }
    }

    @Test
    void loadKafkaConfigurationReadsWaterKeys() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("it.water.connectors.kafka.bootstrap.servers", "kafka:9092");
        values.put("it.water.connectors.kafka.connect.url", "connect-host:8083");
        values.put("it.water.connectors.kafka.system.max.consumer.threads", "3");
        values.put(KafkaConnectorConstants.WATER_KAFKA_REACTOR_PROP_MAX_CONSUMER_THREADS, "2");
        values.put(KafkaConnectorConstants.WATER_KAFKA_SYSTEM_CONSUMER_POLL_MS, "750");
        values.put("it.water.connectors.kafka.consumer.security.protocol", "SASL_SSL");
        values.put(KafkaConnectorConstants.WATER_KAFKA_PROPS_PRODUCER_PREFIX + ".ssl.key.password", "secret");

        ApplicationProperties props = mockAppProperties(values);
        Method loadMethod = KafkaConnectorSystemServiceImpl.class.getDeclaredMethod("loadKafkaConfiguration", ApplicationProperties.class);
        loadMethod.setAccessible(true);
        loadMethod.invoke(service, props);

        Properties consumerProps = getField(service, "consumerProperties", Properties.class);
        Properties producerProps = getField(service, "producerProperties", Properties.class);
        Properties adminProps = getField(service, "adminProperties", Properties.class);
        Properties connectorProps = getField(service, "connectorProperties", Properties.class);
        Properties systemConsumerProps = getField(service, "systemConsumerProperties", Properties.class);

        assertEquals("kafka:9092", consumerProps.getProperty("bootstrap.servers"));
        assertEquals("kafka:9092", producerProps.getProperty("bootstrap.servers"));
        assertEquals("kafka:9092", adminProps.getProperty("bootstrap.servers"));
        assertEquals("SASL_SSL", consumerProps.getProperty("security.protocol"));
        assertEquals("secret", producerProps.getProperty("ssl.key.password"));
        assertEquals("3", connectorProps.getProperty(KafkaConnectorConstants.WATER_KAFKA_SYSTEM_CONSUMER_MAX_THREADS));
        assertEquals("2", connectorProps.getProperty(KafkaConnectorConstants.WATER_KAFKA_REACTOR_PROP_MAX_CONSUMER_THREADS));
        assertEquals("750", systemConsumerProps.getProperty(KafkaConnectorConstants.WATER_KAFKA_SYSTEM_CONSUMER_POLL_MS));
        assertEquals("http://connect-host:8083", getField(service, "kafkaConnectUrl", String.class));
    }

    @Test
    void activateAndDeactivateManageLifecycleAndTopics() throws Exception {
        KafkaConnectorSystemServiceImpl spyService = spy(new KafkaConnectorSystemServiceImpl());
        doNothing().when(spyService).startConsumingFromKafka(anyList());

        Map<String, String> values = new HashMap<>();
        values.put("it.water.layer", "prod");
        values.put("it.water.node.id", "node-7");
        values.put("it.water.connectors.kafka.bootstrap.servers", "kafka:9092");
        ApplicationProperties props = mockAppProperties(values);

        try (MockedStatic<AdminClient> mockedAdminClient = org.mockito.Mockito.mockStatic(AdminClient.class)) {
            AdminClient adminClient = mock(AdminClient.class);
            mockedAdminClient.when(() -> AdminClient.create(any(Properties.class))).thenReturn(adminClient);

            spyService.activate(props);

            verify(spyService).startConsumingFromKafka(anyList());
            assertEquals("prod", getField(spyService, "layerName", String.class));
            assertEquals("node-7", getField(spyService, "nodeId", String.class));
            assertNotNull(getField(spyService, "reactorScheduler", Object.class));

            spyService.deactivate();
            verify(adminClient).close(Duration.ofSeconds(10));
        }
    }

    @Test
    void getClusterSystemTopicUsesConfiguredLayer() throws Exception {
        setField(service, "layerName", "production");
        assertEquals("water_layer_production", service.getClusterSystemTopic());
    }

    private void invokeNotify(KafkaMessage message) throws Exception {
        Method method = KafkaConnectorSystemServiceImpl.class.getDeclaredMethod("notifyMessageReceivers", KafkaMessage.class);
        method.setAccessible(true);
        method.invoke(service, message);
    }

    private CloseableHttpResponse httpResponse(String body) throws IOException {
        return httpResponse(body, 200);
    }

    private CloseableHttpResponse httpResponse(String body, int statusCode) throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
        when(response.getEntity()).thenReturn(entity);
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        when(response.getStatusLine()).thenReturn(statusLine);
        return response;
    }

    private ApplicationProperties mockAppProperties(Map<String, String> values) {
        ApplicationProperties properties = mock(ApplicationProperties.class);
        when(properties.containsKey(anyString())).thenAnswer(invocation -> values.containsKey(invocation.getArgument(0, String.class)));
        when(properties.getProperty(anyString())).thenAnswer(invocation -> values.get(invocation.getArgument(0, String.class)));
        when(properties.getPropertyOrDefault(anyString(), anyString())).thenAnswer(invocation -> {
            String key = invocation.getArgument(0, String.class);
            String fallback = invocation.getArgument(1, String.class);
            return values.getOrDefault(key, fallback);
        });
        return properties;
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(Object target, String fieldName, Class<T> type) throws Exception {
        Field field = KafkaConnectorSystemServiceImpl.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private <T> T getFieldQuietly(Object target, String fieldName, Class<T> type) {
        try {
            return getField(target, fieldName, type);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = KafkaConnectorSystemServiceImpl.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        if (field.getType().isAssignableFrom(List.class) && value instanceof ArrayList) {
            field.set(target, new CopyOnWriteArrayList<>((List<?>) value));
            return;
        }
        field.set(target, value);
    }
}
