package it.water.connectors.kafka;

import it.water.connectors.kafka.api.KafkaConnectorApi;
import it.water.connectors.kafka.model.ACLConfig;
import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.KafkaPermission;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.connectors.kafka.model.TopicConfig;
import it.water.connectors.kafka.service.rest.KafkaConnectorRestControllerImpl;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaConnectorRestControllerImplUnitTest {

    @Mock
    private KafkaConnectorApi kafkaConnectorApi;

    @InjectMocks
    private KafkaConnectorRestControllerImpl restController;

    @Test
    void checkModuleWorkingReturnsOk() {
        Response response = restController.checkModuleWorking();
        assertEquals(200, response.getStatus());
        assertEquals("Water Kafka Connector Module works!", response.getEntity());
    }

    @Test
    void addConnectorSuccess() throws IOException {
        ConnectorConfig config = new ConnectorConfig();
        config.setName("my-connector");
        KafkaConnector connector = new KafkaConnector("my-connector");

        when(kafkaConnectorApi.addNewConnector("my-connector", config)).thenReturn(connector);

        Response response = restController.addConnector(config);

        assertEquals(200, response.getStatus());
        assertSame(connector, response.getEntity());
    }

    @Test
    void addConnectorErrorReturnsServerError() throws IOException {
        ConnectorConfig config = new ConnectorConfig();
        config.setName("my-connector");
        when(kafkaConnectorApi.addNewConnector(anyString(), any(ConnectorConfig.class))).thenThrow(new IOException("boom"));

        Response response = restController.addConnector(config);

        assertEquals(500, response.getStatus());
        assertEquals("boom", response.getEntity());
    }

    @Test
    void updateConnectorSuccess() throws IOException {
        ConnectorConfig config = new ConnectorConfig();
        KafkaConnector connector = new KafkaConnector("my-connector");
        when(kafkaConnectorApi.updateConnector("my-connector", config)).thenReturn(connector);

        Response response = restController.updateConnector("my-connector", config);

        assertEquals(200, response.getStatus());
        assertSame(connector, response.getEntity());
    }

    @Test
    void updateConnectorErrorReturnsServerError() throws IOException {
        ConnectorConfig config = new ConnectorConfig();
        when(kafkaConnectorApi.updateConnector(anyString(), any(ConnectorConfig.class))).thenThrow(new IOException("update-error"));

        Response response = restController.updateConnector("my-connector", config);

        assertEquals(500, response.getStatus());
        assertEquals("update-error", response.getEntity());
    }

    @Test
    void deleteConnectorSuccess() throws IOException {
        Response response = restController.deleteConnector("my-connector");

        verify(kafkaConnectorApi).deleteConnector("my-connector", true);
        assertEquals(200, response.getStatus());
        assertEquals("Connector deleted.", response.getEntity());
    }

    @Test
    void deleteConnectorErrorReturnsServerError() throws IOException {
        doThrow(new IOException("delete-error")).when(kafkaConnectorApi).deleteConnector("my-connector", true);

        Response response = restController.deleteConnector("my-connector");

        assertEquals(500, response.getStatus());
        assertEquals("delete-error", response.getEntity());
    }

    @Test
    void createTopicSuccess() {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopic("topic-1");
        topicConfig.setNumPartition(3);
        topicConfig.setReplicationFactor((short) 2);
        CreateTopicsResult result = org.mockito.Mockito.mock(CreateTopicsResult.class);

        when(kafkaConnectorApi.adminCreateTopic("topic-1", 3, (short) 2)).thenReturn(result);

        Response response = restController.createTopic(topicConfig);

        assertEquals(200, response.getStatus());
        assertSame(result, response.getEntity());
    }

    @Test
    void createTopicErrorReturnsServerError() {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopic("topic-1");
        when(kafkaConnectorApi.adminCreateTopic(anyString(), anyInt(), anyShort())).thenThrow(new RuntimeException("topic-error"));

        Response response = restController.createTopic(topicConfig);

        assertEquals(500, response.getStatus());
        assertEquals("topic-error", response.getEntity());
    }

    @Test
    void createMultipleTopicsBuildsArraysAndDelegates() {
        TopicConfig topicOne = new TopicConfig();
        topicOne.setTopic("t1");
        topicOne.setNumPartition(2);
        topicOne.setReplicationFactor((short) 1);

        TopicConfig topicTwo = new TopicConfig();
        topicTwo.setTopic("t2");
        topicTwo.setNumPartition(4);
        topicTwo.setReplicationFactor((short) 2);

        CreateTopicsResult result = org.mockito.Mockito.mock(CreateTopicsResult.class);
        when(kafkaConnectorApi.adminCreateTopic(any(String[].class), any(int[].class), any(short[].class))).thenReturn(result);

        Response response = restController.createMultipleTopics(new TopicConfig[]{topicOne, topicTwo});

        ArgumentCaptor<String[]> topicsCaptor = ArgumentCaptor.forClass(String[].class);
        ArgumentCaptor<int[]> partitionsCaptor = ArgumentCaptor.forClass(int[].class);
        ArgumentCaptor<short[]> replicasCaptor = ArgumentCaptor.forClass(short[].class);
        verify(kafkaConnectorApi).adminCreateTopic(topicsCaptor.capture(), partitionsCaptor.capture(), replicasCaptor.capture());

        assertEquals(Arrays.asList("t1", "t2"), Arrays.asList(topicsCaptor.getValue()));
        assertEquals(2, partitionsCaptor.getValue()[0]);
        assertEquals(4, partitionsCaptor.getValue()[1]);
        assertEquals(1, replicasCaptor.getValue()[0]);
        assertEquals(2, replicasCaptor.getValue()[1]);
        assertEquals(200, response.getStatus());
        assertSame(result, response.getEntity());
    }

    @Test
    void dropTopicsSuccess() {
        DeleteTopicsResult result = org.mockito.Mockito.mock(DeleteTopicsResult.class);
        when(kafkaConnectorApi.adminDropTopic(Collections.singletonList("t1"))).thenReturn(result);

        Response response = restController.dropTopics(new String[]{"t1"});

        assertEquals(200, response.getStatus());
        assertSame(result, response.getEntity());
    }

    @Test
    void addAndDeleteAclsDelegateAndHandleErrors() {
        HashMap<String, KafkaPermission> permissions = new HashMap<>();
        permissions.put("t1", new KafkaPermission("t1", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW));
        ACLConfig config = new ACLConfig();
        config.setUsername("user");
        config.setPermissions(permissions);

        CreateAclsResult createResult = org.mockito.Mockito.mock(CreateAclsResult.class);
        DeleteAclsResult deleteResult = org.mockito.Mockito.mock(DeleteAclsResult.class);
        when(kafkaConnectorApi.adminAddACLs("user", permissions)).thenReturn(createResult);
        when(kafkaConnectorApi.adminDeleteACLs("user", permissions)).thenReturn(deleteResult);

        Response createResponse = restController.addACLs(config);
        Response deleteResponse = restController.deleteACLs(config);

        assertEquals(200, createResponse.getStatus());
        assertEquals(200, deleteResponse.getStatus());
        assertSame(createResult, createResponse.getEntity());
        assertSame(deleteResult, deleteResponse.getEntity());

        when(kafkaConnectorApi.adminAddACLs(anyString(), anyMap())).thenThrow(new RuntimeException("acl-add-error"));
        when(kafkaConnectorApi.adminDeleteACLs(anyString(), anyMap())).thenThrow(new RuntimeException("acl-del-error"));

        Response createError = restController.addACLs(config);
        Response deleteError = restController.deleteACLs(config);

        assertEquals(500, createError.getStatus());
        assertEquals("acl-add-error", createError.getEntity());
        assertEquals(500, deleteError.getStatus());
        assertEquals("acl-del-error", deleteError.getEntity());
    }

    @Test
    void getEntityServiceReturnsKafkaApi() throws Exception {
        Method method = KafkaConnectorRestControllerImpl.class.getDeclaredMethod("getEntityService");
        method.setAccessible(true);
        Object service = method.invoke(restController);
        assertNotNull(service);
        assertSame(kafkaConnectorApi, service);
    }
}
