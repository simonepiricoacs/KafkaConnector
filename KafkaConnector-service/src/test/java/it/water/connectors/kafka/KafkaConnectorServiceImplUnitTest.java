package it.water.connectors.kafka;

import it.water.connectors.kafka.api.KafkaConnectorSystemApi;
import it.water.connectors.kafka.model.ConnectorConfig;
import it.water.connectors.kafka.model.KafkaPermission;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.connectors.kafka.service.KafkaConnectorServiceImpl;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaConnectorServiceImplUnitTest {

    @Mock
    private KafkaConnectorSystemApi systemService;

    @InjectMocks
    private KafkaConnectorServiceImpl service;

    @Mock
    private Flux<ReceiverRecord<byte[], byte[]>> flux;

    @Mock
    private CreateTopicsResult createTopicsResult;

    @Mock
    private DeleteTopicsResult deleteTopicsResult;

    @Mock
    private CreateAclsResult createAclsResult;

    @Mock
    private DeleteAclsResult deleteAclsResult;

    @Test
    void consumeReactiveDelegatesToSystemService() throws ClassNotFoundException {
        when(systemService.consumeReactive(anyString(), any(List.class), anyLong(), any(), any())).thenReturn(flux);
        Flux<ReceiverRecord<byte[], byte[]>> result =
            service.consumeReactive("group", Collections.singletonList("topic"), 1000L, String.class, String.class);
        assertEquals(flux, result);
        verify(systemService, times(1)).consumeReactive("group", Collections.singletonList("topic"), 1000L, String.class, String.class);
    }

    @Test
    void consumeReactiveWithPartitionDelegatesToSystemService() throws ClassNotFoundException {
        when(systemService.consumeReactive(anyString(), anyString(), anyInt(), anyLong(), any(), any())).thenReturn(flux);
        Flux<ReceiverRecord<byte[], byte[]>> result =
            service.consumeReactive("group", "topic", 1000L, 1, String.class, String.class);
        assertEquals(flux, result);
        verify(systemService, times(1)).consumeReactive("group", "topic", 1, 1000L, String.class, String.class);
    }

    @Test
    void consumeReactiveAsSystemDelegatesToSystemService() throws ClassNotFoundException {
        Pattern pattern = Pattern.compile("topic.*");
        doNothing().when(systemService).consumeReactiveAsSystem(anyString(), any(Pattern.class), anyLong(), any(), any());
        service.consumeReactiveAsSystem("group", pattern, 1000L, String.class, String.class);
        verify(systemService, times(1)).consumeReactiveAsSystem("group", pattern, 1000L, String.class, String.class);
    }

    @Test
    void adminOperationsDelegateToSystemService() {
        when(systemService.adminCreateTopic(anyString(), anyInt(), anyShort())).thenReturn(createTopicsResult);
        when(systemService.adminCreateTopic(any(String[].class), any(int[].class), any(short[].class))).thenReturn(createTopicsResult);
        when(systemService.adminDropTopic(any(List.class))).thenReturn(deleteTopicsResult);
        when(systemService.adminAddACLs(anyString(), anyMap())).thenReturn(createAclsResult);
        when(systemService.adminDeleteACLs(anyString(), anyMap())).thenReturn(deleteAclsResult);

        assertEquals(createTopicsResult, service.adminCreateTopic("topic", 3, (short) 1));
        assertEquals(createTopicsResult, service.adminCreateTopic(new String[]{"topic1"}, new int[]{3}, new short[]{1}));
        assertEquals(deleteTopicsResult, service.adminDropTopic(Collections.singletonList("topic")));

        Map<String, KafkaPermission> permissions =
            Collections.singletonMap("topic", new KafkaPermission("topic", PatternType.LITERAL, AclOperation.READ, AclPermissionType.ALLOW));
        assertEquals(createAclsResult, service.adminAddACLs("user", permissions));
        assertEquals(deleteAclsResult, service.adminDeleteACLs("user", permissions));
    }

    @Test
    void connectorOperationsDelegateToSystemService() throws IOException {
        KafkaConnector connector = new KafkaConnector();
        connector.setName("connector");
        ConnectorConfig config = new ConnectorConfig();
        config.setName("connector");

        when(systemService.addNewConnector(anyString(), any(ConnectorConfig.class))).thenReturn(connector);
        doNothing().when(systemService).deleteConnector(anyString(), eq(true));
        when(systemService.getConnector(anyString())).thenReturn(connector);
        when(systemService.listConnectors()).thenReturn(Collections.singletonList("connector"));
        when(systemService.updateConnector(anyString(), any(ConnectorConfig.class))).thenReturn(connector);

        assertEquals(connector, service.addNewConnector("connector", config));
        service.deleteConnector("connector", true);
        assertEquals(connector, service.getConnector("connector"));
        assertEquals(Collections.singletonList("connector"), service.listConnectors());
        assertEquals(connector, service.updateConnector("connector", config));
    }

    @Test
    void settersAndGettersWork() {
        KafkaConnectorSystemApi otherSystem = systemService;
        service.setSystemService(otherSystem);
        assertNotNull(service.getSystemService());
    }
}
