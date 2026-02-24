package it.water.connectors.kafka;

import it.water.connectors.kafka.api.KafkaMessageReceiver;
import it.water.connectors.kafka.api.KafkaSystemMessageNotifier;
import it.water.connectors.kafka.consumer.KafkaGlobalNotifier;
import it.water.connectors.kafka.consumer.KafkaOSGISystemNotifier;
import it.water.connectors.kafka.model.KafkaMessage;
import it.water.connectors.kafka.util.KafkaConnectorConstants;
import it.water.core.api.registry.ComponentRegistry;
import it.water.core.api.registry.filter.ComponentFilter;
import it.water.core.api.registry.filter.ComponentFilterBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class KafkaNotifierUnitTest {

    @BeforeEach
    void clearBefore() {
        KafkaGlobalNotifier.getRegisteredNotifiers().clear();
    }

    @AfterEach
    void clearAfter() {
        KafkaGlobalNotifier.getRegisteredNotifiers().clear();
    }

    @Test
    void registerAndUnregisterNotifier() {
        KafkaSystemMessageNotifier notifier = mock(KafkaSystemMessageNotifier.class);

        KafkaGlobalNotifier.registerNotifier(notifier);
        KafkaGlobalNotifier.registerNotifier(null);

        assertEquals(1, KafkaGlobalNotifier.getRegisteredNotifiers().size());
        assertTrue(KafkaGlobalNotifier.getRegisteredNotifiers().contains(notifier));

        KafkaGlobalNotifier.unregisterNotifier(notifier);
        KafkaGlobalNotifier.unregisterNotifier(null);

        assertFalse(KafkaGlobalNotifier.getRegisteredNotifiers().contains(notifier));
    }

    @Test
    void notifyKafkaMessageDispatchesToAllNotifiers() {
        KafkaSystemMessageNotifier notifierOne = mock(KafkaSystemMessageNotifier.class);
        KafkaSystemMessageNotifier notifierTwo = mock(KafkaSystemMessageNotifier.class);
        KafkaGlobalNotifier.registerNotifier(notifierOne);
        KafkaGlobalNotifier.registerNotifier(notifierTwo);

        KafkaMessage message = KafkaMessage.from("topic-a", "k".getBytes(), "v".getBytes(), 1);
        KafkaGlobalNotifier.notifyKafkaMessage(message);

        verify(notifierOne, times(1)).notifyKafkaMessage(message);
        verify(notifierTwo, times(1)).notifyKafkaMessage(message);
    }

    @Test
    void notifyKafkaMessageContinuesWhenOneNotifierFails() {
        KafkaSystemMessageNotifier failingNotifier = mock(KafkaSystemMessageNotifier.class);
        KafkaSystemMessageNotifier notifierTwo = mock(KafkaSystemMessageNotifier.class);
        KafkaGlobalNotifier.registerNotifier(failingNotifier);
        KafkaGlobalNotifier.registerNotifier(notifierTwo);

        KafkaMessage message = KafkaMessage.from("topic-a", "k".getBytes(), "v".getBytes(), 1);
        doThrow(new RuntimeException("boom")).when(failingNotifier).notifyKafkaMessage(message);

        KafkaGlobalNotifier.notifyKafkaMessage(message);

        verify(failingNotifier, times(1)).notifyKafkaMessage(message);
        verify(notifierTwo, times(1)).notifyKafkaMessage(message);
    }

    @Test
    void osgiNotifierRegistersOnActivateAndUnregistersOnDeactivate() {
        KafkaOSGISystemNotifier notifier = new KafkaOSGISystemNotifier();

        notifier.activate();
        assertTrue(KafkaGlobalNotifier.getRegisteredNotifiers().contains(notifier));

        notifier.notifyKafkaMessage(KafkaMessage.from("topic-b", null, "payload".getBytes()));

        notifier.deactivate();
        assertFalse(KafkaGlobalNotifier.getRegisteredNotifiers().contains(notifier));
    }

    @Test
    void osgiNotifierDispatchesToKafkaMessageReceiversUsingFilter() throws Exception {
        KafkaOSGISystemNotifier notifier = new KafkaOSGISystemNotifier();
        ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
        ComponentFilterBuilder filterBuilder = mock(ComponentFilterBuilder.class);
        ComponentFilter topicFilter = mock(ComponentFilter.class);
        ComponentFilter wildcardKeyFilter = mock(ComponentFilter.class);
        ComponentFilter specificKeyFilter = mock(ComponentFilter.class);
        ComponentFilter mergedKeyFilter = mock(ComponentFilter.class);
        ComponentFilter finalFilter = mock(ComponentFilter.class);
        KafkaMessageReceiver receiver = mock(KafkaMessageReceiver.class);

        when(filterBuilder.createFilter(KafkaConnectorConstants.WATER_KAFKA_OSGI_TOPIC_FILTER, "topic-b")).thenReturn(topicFilter);
        when(filterBuilder.createFilter(KafkaConnectorConstants.WATER_KAFKA_OSGI_KEY_FILTER, "*")).thenReturn(wildcardKeyFilter);
        when(filterBuilder.createFilter(KafkaConnectorConstants.WATER_KAFKA_OSGI_KEY_FILTER, "k")).thenReturn(specificKeyFilter);
        when(specificKeyFilter.or(wildcardKeyFilter)).thenReturn(mergedKeyFilter);
        when(topicFilter.and(mergedKeyFilter)).thenReturn(finalFilter);
        when(componentRegistry.findComponents(KafkaMessageReceiver.class,finalFilter)).thenReturn(Collections.singletonList(receiver));

        setField(notifier, "componentRegistry", componentRegistry);
        setField(notifier, "componentFilterBuilder", filterBuilder);

        KafkaMessage message = KafkaMessage.from(
            "topic-b",
            "k".getBytes(StandardCharsets.UTF_8),
            "payload".getBytes(StandardCharsets.UTF_8)
        );
        notifier.notifyKafkaMessage(message);

        verify(receiver, times(1)).receive(message);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = KafkaOSGISystemNotifier.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
