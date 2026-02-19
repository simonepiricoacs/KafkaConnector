package it.water.connectors.kafka;

import it.water.connectors.kafka.api.KafkaSystemMessageNotifier;
import it.water.connectors.kafka.consumer.KafkaGloabalNotifier;
import it.water.connectors.kafka.consumer.KafkaOSGISystemNotifier;
import it.water.connectors.kafka.model.KafkaMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class KafkaNotifierUnitTest {

    @BeforeEach
    void clearBefore() {
        KafkaGloabalNotifier.getRegisteredNotifiers().clear();
    }

    @AfterEach
    void clearAfter() {
        KafkaGloabalNotifier.getRegisteredNotifiers().clear();
    }

    @Test
    void registerAndUnregisterNotifier() {
        KafkaSystemMessageNotifier notifier = mock(KafkaSystemMessageNotifier.class);

        KafkaGloabalNotifier.registerNotifier(notifier);
        KafkaGloabalNotifier.registerNotifier(null);

        assertEquals(1, KafkaGloabalNotifier.getRegisteredNotifiers().size());
        assertTrue(KafkaGloabalNotifier.getRegisteredNotifiers().contains(notifier));

        KafkaGloabalNotifier.unregisterNotifier(notifier);
        KafkaGloabalNotifier.unregisterNotifier(null);

        assertFalse(KafkaGloabalNotifier.getRegisteredNotifiers().contains(notifier));
    }

    @Test
    void notifyKafkaMessageDispatchesToAllNotifiers() {
        KafkaSystemMessageNotifier notifierOne = mock(KafkaSystemMessageNotifier.class);
        KafkaSystemMessageNotifier notifierTwo = mock(KafkaSystemMessageNotifier.class);
        KafkaGloabalNotifier.registerNotifier(notifierOne);
        KafkaGloabalNotifier.registerNotifier(notifierTwo);

        KafkaMessage message = KafkaMessage.from("topic-a", "k".getBytes(), "v".getBytes(), 1);
        KafkaGloabalNotifier.notifyKafkaMessage(message);

        verify(notifierOne, times(1)).notifyKafkaMessage(message);
        verify(notifierTwo, times(1)).notifyKafkaMessage(message);
    }

    @Test
    void osgiNotifierRegistersOnActivateAndUnregistersOnDeactivate() {
        KafkaOSGISystemNotifier notifier = new KafkaOSGISystemNotifier();

        notifier.activate();
        assertTrue(KafkaGloabalNotifier.getRegisteredNotifiers().contains(notifier));

        notifier.notifyKafkaMessage(KafkaMessage.from("topic-b", null, "payload".getBytes()));

        notifier.deactivate();
        assertFalse(KafkaGloabalNotifier.getRegisteredNotifiers().contains(notifier));
    }
}
