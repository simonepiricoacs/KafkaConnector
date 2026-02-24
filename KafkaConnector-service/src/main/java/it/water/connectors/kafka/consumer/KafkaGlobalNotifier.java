package it.water.connectors.kafka.consumer;

import it.water.connectors.kafka.api.KafkaSystemMessageNotifier;
import it.water.connectors.kafka.model.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class KafkaGlobalNotifier {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaGlobalNotifier.class);

    private static final List<KafkaSystemMessageNotifier> NOTIFIERS = new CopyOnWriteArrayList<>();

    private KafkaGlobalNotifier() {
    }

    public static void registerNotifier(KafkaSystemMessageNotifier notifier) {
        if (notifier != null) {
            NOTIFIERS.add(notifier);
        }
    }

    public static void unregisterNotifier(KafkaSystemMessageNotifier notifier) {
        if (notifier != null) {
            NOTIFIERS.remove(notifier);
        }
    }

    public static List<KafkaSystemMessageNotifier> getRegisteredNotifiers() {
        return NOTIFIERS;
    }

    public static void notifyKafkaMessage(KafkaMessage message) {
        if (message == null) {
            return;
        }
        for (KafkaSystemMessageNotifier notifier : NOTIFIERS) {
            if (notifier == null) {
                continue;
            }
            try {
                notifier.notifyKafkaMessage(message);
            } catch (Throwable t) {
                LOG.error("Error while notifying component {}", notifier.getClass().getName(), t);
            }
        }
    }
}
