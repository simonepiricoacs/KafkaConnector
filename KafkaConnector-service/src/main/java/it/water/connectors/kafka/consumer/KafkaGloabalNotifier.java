package it.water.connectors.kafka.consumer;

import it.water.connectors.kafka.api.KafkaSystemMessageNotifier;
import it.water.connectors.kafka.model.KafkaMessage;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class KafkaGloabalNotifier {

    private static final List<KafkaSystemMessageNotifier> NOTIFIERS = new CopyOnWriteArrayList<>();

    private KafkaGloabalNotifier() {
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
        for (KafkaSystemMessageNotifier notifier : NOTIFIERS) {
            notifier.notifyKafkaMessage(message);
        }
    }
}
