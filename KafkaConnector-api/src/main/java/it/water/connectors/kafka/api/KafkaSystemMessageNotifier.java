package it.water.connectors.kafka.api;
import it.water.connectors.kafka.model.KafkaMessage;
public interface KafkaSystemMessageNotifier {
    void notifyKafkaMessage(KafkaMessage message);
}
