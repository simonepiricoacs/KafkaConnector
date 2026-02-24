package it.water.connectors.kafka.api;
import it.water.connectors.kafka.model.KafkaMessage;
public interface KafkaSystemMessageNotifier {
    /**
     *
     * @param message
     */
    void notifyKafkaMessage(KafkaMessage message);
}
