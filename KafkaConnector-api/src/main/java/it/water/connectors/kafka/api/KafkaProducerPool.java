package it.water.connectors.kafka.api;
import it.water.connectors.kafka.model.KafkaMessage;
import org.apache.kafka.clients.producer.Callback;
public interface KafkaProducerPool {

    /**
     *
     * @param message
     */
    void send(KafkaMessage message);

    /**
     *
     * @param message
     * @param callback
     */
    void send(KafkaMessage message, Callback callback);

    /**
     *
     */
    void shutdown();
}
