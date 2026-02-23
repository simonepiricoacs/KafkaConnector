package it.water.connectors.kafka.api;
import it.water.connectors.kafka.model.KafkaMessage;
import org.apache.kafka.clients.producer.Callback;
public interface KafkaProducerPool {
    void send(KafkaMessage message);
    void send(KafkaMessage message, Callback callback);
    void shutdown();
}
