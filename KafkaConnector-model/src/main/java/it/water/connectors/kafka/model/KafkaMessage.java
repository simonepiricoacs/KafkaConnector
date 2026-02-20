package it.water.connectors.kafka.model;

import it.water.core.validation.annotations.NoMalitiusCode;
import lombok.Getter;

@Getter
public class KafkaMessage {
    private byte[] key;
    private byte[] payload;
    @NoMalitiusCode
    private String topic;
    private int partition = -1;

    public KafkaMessage(byte[] key, String topic, byte[] payload) {
        this.key = key;
        this.payload = payload;
        this.topic = topic;
    }

    public KafkaMessage(byte[] key, String topic, int partition, byte[] payload) {
        this.key = key;
        this.payload = payload;
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("KAFKA MESSAGE ON TOPIC:").append(topic);
        if (this.getKey() != null)
            sb.append(" KEY IS:").append(new String(key));
        if (this.getPayload() != null)
            sb.append(" MESSAGE IS:").append(new String(payload));
        return sb.toString();
    }

    public static KafkaMessage from(String topic, byte[] key, byte[] payload) {
        return new KafkaMessage(key, topic, payload);
    }

    public static KafkaMessage from(String topic, byte[] key, byte[] payload, int partition) {
        return new KafkaMessage(key, topic, partition, payload);
    }
}
