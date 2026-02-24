package it.water.connectors.kafka.api;

import it.water.connectors.kafka.model.KafkaMessage;
import it.water.core.api.service.Service;

public interface KafkaMessageReceiver extends Service {

    /**
     *
     * @param message
     */
    void receive(KafkaMessage message);
}
