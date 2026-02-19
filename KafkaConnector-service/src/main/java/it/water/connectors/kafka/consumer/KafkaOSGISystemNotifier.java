package it.water.connectors.kafka.consumer;

import it.water.connectors.kafka.api.KafkaSystemMessageNotifier;
import it.water.connectors.kafka.model.KafkaMessage;
import it.water.core.api.interceptors.OnActivate;
import it.water.core.api.interceptors.OnDeactivate;
import it.water.core.interceptors.annotations.FrameworkComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FrameworkComponent
public class KafkaOSGISystemNotifier implements KafkaSystemMessageNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOSGISystemNotifier.class);

    @OnActivate
    public void activate() {
        KafkaGloabalNotifier.registerNotifier(this);
    }

    @OnDeactivate
    public void deactivate() {
        KafkaGloabalNotifier.unregisterNotifier(this);
    }

    @Override
    public void notifyKafkaMessage(KafkaMessage message) {
        LOG.debug("Received kafka system message on topic {}", message.getTopic());
    }
}
