package it.water.connectors.kafka.consumer;

import it.water.connectors.kafka.api.KafkaMessageReceiver;
import it.water.connectors.kafka.api.KafkaSystemMessageNotifier;
import it.water.connectors.kafka.model.KafkaMessage;
import it.water.connectors.kafka.util.KafkaConnectorConstants;
import it.water.core.api.interceptors.OnActivate;
import it.water.core.api.interceptors.OnDeactivate;
import it.water.core.api.registry.ComponentRegistry;
import it.water.core.api.registry.filter.ComponentFilter;
import it.water.core.api.registry.filter.ComponentFilterBuilder;
import it.water.core.interceptors.annotations.FrameworkComponent;
import it.water.core.interceptors.annotations.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

@FrameworkComponent
public class KafkaOSGISystemNotifier implements KafkaSystemMessageNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOSGISystemNotifier.class);

    @Inject
    private ComponentRegistry componentRegistry;

    @Inject
    private ComponentFilterBuilder componentFilterBuilder;

    @OnActivate
    public void activate() {
        KafkaGlobalNotifier.registerNotifier(this);
    }

    @OnDeactivate
    public void deactivate() {
        KafkaGlobalNotifier.unregisterNotifier(this);
    }

    @Override
    public void notifyKafkaMessage(KafkaMessage message) {
        if (message == null) {
            return;
        }
        if (componentRegistry == null || componentFilterBuilder == null) {
            LOG.debug("Component registry/filter builder not available, skipping component dispatch");
            return;
        }
        try {
            ComponentFilter topicFilter = componentFilterBuilder
                .createFilter(KafkaConnectorConstants.WATER_KAFKA_OSGI_TOPIC_FILTER, message.getTopic());
            ComponentFilter wildcardKeyFilter = componentFilterBuilder
                .createFilter(KafkaConnectorConstants.WATER_KAFKA_OSGI_KEY_FILTER, "*");
            ComponentFilter keyFilter = wildcardKeyFilter;
            if (message.getKey() != null) {
                String key = new String(message.getKey(), StandardCharsets.UTF_8);
                keyFilter = componentFilterBuilder
                    .createFilter(KafkaConnectorConstants.WATER_KAFKA_OSGI_KEY_FILTER, key)
                    .or(wildcardKeyFilter);
            }
            ComponentFilter receiverFilter = topicFilter.and(keyFilter);
            List<KafkaMessageReceiver> receivers = componentRegistry.findComponents(KafkaMessageReceiver.class, receiverFilter);
            for (KafkaMessageReceiver receiver : receivers) {
                notifyReceiver(receiver, message);
            }
        } catch (Throwable t) {
            LOG.error("Error while dispatching kafka system message", t);
        }
    }

    private void notifyReceiver(KafkaMessageReceiver receiver, KafkaMessage message) {
        try {
            receiver.receive(message);
        } catch (Throwable t) {
            LOG.error("Error while notifying KafkaMessageReceiver {}", receiver.getClass().getName(), t);
        }
    }
}
