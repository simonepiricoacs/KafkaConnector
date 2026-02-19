package it.water.connectors.kafka.repository;

import it.water.connectors.kafka.api.KafkaConnectorRepository;
import it.water.connectors.kafka.model.KafkaConnector;
import it.water.core.interceptors.annotations.FrameworkComponent;
import it.water.repository.jpa.WaterJpaRepositoryImpl;

/**
 * Repository implementation for KafkaConnector entity.
 */
@FrameworkComponent
public class KafkaConnectorRepositoryImpl extends WaterJpaRepositoryImpl<KafkaConnector> implements KafkaConnectorRepository {

    private static final String KAFKA_CONNECTOR_PERSISTENCE_UNIT = "water-default-persistence-unit";

    public KafkaConnectorRepositoryImpl() {
        super(KafkaConnector.class, KAFKA_CONNECTOR_PERSISTENCE_UNIT);
    }
}
