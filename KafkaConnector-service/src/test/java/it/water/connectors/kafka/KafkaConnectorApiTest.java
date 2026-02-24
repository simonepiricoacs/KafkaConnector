package it.water.connectors.kafka;

import it.water.connectors.kafka.api.KafkaConnectorApi;
import it.water.connectors.kafka.api.KafkaConnectorSystemApi;
import it.water.core.api.bundle.Runtime;
import it.water.core.api.registry.ComponentRegistry;
import it.water.core.api.service.Service;
import it.water.core.interceptors.annotations.Inject;
import it.water.core.testing.utils.junit.WaterTestExtension;
import lombok.Setter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Generated with Water Generator.
 * Test class for KafkaConnector Services.
 * 
 * Please use KafkaConnectorRestTestApi for ensuring format of the json response
 

 */
@ExtendWith(WaterTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaConnectorApiTest implements Service {
    
    @Inject
    @Setter
    private ComponentRegistry componentRegistry;
    
    @Inject
    @Setter
    private KafkaConnectorApi kafkaconnectorApi;

    @Inject
    @Setter
    private Runtime runtime;

    /**
     * Testing basic injection of basic component for kafkaconnector entity.
     */
    @Test
    @Order(1)
    void componentsInsantiatedCorrectly() {
        this.kafkaconnectorApi = this.componentRegistry.findComponent(KafkaConnectorApi.class, null);
        Assertions.assertNotNull(this.kafkaconnectorApi);
        Assertions.assertNotNull(this.componentRegistry.findComponent(KafkaConnectorSystemApi.class, null));
    }

}
