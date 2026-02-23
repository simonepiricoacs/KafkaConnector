package it.water.connectors.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import it.water.core.validation.annotations.NoMalitiusCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaConnector {
    @NoMalitiusCode
    private String name;
    @NoMalitiusCode
    private String type = "source";
    private ConnectorConfig config;
    private ConnectorTask[] tasks;
}
