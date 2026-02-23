package it.water.connectors.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.water.core.validation.annotations.NoMalitiusCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorConfig {
    @NoMalitiusCode
    private String name;
    @JsonProperty("max.poll.interval.ms")
    private int maxPollIntervalMs = 500;
    @JsonProperty("connector.class")
    @NoMalitiusCode
    private String connectorClass;
    @JsonProperty("tasks.max")
    private int taskMax = 1;

    public ConnectorConfig(String connectorName, String connectorClass) {
        this.name = connectorName;
        this.connectorClass = connectorClass;
    }
}
