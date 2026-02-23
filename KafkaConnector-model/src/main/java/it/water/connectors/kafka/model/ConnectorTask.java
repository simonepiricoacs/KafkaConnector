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
public class ConnectorTask {
    @NoMalitiusCode
    private String connector;
    private int task;
}
