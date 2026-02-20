package it.water.connectors.kafka.model;

import it.water.core.validation.annotations.NoMalitiusCode;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class KafkaRestMessage {
    @NoMalitiusCode
    private String key;
    @NoMalitiusCode
    private String content;
}
