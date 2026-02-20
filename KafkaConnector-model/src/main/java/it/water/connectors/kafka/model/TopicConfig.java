package it.water.connectors.kafka.model;

import it.water.core.validation.annotations.NoMalitiusCode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class TopicConfig {
    @NoMalitiusCode
    private String topic;
    private int numPartition;
    private short replicationFactor;
}
