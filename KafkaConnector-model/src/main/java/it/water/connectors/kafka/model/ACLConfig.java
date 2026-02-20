package it.water.connectors.kafka.model;

import it.water.core.validation.annotations.NoMalitiusCode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class ACLConfig {
    @NoMalitiusCode
    private String username;
    private HashMap<String, KafkaPermission> permissions;
}
