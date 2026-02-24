package it.water.connectors.kafka.util;
public class KafkaConnectorConstants {
    private KafkaConnectorConstants(){}

    public static final String WATER_KAFKA_SYSTEM_CONSUMER_MAX_THREADS = "it.water.connectors.kafka.system.max.consumer.threads";
    public static final String WATER_KAFKA_REACTOR_PROP_MAX_CONSUMER_THREADS = "it.water.connectors.kafka.reactor.max.consumer.threads";
    public static final String WATER_KAFKA_PROPS_CONSUMER_PREFIX = "it.water.connectors.kafka.consumer";
    public static final String WATER_KAFKA_PROPS_SYSTEM_CONSUMER_PREFIX = "it.water.connectors.kafka.system.consumer";
    public static final String WATER_KAFKA_PROPS_PRODUCER_PREFIX = "it.water.connectors.kafka.producer";
    public static final String WATER_KAFKA_PROPS_ADMIN_PREFIX = "it.water.connectors.kafka.admin";
    public static final String WATER_KAFKA_PROPS_GLOBAL_PREFIX = "it.water.connectors.kafka";
    public static final String WATER_KAFKA_OSGI_TOPIC_FILTER = "it.water.connectors.kafka.topic";
    public static final String WATER_KAFKA_OSGI_KEY_FILTER = "it.water.connectors.kafka.key";
    public static final String WATER_KAFKA_SYSTEM_CONSUMER_POLL_MS = "it.water.connectors.kafka.system.consumer.poll.ms";
    public static final String WATER_KAFKA_OSGI_BASIC_TOPIC = "water_layer";
    public static final String WATER_KAFKA_OSGI_MICROSERVICES_TOPIC =
            WATER_KAFKA_OSGI_BASIC_TOPIC + "_microservices";
    public static final String WATER_KAFKA_OSGI_CONNECT_URL = "it.water.connectors.kafka.connect.url";
    public static final String WATER_KAFKA_SYSTEM_MESSAGE_TYPE_PROCESS_EVENT = "PROCESS_EVENT";
    public static final String WATER_KAFKA_BOOTSTRAP_SERVERS = "it.water.connectors.kafka.bootstrap.servers";
}
