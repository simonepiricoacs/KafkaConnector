package it.water.connectors.kafka.util;
public class KafkaConnectorConstants {
    public static final String HYPERIOT_KAFKA_SYSTEM_CONSUMER_MAX_THREADS = "it.acsoftware.hyperiot.kafka.system.max.consumer.thread";
    public static final String HYPERIOT_KAFKA_REACTOR_PROP_MAX_CONSUMER_THREADS = "it.acsoftware.hyperiot.kafka.reactor.max.consumer.thread";
    public static final String HYPERIOT_KAFKA_PROPS_CONSUMER_PREFIX = "it.acsoftware.hyperiot.kafka.consumer";
    public static final String HYPERIOT_KAFKA_PROPS_SYSTEM_CONSUMER_PREFIX = "it.acsoftware.hyperiot.kafka.system.consumer";
    public static final String HYPERIOT_KAFKA_PROPS_PRODUCER_PREFIX = "it.acsoftware.hyperiot.kafka.producer";
    public static final String HYPERIOT_KAFKA_PROPS_ADMIN_PREFIX = "it.acsoftware.hyperiot.kafka.admin";
    public static final String HYPERIOT_KAFKA_PROPS_GLOBAL_PREFIX = "it.acsoftware.hyperiot.kafka.all";
    public static final String HYPERIOT_KAFKA_OSGI_TOPIC_FILTER = "it.acsoftware.hyperiot.kafka.topic";
    public static final String HYPERIOT_KAFKA_OSGI_KEY_FILTER = "it.acsoftware.hyperiot.kafka.key";
    public static final String HYPERIOT_KAFKA_SYSTEM_CONSUMER_POLL_MS = "it.acsoftware.hyperiot.kafka.system.consumer.poll.ms";
    public static final String HYPERIOT_KAFKA_OSGI_BASIC_TOPIC = "hyperiot_layer";
    public static final String HYPERIOT_KAFKA_OSGI_MICROSERVICES_TOPIC =
            HYPERIOT_KAFKA_OSGI_BASIC_TOPIC + "_microservices";
    public static final String HYPERIOT_KAFKA_OSGI_CONNECT_URL = "it.acsoftware.hyperiot.kafka.connect.url";
    public static final String HYPERIOT_KAFKA_SYSTEM_MESSAGE_TYPE_PROCESS_EVENT = "PROCESS_EVENT";
}
