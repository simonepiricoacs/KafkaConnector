package it.water.connectors.kafka.consumer;

import it.water.connectors.kafka.model.KafkaMessage;
import it.water.connectors.kafka.util.KafkaConnectorConstants;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class KafkaConsumerThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerThread.class);

    private volatile boolean consume;
    private final Consumer<byte[], byte[]> consumer;
    private final List<String> topics;
    private final Properties systemConsumerProperties;
    private final ExecutorService notifierExecutor;
    private final java.util.function.Consumer<KafkaMessage> messageHandler;

    public KafkaConsumerThread(
        Properties props,
        Properties systemConsumerProperties,
        List<String> topics,
        ExecutorService notifierExecutor,
        java.util.function.Consumer<KafkaMessage> messageHandler
    ) {
        this.consume = true;
        this.systemConsumerProperties = systemConsumerProperties;
        this.notifierExecutor = notifierExecutor;
        this.consumer = new KafkaConsumer<>(props);
        this.topics = topics;
        this.messageHandler = messageHandler != null ? messageHandler : ignored -> { };
    }

    @Override
    public void run() {
        if (topics == null || topics.isEmpty()) {
            LOG.warn("No topics configured for Kafka consumer thread");
            consumer.close();
            return;
        }
        consumer.subscribe(topics);

        long pollDurationMillis = 500L;
        try {
            Object configured = systemConsumerProperties.get(KafkaConnectorConstants.WATER_KAFKA_SYSTEM_CONSUMER_POLL_MS);
            if (configured != null) {
                pollDurationMillis = Long.parseLong(String.valueOf(configured));
            }
        } catch (Exception e) {
            LOG.warn("Invalid system consumer poll configuration, using default 500ms");
        }

        while (consume) {
            try {
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(pollDurationMillis));
                for (TopicPartition partition : consumerRecords.partitions()) {
                    List<ConsumerRecord<byte[], byte[]>> partitionRecords = consumerRecords.records(partition);
                    for (ConsumerRecord<byte[], byte[]> recordP : partitionRecords) {
                        KafkaMessage message = KafkaMessage.from(recordP.topic(), recordP.key(), recordP.value());
                        notifyKafkaMessage(message);
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            } catch (WakeupException e) {
                LOG.debug("Kafka consumer thread wakeup requested");
            } catch (Throwable t) {
                LOG.error(t.getMessage(), t);
            }
        }

        consumer.close();
        LOG.info("Kafka Consumer stopped");
    }

    public void notifyKafkaMessage(KafkaMessage message) {
        if (message == null) {
            return;
        }
        Runnable notificationTask = () -> {
            try {
                messageHandler.accept(message);
            } catch (Throwable t) {
                LOG.error("Error while notifying kafka message", t);
            }
        };
        if (notifierExecutor == null) {
            notificationTask.run();
            return;
        }
        try {
            notifierExecutor.execute(notificationTask);
        } catch (Throwable t) {
            LOG.error("Error while submitting kafka message notification task", t);
            notificationTask.run();
        }
    }

    public void stop() {
        this.consumer.wakeup();
        this.consume = false;
    }
}
