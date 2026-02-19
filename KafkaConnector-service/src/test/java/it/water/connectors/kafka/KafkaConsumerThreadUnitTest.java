package it.water.connectors.kafka;

import it.water.connectors.kafka.consumer.KafkaConsumerThread;
import it.water.connectors.kafka.model.KafkaMessage;
import it.water.connectors.kafka.util.KafkaConnectorConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaConsumerThreadUnitTest {

    private ExecutorService notifierExecutor;

    @AfterEach
    void tearDown() {
        if (notifierExecutor != null) {
            notifierExecutor.shutdownNow();
        }
    }

    @Test
    void runWithEmptyTopicsDoesNotSubscribe() throws Exception {
        notifierExecutor = Executors.newSingleThreadExecutor();
        try (MockedConstruction<KafkaConsumer> construction = org.mockito.Mockito.mockConstruction(KafkaConsumer.class)) {
            KafkaConsumerThread thread = new KafkaConsumerThread(
                baseConsumerProps(),
                new Properties(),
                Collections.emptyList(),
                notifierExecutor,
                message -> { }
            );

            thread.run();

            KafkaConsumer<byte[], byte[]> consumer = construction.constructed().get(0);
            verify(consumer, times(0)).subscribe(any(List.class));
            verify(consumer, times(0)).poll(any(Duration.class));
        }
    }

    @Test
    void runPollsCommitsAndStops() throws Exception {
        notifierExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch handlerLatch = new CountDownLatch(1);

        TopicPartition partition = new TopicPartition("topic-1", 0);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic-1", 0, 3L, "k".getBytes(), "v".getBytes());
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Map.of(partition, Collections.singletonList(record)));

        AtomicInteger pollCounter = new AtomicInteger();
        try (MockedConstruction<KafkaConsumer> construction = org.mockito.Mockito.mockConstruction(KafkaConsumer.class, (mock, context) -> {
            when(mock.poll(any(Duration.class))).thenAnswer(invocation -> {
                if (pollCounter.getAndIncrement() == 0) {
                    return records;
                }
                throw new WakeupException();
            });
            doNothing().when(mock).subscribe(any(List.class));
            doNothing().when(mock).commitSync(anyMap());
            doNothing().when(mock).wakeup();
            doNothing().when(mock).close();
        })) {
            KafkaConsumerThread thread = new KafkaConsumerThread(
                baseConsumerProps(),
                pollProps("150"),
                Collections.singletonList("topic-1"),
                notifierExecutor,
                message -> handlerLatch.countDown()
            );

            Thread runner = new Thread(thread);
            runner.start();
            assertTrue(handlerLatch.await(2, TimeUnit.SECONDS));
            thread.stop();
            runner.join(2000);

            KafkaConsumer<byte[], byte[]> consumer = construction.constructed().get(0);
            verify(consumer).subscribe(Collections.singletonList("topic-1"));
            verify(consumer, times(1)).commitSync(anyMap());
            verify(consumer).wakeup();
            verify(consumer).close();
        }
    }

    @Test
    void runFallsBackToDefaultPollDurationOnInvalidProperty() throws Exception {
        notifierExecutor = Executors.newSingleThreadExecutor();

        AtomicInteger pollCounter = new AtomicInteger();
        try (MockedConstruction<KafkaConsumer> construction = org.mockito.Mockito.mockConstruction(KafkaConsumer.class, (mock, context) -> {
            when(mock.poll(any(Duration.class))).thenAnswer(invocation -> {
                if (pollCounter.getAndIncrement() > 0) {
                    throw new WakeupException();
                }
                throw new WakeupException();
            });
            doNothing().when(mock).subscribe(any(List.class));
            doNothing().when(mock).wakeup();
            doNothing().when(mock).close();
        })) {
            KafkaConsumerThread thread = new KafkaConsumerThread(
                baseConsumerProps(),
                pollProps("not-a-number"),
                Collections.singletonList("topic-1"),
                notifierExecutor,
                message -> { }
            );

            Thread runner = new Thread(thread);
            runner.start();
            Thread.sleep(100);
            thread.stop();
            runner.join(2000);

            KafkaConsumer<byte[], byte[]> consumer = construction.constructed().get(0);
            verify(consumer, atLeastOnce()).poll(any(Duration.class));
            verify(consumer).close();
        }
    }

    @Test
    void notifyKafkaMessageSubmitsToExecutor() throws Exception {
        notifierExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch handlerLatch = new CountDownLatch(1);

        try (MockedConstruction<KafkaConsumer> ignored = org.mockito.Mockito.mockConstruction(KafkaConsumer.class)) {
            KafkaConsumerThread thread = new KafkaConsumerThread(
                baseConsumerProps(),
                new Properties(),
                Collections.singletonList("topic-2"),
                notifierExecutor,
                message -> handlerLatch.countDown()
            );

            thread.notifyKafkaMessage(KafkaMessage.from("topic-2", null, "v".getBytes()));
            assertTrue(handlerLatch.await(2, TimeUnit.SECONDS));
        }
    }

    private Properties baseConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        return props;
    }

    private Properties pollProps(String value) {
        Properties props = new Properties();
        props.put(KafkaConnectorConstants.HYPERIOT_KAFKA_SYSTEM_CONSUMER_POLL_MS, value);
        return props;
    }
}
