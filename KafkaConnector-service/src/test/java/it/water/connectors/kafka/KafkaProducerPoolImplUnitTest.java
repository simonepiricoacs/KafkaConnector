package it.water.connectors.kafka;

import it.water.connectors.kafka.api.KafkaConnectorSystemApi;
import it.water.connectors.kafka.model.KafkaMessage;
import it.water.connectors.kafka.producer.KafkaProducerPoolImpl;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaProducerPoolImplUnitTest {

    @Mock
    private KafkaConnectorSystemApi systemApi;

    @Mock
    private KafkaProducer<byte[], byte[]> producerOne;

    @Mock
    private KafkaProducer<byte[], byte[]> producerTwo;

    private KafkaProducerPoolImpl pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.shutdown();
        }
    }

    @Test
    void constructorBuildsPoolAndSendsWithCallback() throws Exception {
        when(systemApi.getNewProducer(anyString())).thenReturn(producerOne, producerTwo);
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(systemApi).produceMessage(any(KafkaMessage.class), any(Producer.class), any(Callback.class));

        pool = new KafkaProducerPoolImpl(2, systemApi);
        KafkaMessage message = KafkaMessage.from("topic-a", "k".getBytes(), "v".getBytes());
        Callback callback = (metadata, exception) -> { };

        pool.send(message, callback);

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        verify(systemApi, times(2)).getNewProducer(anyString());
        verify(systemApi).produceMessage(eq(message), any(Producer.class), eq(callback));
    }

    @Test
    void sendWithoutCallbackDelegatesWithNullCallback() throws Exception {
        when(systemApi.getNewProducer(anyString())).thenReturn(producerOne);
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(systemApi).produceMessage(any(KafkaMessage.class), any(Producer.class), isNull());

        pool = new KafkaProducerPoolImpl(1, systemApi);
        KafkaMessage message = KafkaMessage.from("topic-b", null, "v".getBytes());

        pool.send(message);

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        verify(systemApi).produceMessage(eq(message), any(Producer.class), isNull());
    }

    @Test
    void sendWithNullProducerInPoolPassesNullProducer() throws Exception {
        when(systemApi.getNewProducer(anyString())).thenReturn(null);
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(systemApi).produceMessage(any(KafkaMessage.class), isNull(), isNull());

        pool = new KafkaProducerPoolImpl(1, systemApi);
        KafkaMessage message = KafkaMessage.from("topic-zero", null, "v".getBytes());

        pool.send(message);

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        verify(systemApi).produceMessage(eq(message), isNull(), isNull());
    }

    @Test
    void shutdownClosesAllProducersAndSwallowsExceptions() {
        when(systemApi.getNewProducer(anyString())).thenReturn(producerOne, producerTwo);
        doThrow(new RuntimeException("close-error")).when(producerOne).close();

        pool = new KafkaProducerPoolImpl(2, systemApi);
        pool.shutdown();

        verify(producerOne).close();
        verify(producerTwo).close();
    }

    @Test
    void sendCyclesProducersAcrossCalls() throws Exception {
        when(systemApi.getNewProducer(anyString())).thenReturn(producerOne, producerTwo);
        CountDownLatch latch = new CountDownLatch(3);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(systemApi).produceMessage(any(KafkaMessage.class), any(Producer.class), any());

        pool = new KafkaProducerPoolImpl(2, systemApi);
        Callback callback = mock(Callback.class);

        pool.send(KafkaMessage.from("topic-1", null, "v1".getBytes()), callback);
        pool.send(KafkaMessage.from("topic-2", null, "v2".getBytes()), callback);
        pool.send(KafkaMessage.from("topic-3", null, "v3".getBytes()), callback);

        assertTrue(latch.await(2, TimeUnit.SECONDS));

        ArgumentCaptor<Producer<byte[], byte[]>> producerCaptor = ArgumentCaptor.forClass(Producer.class);
        verify(systemApi, times(3)).produceMessage(any(KafkaMessage.class), producerCaptor.capture(), eq(callback));
        assertTrue(producerCaptor.getAllValues().contains(producerOne));
        assertTrue(producerCaptor.getAllValues().contains(producerTwo));
    }
}
