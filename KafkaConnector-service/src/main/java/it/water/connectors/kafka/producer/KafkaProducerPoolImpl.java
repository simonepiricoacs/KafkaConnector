package it.water.connectors.kafka.producer;

import it.water.connectors.kafka.api.KafkaConnectorSystemApi;
import it.water.connectors.kafka.api.KafkaProducerPool;
import it.water.connectors.kafka.model.KafkaMessage;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaProducerPoolImpl implements KafkaProducerPool {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerPoolImpl.class);

    private final Map<String, KafkaProducer<byte[], byte[]>> producerPool;
    private final ExecutorService producerPoolExecutor;
    private final KafkaConnectorSystemApi kafkaConnectorSystemApi;
    private Iterator<KafkaProducer<byte[], byte[]>> producerIterator;
    private KafkaProducer<byte[], byte[]> currentProducer;

    public KafkaProducerPoolImpl(int poolSize, KafkaConnectorSystemApi kafkaConnectorSystemApi) {
        this.kafkaConnectorSystemApi = kafkaConnectorSystemApi;
        this.producerPool = Collections.synchronizedMap(new HashMap<>());
        for (int i = 0; i < poolSize; i++) {
            String uuid = UUID.randomUUID().toString();
            this.producerPool.put(uuid, this.kafkaConnectorSystemApi.getNewProducer(uuid));
        }
        this.producerPoolExecutor = Executors.newFixedThreadPool(poolSize);
        this.resetIterator();
        this.nextProducer();
    }

    private synchronized void resetIterator() {
        this.producerIterator = this.producerPool.values().iterator();
    }

    private synchronized void nextProducer() {
        if (this.producerIterator == null || !this.producerIterator.hasNext()) {
            resetIterator();
        }
        if (this.producerIterator.hasNext()) {
            this.currentProducer = this.producerIterator.next();
        }
    }

    @Override
    public synchronized void send(KafkaMessage message) {
        this.send(message, null);
    }

    @Override
    public synchronized void send(KafkaMessage message, Callback callback) {
        KafkaProducer<byte[], byte[]> selectedProducer = this.currentProducer;
        this.nextProducer();
        this.producerPoolExecutor.execute(
            () -> this.kafkaConnectorSystemApi.produceMessage(message, selectedProducer, callback)
        );
    }

    @Override
    public void shutdown() {
        this.producerPool.values().forEach(producer -> {
            try {
                producer.close();
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            }
        });
        this.producerPoolExecutor.shutdown();
    }
}
