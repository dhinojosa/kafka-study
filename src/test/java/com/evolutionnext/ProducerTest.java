package com.evolutionnext;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerTest {


    private KafkaProducer<String, String> producer;

    @Before
    public void setUp() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kaf0:9092,kaf1:9092,kaf2:9092"); //At least 2
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
    }

    @Test
    public void testSendASimpleMessage() throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("test", "Hello! St. Louis! Go Blues");
        Future<RecordMetadata> future = producer.send(producerRecord);
        producer.flush();
        RecordMetadata recordMetadata = future.get();
        System.out.format("checksum: %d\n", recordMetadata.checksum());
        System.out.format("offset: %d\n", recordMetadata.offset());
        System.out.format("partition: %d\n", recordMetadata.partition());
        System.out.format("timestamp: %d\n", recordMetadata.timestamp());
        System.out.format("topic: %s\n", recordMetadata.topic());
        System.out.format("toString: %s\n", recordMetadata.toString());
    }
}
