package com.evolutionnext.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class StandardProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "kaf0:9092,kaf1:9092,kaf2:9092"); //At least 2
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("scaled-cities",
                        "Marfa, TX");
        Future<RecordMetadata> future = producer.send(producerRecord);
        producer.flush();
        RecordMetadata recordMetadata = future.get();  //blocks
        System.out.format("checksum: %d\n", recordMetadata.checksum());
        System.out.format("offset: %d\n", recordMetadata.offset());
        System.out.format("partition: %d\n", recordMetadata.partition());
        System.out.format("timestamp: %d\n", recordMetadata.timestamp());
        System.out.format("topic: %s\n", recordMetadata.topic());
        System.out.format("toString: %s\n", recordMetadata.toString());
    }
}
