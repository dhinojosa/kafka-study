package com.evolutionnext;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerTest {

    private KafkaConsumer<String, String> consumer;

    @Before
    public void setUp() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kaf0:9092, kaf1:9092");
        properties.put("group.id", "testGroup4");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        consumer = new KafkaConsumer<>(properties);
    }

    @Test
    public void testShowPartitionsForTopic() {
        System.out.println(consumer.partitionsFor("scaled-cities"));
        consumer.close();
    }

    @Test
    public void testConsumerCallWithPolling () {
        consumer.subscribe(Collections.singletonList("state-group"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(5);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.format("checksum: %d\n", record.checksum());
                    System.out.format("offset: %d\n", record.offset());
                    System.out.format("partition: %d\n", record.partition());
                    System.out.format("timestamp: %d\n", record.timestamp());
                    System.out.format("timeStampType: %s\n", record.timestampType());
                    System.out.format("topic: %s\n", record.topic());
                    System.out.format("key: %s\n", record.key());
                    System.out.format("value: %s\n", record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testConsumerCallWithStartFromTheBeginning () {
        consumer.subscribe(Collections.singletonList("scaled-cities"));
        consumer.seekToBeginning(consumer.assignment());
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(5);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.format("checksum: %d\n", record.checksum());
                    System.out.format("offset: %d\n", record.offset());
                    System.out.format("partition: %d\n", record.partition());
                    System.out.format("timestamp: %d\n", record.timestamp());
                    System.out.format("timeStampType: %s\n", record.timestampType());
                    System.out.format("topic: %s\n", record.topic());
                    System.out.format("key: %s\n", record.key());
                    System.out.format("value: %s\n", record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
