package com.evolutionnext;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

public class ConsumerGroupsTest {

    private KafkaConsumer<String, String> consumer;

    @Before
    public void setUp() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kaf0:9092, kaf1:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup4");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
    }

    @Test
    public void testConsumerCallWithPollingDifferentGroup () {
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
