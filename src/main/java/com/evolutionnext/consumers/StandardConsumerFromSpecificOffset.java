package com.evolutionnext.consumers;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class StandardConsumerFromSpecificOffset {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kaf0:9092, kaf1:9092");
        properties.put("group.id", "testGroup4");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("scaled-cities"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("Partitions " + partitions + " revoked");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("Partitions " + partitions + " assigned");
                //consumer.seek(partitions, getFromDB());
            }
        });
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
