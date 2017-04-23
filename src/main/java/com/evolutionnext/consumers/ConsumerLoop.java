package com.evolutionnext.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;

public class ConsumerLoop implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    public ConsumerLoop(int id,
                        String groupId,
                        List<String> topics) {
        this.id = id;
        this.topics = topics;
        Properties props = new Properties();
        props.put("bootstrap.servers", "kaf0:9092,kaf1:9092,kaf2:9092");
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(5);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.format("consumer id: %d\n", id);
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
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}