package com.evolutionnext.streaming;

import com.evolutionnext.consumers.ConsumerLoop;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Streaming {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "californiafilter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kaf0:9092,kaf1:9092,kaf2:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Thread mainThread = Thread.currentThread();

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> stream = builder.stream("scaled-cities");

        stream.filter((key, value) -> value.endsWith("CA"))
              .to("state-california-cities");

        KafkaStreams streams = new KafkaStreams(builder, props);

        streams.start();
        Thread.sleep(1000 * 60 * 20); //20 minutes
        streams.close();
    }
}
