package com.evolutionnext.streaming;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class StreamingWithStore {
    //github.com/dhinojosa/kafka-study
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Another_group");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kaf0:9092,kaf1:9092,kaf2:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> stream = builder.stream("scaled-cities");

        stream.groupBy((key, value) ->
                value.split(",")[1],Serdes.String(), Serdes.String())
              .count("StateCount")
              .to(Serdes.String(), Serdes.Long(), "state-group");

        KafkaStreams streams = new KafkaStreams(builder, props);

        streams.start();
        Thread.sleep(1000 * 60 * 20); //20 minutes
        streams.close();
    }
}
