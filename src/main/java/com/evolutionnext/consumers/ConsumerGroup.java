package com.evolutionnext.consumers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerGroup {


    public static void main(String[] args) {
        if (args.length != 3) System.err.println("Usage ConsumerGroup <groupId> <topic> <numberConsumers>");
        String groupId = args[0];
        String topic = args[1];
        int numberConsumers = Integer.parseInt(args[2]);
        System.out.format("Starting consumer group named %s on topic %s with %d number of threads/consumers",
                groupId, topic, numberConsumers);
        List<String> topics = Collections.singletonList(topic);
        ExecutorService executor = Executors.newFixedThreadPool(numberConsumers);

        final List<ConsumerLoop> consumers = new ArrayList<>();
        for (int i = 0; i < numberConsumers; i++) {
            ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ConsumerLoop consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                System.out.println("Shutting Down Safely");
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
