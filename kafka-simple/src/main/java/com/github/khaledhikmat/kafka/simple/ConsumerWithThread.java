package com.github.khaledhikmat.kafka.simple;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class.getName());
        String topic = "first_topic";
        String groupId = "my-java-consumer-app";
        String bootstrapServers = "127.0.0.1:9092";
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating consumer runnable...");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        logger.info("Creating shutdown hook....");
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook....");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                logger.info("Application has exited!!");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application interrupted!!");
        } finally {
            logger.info("Application is closing!!");
        }
    }
}


class ConsumerRunnable implements Runnable {

    Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    String bootstrapServers;
    String groupId;
    String topic;
    CountDownLatch latch;
    Properties props = new Properties();

    KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String bootstrapServers,
                            String groupId,
                            String topic,
                            CountDownLatch latch) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;
        this.latch = latch;
        this.props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        this.props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<String, String>(this.props);
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + "\n" +
                                "Topic: " + record.topic() + "\n" +
                                "Value: " + record.value() + "\n" +
                                "Offset: " + record.offset() + "\n" +
                                "Partition: " + record.partition());
                }
            }
        } catch (WakeupException e) {
            logger.info("Received shutdown signal!!!");
        } finally {
            consumer.close();
            // Tell the main thread we are done with the consumer
            latch.countDown();
        }
    }

    public void shutdown() {
        // The wakeup method is a special method to interrupt the consumer poll
        // It will throw a wakeup exception
        consumer.wakeup();
    }
}
