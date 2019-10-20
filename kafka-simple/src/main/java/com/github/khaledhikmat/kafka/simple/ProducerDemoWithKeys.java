package com.github.khaledhikmat.kafka.simple;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Hello world!");

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        // Producer props
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // Messages from the same key go to the same partition
        for (int i=0; i < 10; i++) {
            String topic = "first_topic";
            String key = "id_" + Integer.toString(i);
            String value = "Hello World: " + Integer.toString(i);

            logger.info("Key: " + key);
            // Send data asynch and flush
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        logger.error("Error occurred: " + e.getMessage());
                    } else {
                        logger.info("Received metadata:\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    }
                }
            }).get();
        }

        producer.flush();
        producer.close();
    }
}
