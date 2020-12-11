package com.github.cassiolli.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        // creates a logger for my class
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092");
        // what type of value you're sending to kafka
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // create the producer
        // key and value are string in this examples
        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(properties);

        for (int i = 0; i <= 5; i++) {
            // create producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            "first_topic",
                            "hello word " + Integer.toString(i));

            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info(
                            "\n\nReceive new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() + "\n\n");
                    } else {
                        logger.error("Error while producing: ", e);
                    }
                }
            });
        }

        producer.close();
    }
}
