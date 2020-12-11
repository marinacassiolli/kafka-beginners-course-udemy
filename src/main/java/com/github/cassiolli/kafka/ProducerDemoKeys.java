package com.github.cassiolli.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // creates a logger for my class
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        final String  kafkaServer = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaServer);
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
            final String topic = "first_topic";
            String value = "hello word " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // create producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);

            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info(
                            "\nReceive new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing: ", e);
                    }
                }
            }).get(); // makes it synchronous, but its just an example, don't do it in production.
        }

        producer.close();
    }
}
