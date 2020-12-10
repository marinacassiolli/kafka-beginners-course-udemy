package com.github.cassiolli.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

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

        // create producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<>("first_topic", "hello word");

        // send data
        producer.send(record);
        producer.close();
    }
}
