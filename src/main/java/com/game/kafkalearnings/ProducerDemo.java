package com.game.kafkalearnings;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

//        Create producer properties
        String bootstrap_Servers = "ec2-3-15-17-134.us-east-2.compute.amazonaws.com:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_Servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

//        Create record to send
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","hello java world");

//        send data
        producer.send(record);

//        close connection
        producer.flush();
        producer.close();
    }
}
