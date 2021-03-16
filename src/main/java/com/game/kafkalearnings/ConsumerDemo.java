package com.game.kafkalearnings;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);
//        Create producer properties
        String bootstrap_Servers = "ec2-3-15-17-134.us-east-2.compute.amazonaws.com:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_Servers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

//        subscribe consumer
       consumer.subscribe(Collections.singleton(topic)); //Arrays.asList() for multiple topics

//        poll for data
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record:records){
                log.info("key: "+record.key()+"\tvalue:"+record.value()+"\n"
                +"partition:"+record.partition()+"\toffset:"+record.offset());
            }
        }

//        close connection

    }
}
