package com.game.kafkalearnings;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
//        Create producer properties
        String bootstrap_Servers = "ec2-3-15-17-134.us-east-2.compute.amazonaws.com:9092";
        String topic = "first_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_Servers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

//      Assign and seek is used to read msg from particular offset from the topic
//        assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        long offsetToReadFrom = 5l;
        consumer.assign(Arrays.asList(partitionToReadFrom));

//        seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);
        int numberOfMsgToRead = 5;
        boolean keepOnReading = true;
        int numberOfMsgsReadSoFar = 0;

//        poll for data
        while(keepOnReading){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record:records){
                log.info("key: "+record.key()+"\tvalue:"+record.value()+"\n"
                +"partition:"+record.partition()+"\toffset:"+record.offset());
                if(numberOfMsgsReadSoFar>=numberOfMsgToRead){ keepOnReading = false; break;}
            }
        }
        log.info("Exiting the application");
//        close connection

    }
}
