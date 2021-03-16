package com.game.kafkalearnings;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class Revision {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "ec2-3-21-21-38.us-east-2.compute.amazonaws.com:9092";
        String topic = "first_topic";
        String message = "value #";
        Logger logger = LoggerFactory.getLogger(Revision.class);
        //create properties for producer
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer with properties
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //iterate for producer
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,message+i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Producer info: topic"+recordMetadata.topic()
                        +"\t offset:"+recordMetadata.offset()+"" +
                                "\nPartition:"+recordMetadata.partition()+" timestamp:"+recordMetadata.timestamp());
                    }
                }
            }).get();
        }
    }

}
