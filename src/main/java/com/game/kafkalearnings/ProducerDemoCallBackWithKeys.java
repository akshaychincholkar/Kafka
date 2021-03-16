package com.game.kafkalearnings;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoCallBackWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger log = LoggerFactory.getLogger(ProducerDemoCallBackWithKeys.class);
//        Create producer properties
        String bootstrap_Servers = "ec2-3-15-17-134.us-east-2.compute.amazonaws.com:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_Servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++){
            String topic = "first_topic";
            String value = "value :"+(i+30);
            String key = "id_"+i;
//        Create record to send
            ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,key,value);

            log.info("key:"+key);
            //id0 - partition 1
            //id1 - partition 0
            //id2 - partition 2
            //id3 - partition 0
            //id4 - partition 2
            //id5 - partition 2
            //id6 - partition 0
            //id7 - partition 2
            //id8 - partition 1
            //id9 - partition 2
//        send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        log.info("Topic: "+recordMetadata.topic()+"\n"+
                                "Offset: "+recordMetadata.offset()+"\n"+
                                "Partition: "+recordMetadata.partition() +"\n"+
                                "Timestamp: "+recordMetadata.timestamp());
                    }else{
                        log.error(e.getMessage());
                    }
                }
            }).get();
        }
//        close connection
        producer.flush();
        producer.close();
    }
}
