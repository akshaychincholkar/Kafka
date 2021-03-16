package com.game.kafkalearnings;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }
    private ConsumerDemoWithThread(){

    }
    private void run(){
        final Logger log = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
//        Create producer properties
        String bootstrap_Servers = "ec2-3-15-17-134.us-east-2.compute.amazonaws.com:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        //latch dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        log.info("Creating the consumer thread");

        //creating runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrap_Servers,
                groupId,
                topic,
                latch);

        //Create the thread to pass runnable
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                ()->{
                    log.info("Caught shutdown hook");
                    ((ConsumerRunnable)  myConsumerRunnable).shutDown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    log.info("Application has exited");
                }
        ));
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupted: "+e);
        }finally {
            log.info("Application is closing");

        }
    }
    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        private Logger log = LoggerFactory.getLogger(ConsumerRunnable.class);
        //        create consumer
        private KafkaConsumer<String, String> consumer;
        public ConsumerRunnable(String bootstrap_Servers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_Servers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<String, String>(properties);

            //        subscribe consumer
            consumer.subscribe(Collections.singleton(topic)); //Arrays.asList() for multiple topics
        }
        @Override
        public void run() {
//        poll for data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("key: " + record.key() + "\tvalue:" + record.value() + "\n"
                                + "partition:" + record.partition() + "\toffset:" + record.offset());
                    }
                }
            }catch(WakeupException e){
                log.info("Received shutdown signal!");
            }finally {
                //        close connection
                consumer.close();
                //tell the main code that we are done with consumer
                latch.countDown();
            }
        }
        public void shutDown(){
            //.wakeup() is the special method to interrupt consumer.poll()
            //it will throw an exception - W akeUpException
            consumer.wakeup();
        }

    }
}
