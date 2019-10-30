package com.github.vkvish19.kafka.tutorial1.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread
{
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String GROUP_ID = "my-sixth-application";
    private final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
    
    public static void main(String[] args)
    {
        new ConsumerDemoWithThread().run();
    }
    
    private ConsumerDemoWithThread() {}
    
    private void run()
    {
        String topic = "first_topic";
        // latch for dealing with multiple threads
        CountDownLatch latch =  new CountDownLatch(1);
    
        // create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(BOOTSTRAP_SERVERS, topic, GROUP_ID, latch);
        
        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        
        // add a shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook.");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try
            {
                latch.await();
            }
            catch(InterruptedException e)
            {
                logger.error("Application got interrupted...", e);
            }
            finally
            {
                logger.info("Application has exited...");
            }
        }));
        
        try
        {
            latch.await();
        }
        catch(InterruptedException e)
        {
            logger.error("Application got interrupted", e);
        }
        finally
        {
            logger.info("Application is closing...");
        }
    }
    
    public class ConsumerRunnable implements Runnable
    {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
    
        public ConsumerRunnable(String bootstrapServers, String topic, String groupId, CountDownLatch latch)
        {
            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            
            this.latch = latch;
            consumer = new KafkaConsumer<>(properties);
    
            //subscribe consumer to out topic(s)
            consumer.subscribe(Collections.singleton(topic));
            
        }
        
        @Override
        public void run()
        {
            try
            {
                // poll for new data
                while(true)
                {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        
                    for(ConsumerRecord record : records)
                    {
                        logger.info(String.format("key : %s, value : %s", record.key(), record.value()));
                        logger.info(String.format("Partition : %d, Offset : %d", record.partition(), record.offset()));
                    }
                }
            }
            catch(WakeupException e)
            {
                logger.error("Received shutdown signal!");
            }
            finally
            {
                consumer.close();
                latch.countDown();
            }
        }
        
        public void shutdown()
        {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception - WakeUpException
            
            consumer.wakeup();
        }
    }
}
