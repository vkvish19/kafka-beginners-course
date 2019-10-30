package com.github.vkvish19.kafka.tutorial1.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo
{
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String GROUP_ID = "my-fourth-application";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
    
    public static void main(String[] args)
    {
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        
        String topic = "first_topic";
        
        //subscribe consumer to out topic(s)
        consumer.subscribe(Collections.singleton(topic));
        
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
}
