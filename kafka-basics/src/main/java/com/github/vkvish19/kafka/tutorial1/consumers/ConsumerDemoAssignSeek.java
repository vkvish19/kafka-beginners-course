package com.github.vkvish19.kafka.tutorial1.consumers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek
{
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
    
    public static void main(String[] args)
    {
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        
        // assign and seek are mostly used to replay data or fetch a specific message
        
        //assign
        String topic = "first_topic";
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));
        
        //seek
        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);
        
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;
        
        // poll for new data
        while(keepOnReading)
        {
             ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        
            for(ConsumerRecord record : records)
            {
                numberOfMessagesReadSoFar += 1;
                logger.info(String.format("key : %s, value : %s", record.key(), record.value()));
                logger.info(String.format("Partition : %d, Offset : %d", record.partition(), record.offset()));
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead)
                {
                    keepOnReading = false;
                    break;  // exit for-loop
                }
            }
        }
        logger.info("Exiting the application...");
    }
}
