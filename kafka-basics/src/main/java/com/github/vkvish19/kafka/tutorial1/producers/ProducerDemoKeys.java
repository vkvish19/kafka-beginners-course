package com.github.vkvish19.kafka.tutorial1.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys
{
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
    
    public static void main(String[] args) throws ExecutionException, InterruptedException
    {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // create Producer
        try(KafkaProducer<String, String> producer = new KafkaProducer<>(properties))
        {
            
            for(int i=1; i<11; i++)
            {
                String topic = "first_topic";
                String key = "id_" + i;
                String value = "hello world #" + i;
    
                logger.info("key = " + key);
                
                // create Producer record
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
    
                // send data - asynchronous
                producer.send(record, (recordMetadata, e) -> {
                    //this executes every time record is sent or an exception is thrown.
                    if(e == null)
                    {
                        // record was successfully sent
                        logger.info("Received new Metadata.");
                        logger.info(String.format("Topic : %s", recordMetadata.topic()));
                        logger.info(String.format("Partition : %d", recordMetadata.partition()));
                        logger.info(String.format("Offset : %d", recordMetadata.offset()));
                        logger.info(String.format("Timestamp : %d", recordMetadata.timestamp()));
            
                    }
                    else
                    {
                        logger.error("Error occurred while Producing", e);
                    }
        
                }).get();   // block the .send() to make it synchronous - DON'T DO THIS IN PRODUCTION.
            }
        }
    }
}
