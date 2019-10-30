package com.github.vkvish19.kafka.tutorial1.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo
{
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    
    public static void main(String[] args)
    {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    
        // create Producer
        try(KafkaProducer<String, String> producer = new KafkaProducer<>(properties))
        {
    
            // create Producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world");
    
            // send data
            producer.send(record);
        }
    }
}
