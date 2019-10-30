package com.github.vkvish19.kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


public class TwitterProducer
{
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    
    String consumerKey = "2YsCRlLtW05C3hhUnvnUvwM2R";
    String consumerSecret = "HKpGXjviz1ljOe07Hvobu9BZJdEKxwyMaXdAgVzxVTK8gZE62T";
    String token = "315167851-FBRoyPpno96iCMvGqfuANbODUSkxhGYmfkVFHkbn";
    String secret = "XunsLatSmGhDvX5ZwUqkqAb18tGgdKpBMwsf1qn90vaUu";
    
    private List<String> terms = Lists.newArrayList("ManUtd", "bitcoin", "india");
    
    private TwitterProducer() {}
    
    /**
     *
     * Before running this, make sure that zookeeper and kafka are running.
     */
    public static void main(String[] args)
    {
        new TwitterProducer().run();
    }
    
    public void run()
    {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //create a twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();
    
        //create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();
    
        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping the Application...");
            logger.info("Shutting down the client from Twitter...");
            client.stop();
            logger.info("Closing the kafka producer...");
            producer.close();
            logger.info("Application stopped successfully!!!");
        }));
        
        //loop to send tweets to kafka
        while(!client.isDone())
        {
            String msg = null;
            try
            {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            }
            catch(InterruptedException e)
            {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null)
            {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback()
                {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e)
                    {
                        if(e != null)
                        {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }
    
    protected Client createTwitterClient(BlockingQueue<String> msgQueue)
    {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        
        hosebirdEndpoint.trackTerms(terms);
    
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
    
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
    
        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
    
    public KafkaProducer<String, String> createKafkaProducer()
    {
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        //below properties are added to create Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        
        //high throughput producer properties (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
    
    
        return new KafkaProducer<>(properties);
    }
}
