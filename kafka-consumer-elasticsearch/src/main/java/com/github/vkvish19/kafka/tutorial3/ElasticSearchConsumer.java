package com.github.vkvish19.kafka.tutorial3;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer
{
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String GROUP_ID = "kafka-demo-elasticsearch";
    
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getSimpleName());
    
    private static RestHighLevelClient createClient()
    {
        //////////////////////////
        /////////// IF YOU USE LOCAL ELASTICSEARCH
        //////////////////////////
    
        //  String hostname = "localhost";
        //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));
    
    
        //////////////////////////
        /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
        //////////////////////////
    
        // replace with your own credentials
        String hostname = "kafka-beginner-cours-5854870234.ap-southeast-2.bonsaisearch.net";    //localhost or bonsai url
        String username = "15dhaqay21"; //needed only for bonsai
        String password = "rmxk7c451e"; //needed only for bonsai
    
        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
    
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        
        return new RestHighLevelClient(builder);
        

    }
    
    private static KafkaConsumer<String, String> createConsumer(String... topics)
    {
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // disable auto commit of offsets.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        
        consumer.subscribe(Arrays.asList(topics));
        
        return consumer;
    }
    
    public static void main(String[] args) throws IOException
    {
        RestHighLevelClient client = createClient();
        
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        
        // poll for new data
        while(true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    
            Integer recordCount = records.count();
            
            //COMMENT THIS BULK REQUEST IF YOU WANT TO TRY OUT ONE REQUEST AT A TIME.
            BulkRequest bulkRequest = new BulkRequest();
        
            logger.info("Received " + recordCount + " records...");
            for(ConsumerRecord record : records)
            {
                String recordValueString = record.value().toString();
                
                // two strategies to generate ID
                //1. kafka generic ID: topic_partition_offset
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                
                //2. twitter feed specific ID
                try
                {
                    String id = extractIdFromTweet(recordValueString);
                    String userName = extractUserNameFromTweet(recordValueString);
    
                    //here we insert data into elasticsearch
                    IndexRequest indexRequest = new IndexRequest("twitter", "_doc", id);
                    indexRequest.source(record.value().toString(), XContentType.JSON);
                    bulkRequest.add(indexRequest);  // we add to our bulk request (takes no time)
                    logger.info(String.format("id : %s, userName : %s", id, userName));
                }
                catch(NullPointerException e)
                {
                    logger.warn("Skipping bad data : " + record.value());
                }

                //UNCOMMENT BELOW IF YOU WANT TO TRY OUT ONE REQUEST AT A TIME.
                /*IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                // using the 'id' you can check using: <url>/twitter/_doc/<id>
                try
                {
                    Thread.sleep(10); // introduce a small delay.
                }
                catch(InterruptedException e)
                {
                    e.printStackTrace();
                }*/
            }
            if(recordCount > 0)
            {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed...");
                try
                {
                    Thread.sleep(1000);
                }
                catch(InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }
        
        //close the client gracefully
        //client.close();
        //logger.info("ElasticSearch client closed gracefully...");
    }

    private static String extractIdFromTweet(String recordValueString)
    {
        //gson library used.
        return JsonParser.parseString(recordValueString)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
    
    private static String extractUserNameFromTweet(String recordValueString)
    {
        //gson library used.
        return JsonParser.parseString(recordValueString)
                .getAsJsonObject()
                .get("user")
                .getAsJsonObject()
                .get("name")
                .getAsString();
    }
}


// set the below fields in elasticsearch for that particular 'index' using PUT /<index>/_settings
// {"index.mapping.total_fields.limit": 100000, "index.mapping.depth.limit": 8000}

//CONVENTION: setting heartbeat.interval.ms to 1/3rd of session.timeout.ms