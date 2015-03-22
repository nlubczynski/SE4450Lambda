package com.se4450.data;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class TestProducer {
    public static void main(String[] args) {
        long events = Long.parseLong(args[0]);
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "kafka1:9092,kafka2:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               int id = rnd.nextInt(6) + 1;
               int value = (rnd.nextInt(1000) + 1);
               long timestamp = System.currentTimeMillis()/1000;
               String output = id + " " + value + " " + timestamp;
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("sensorData", "/sensorData", output);
               producer.send(data);
        }
        producer.close();
    }
}