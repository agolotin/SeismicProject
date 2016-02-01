package edu.byu.kafka_messaging;

//import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import kafka.javaapi.producer.Producer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.Partitioner;


import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;
import kafka.common.TopicExistsException;


@SuppressWarnings({ "rawtypes", "unchecked" })
public class ProducerKafka {
	
//	private ProducerConfig config;
	private Properties props;
	private Producer producer;
	
	public ProducerKafka() {}
	
	public static void main(String[] args) {
		
		ProducerKafka prod = new ProducerKafka();
		try {
			prod.defineKafkaProducer();
			prod.runKafkaProducer();
		}	
		catch (TopicExistsException | IOException e) {
			e.printStackTrace();
		}
		
	}

	
	public void defineKafkaProducer() throws IOException {
		// Define producer properties
		this.props = new Properties();
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");//"org.bigdata.kafka.Serializer");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "127.0.0.1:9092");//"192.168.1.104:9092");
//		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.104:9092");
//		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
////        props.load(new FileInputStream("producer.properties"));
//		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SimplePartitioner.class.getName());
		 
		// Define actual producer
		// first String is partition key, and the second is the type of the message
		this.producer = new Producer(new kafka.producer.ProducerConfig(this.props));
	}
	
	public void runKafkaProducer() {
    	System.out.println("Kafka Producer starting...");
    	// log4j writes to stdout for now
		org.apache.log4j.BasicConfigurator.configure();
		
		try {
			this.defineKafkaProducer();
		} catch (IOException e) {
			System.out.println("Could not define Kafka Producer settings");
			e.printStackTrace();
		}
		
		// This is a fake message
		// event_data is a topic, IP is a partition key, 
		KeyedMessage<String, byte[]> data = new KeyedMessage("test", "test-message".getBytes(StandardCharsets.UTF_8));
		
		ArrayList<KeyedMessage> messages = new ArrayList<KeyedMessage>();
        messages.add(data);
        
        this.producer.send(messages);
        this.producer.close();
	}

}