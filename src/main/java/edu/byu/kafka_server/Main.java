package edu.byu.kafka_server;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import edu.byu.kafka_messaging.ConsumerKafka;
import edu.byu.kafka_messaging.ProducerKafka;

public class Main {

	private static ServerKafka server;
	private Thread kafkaServerRunnable;
	
	public static void main(String[] args) {
		//Main m_server = new Main();
		startKafkaServers();

		ProducerKafka prod = new ProducerKafka();
    	ConsumerKafka cons = new ConsumerKafka();
		try {
			cons.runKafkaConsumers();

			prod.runKafkaProducer();
		}	
		catch (IOException e) {
			System.out.println("Could not run Kafka Producer OR Kafka Consumer");
			e.printStackTrace(System.err);
		}

    	cons.shutdown();
    	server.stop();
	}
	
	public static void startKafkaServers(){
		Properties kafkaProperties = new Properties();
		Properties zkProperties = new Properties();
		
		try {
			//load properties
			kafkaProperties.load(new FileInputStream("server.properties"));
			zkProperties.load(new FileInputStream("zookeeper.properties"));
			
			server = new ServerKafka(kafkaProperties, zkProperties);
			
			/*
			//start kafka
			kafkaServerRunnable = new Thread() {
				
				public void run() {
					try {
						server = new ServerKafka(kafkaProperties, zkProperties);
					} 
					catch (IOException | InterruptedException e) {
						e.printStackTrace();
					}
				}
			};
			kafkaServerRunnable.run();
			*/
			
		} catch (Exception e){
			e.printStackTrace();
			System.err.println("Error running local Kafka broker");
		}
	}
}
