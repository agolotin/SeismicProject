package edu.byu.kafka_messaging;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.ConsumerConnector;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerKafka {
    
    /*
    public static void main(String[] args) {
    	ConsumerKafka cons = new ConsumerKafka();
    	
    	try {
			cons.runKafkaConsumers();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    */

//	private ConsumerConnector consumer;
	private kafka.javaapi.consumer.ConsumerConnector consumer;
    private String topic;
    private ExecutorService executor;

    private Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;
    
    public ConsumerKafka() {}
    
    // Entry point for running a consumer code
    public void runKafkaConsumers() throws IOException
    {
    	System.out.println("Kafka Consumer starting...");

		String zooKeeper = "127.0.0.1:2181";
		String groupId = "test"; //Consumer Group this process is consuming on behalf of

		this.topic = "test";
    	// log4j writes to stdout for now
		org.apache.log4j.BasicConfigurator.configure();

		// Set up the consumer
        Properties props = new Properties();
        props.put("zookeeper.connect", zooKeeper);
        props.put("group.id", groupId);
//        props.put("bootstrap.servers", "192.168.0.129:9092");
//        props.put("zookeeper.session.timeout.ms", "400");
//        props.put("zookeeper.sync.time.ms", "200");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("auto.offset.reset", "smallest");

        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

		int threads = 1;
		this.run(threads);
    }
    
    
    public void run(int a_numThreads) {
    	// We are creating a map that tells Kafka how many threads are provided for which topics
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        
        // This specifies how we pass this information to kafka.
        // ere we only asked Kafka for a single Topic but we could have asked for multiple by adding another element to the Map
        consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        
        // now launch all the threads
        executor = Executors.newFixedThreadPool(a_numThreads);
 
        // now create an object to consume the messages
        // we create the thread pool and pass a new ConsumerRunnable object to each thread as our business logic
        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
        	executor.submit(new ConsumerRunnable(stream, threadNumber));
            threadNumber++;
        }
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        } 
    }

}
