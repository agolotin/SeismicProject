package main.java.edu.byu.seismicproject.consumer;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import main.java.edu.byu.seismicproject.producer.KafkaTopics;
import main.java.edu.byu.seismicproject.signalprocessing.processing.SeismicStreamProcessor;


/**
 * Consumer run simply starts the ConsumerKafka instances desired.
 * It currently runs only one instance, but could easily be configured
 * to run as many as needed.
 */
public class ConsumerRun {
	
	private final String[] allTopics;
	private final String groupId;
	private final String zkHost;
	
	// Just in case we need to keep track of executors
	private final List<ExecutorService> executors;
	
	public ConsumerRun(Properties inputProps) {
		allTopics = inputProps.getProperty("topics").split(",");
		groupId = inputProps.getProperty("groupid");
		zkHost = inputProps.getProperty("zookeeper_host");
		
		executors = new ArrayList<ExecutorService>();
	}

	/**
	 * Entry point for ConsumerRun, calls runConsumers function.
	 * @param The input argument is a .properties file
	 */
	public static void main(String[] args) {
		// In the args input file we specify the number of consumers per topic, 
		//	group id, and a list of topic for all sets of consumers
    	if (args.length != 1) {
    		System.out.println("USAGE: java -cp target/SeismicProject-X.X.X.jar "
    				+ "main.java.consumer.ConsumerRun input/consumer.input.properties");
    		System.exit(1);
    	}
    	
    	try {
			Properties inputProps = new Properties();
			FileInputStream in = new FileInputStream(args[0]);
			inputProps.load(in);
			in.close();
			
			// Load the detectors from disk so we have them in cache for now...
			SeismicStreamProcessor.BootstrapDetectors();
			ConsumerRun runner = new ConsumerRun(inputProps);
			
			runner.runConsumers();
    	}
    	catch (Exception e) {
            Logger.getLogger(ConsumerRun.class.getName()).log(Level.SEVERE, null, e);
    	}
	}
	
	/**
	 * Runs the consumers desired by the user. Called directly by main().
	 */
	public void runConsumers()
	{
		KafkaTopics topicCreator = new KafkaTopics(zkHost);
		
		for (int topicNum = 0; topicNum < allTopics.length; topicNum++) {
			String topic = allTopics[topicNum];
			// Get topic metadata to find out how many consumers we need
			TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, topicCreator.getZkUtils());
			int partitionsPerTopic = topicMetadata.partitionsMetadata().size();
			
			final ExecutorService executor = Executors.newFixedThreadPool(partitionsPerTopic);
			//final List<ConsumerKafka> consumers = new ArrayList<>();

			for (int consumerNum = 0; consumerNum < partitionsPerTopic; consumerNum++) {
				ConsumerKafka consumer = new ConsumerKafka(groupId, topic, consumerNum);
				//consumers.add(consumer);
				executor.submit(consumer);
			}
			executors.add(executor);
		}
		
		topicCreator.close();
	}
}


/*
Runtime.getRuntime().addShutdownHook(new Thread() {
  @Override
  public void run() {
    for (ConsumerKafka consumer : consumers) {
      consumer.shutdown();
    } 
    executor.shutdown();
    try {
      executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
});
*/