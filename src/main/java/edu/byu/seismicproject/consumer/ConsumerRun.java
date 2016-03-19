package main.java.edu.byu.seismicproject.consumer;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import main.java.edu.byu.seismicproject.signalprocessing.processing.SeismicStreamProcessor;


/**
 * Consumer run simply starts the ConsumerKafka instances desired.
 * It currently runs only one instance, but could easily be configured
 * to run as many as needed.
 */
public class ConsumerRun {
	
	private final Integer[] allNumConsumers;
	private final String[] allTopics;
	private final String groupId;
	
	// Just in case we need to keep track of executors
	private final List<ExecutorService> executors;
	
	public ConsumerRun(Properties inputProps) {
		allTopics = inputProps.getProperty("topics").split(",");
		String[] tempNumConsumers = ((String) inputProps.get("numconsumers")).split(",");
		
		if (tempNumConsumers.length != allTopics.length) {
			String msg = "Number of topics does not match the size of the list of consumers";
            Logger.getLogger(ConsumerRun.class.getName()).log(Level.SEVERE, msg);
			System.exit(1);
		}
		
		allNumConsumers = new Integer[tempNumConsumers.length];
		for (int i = 0; i < tempNumConsumers.length; i++) {
			allNumConsumers[i] = Integer.parseInt(tempNumConsumers[i]);
		}
		
		groupId = inputProps.getProperty("groupid");
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
		for (int topicNum = 0; topicNum < allTopics.length; topicNum++) {
			final ExecutorService executor = Executors.newFixedThreadPool(allNumConsumers[topicNum]);
			//final List<ConsumerKafka> consumers = new ArrayList<>();

			for (int consumerNum = 0; consumerNum < allNumConsumers[topicNum]; consumerNum++) {
				ConsumerKafka consumer = new ConsumerKafka(groupId, allTopics[topicNum], consumerNum);
				//consumers.add(consumer);
				executor.submit(consumer);
			}
			executors.add(executor);
		}
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