package main.java.consumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
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
		allTopics = ((String) inputProps.get("topics")).split(",");
		String[] tempNumConsumers = ((String) inputProps.get("numconsumers")).split(",");
		
		if (tempNumConsumers.length != allTopics.length) {
			System.out.println("Number of topics does not match the size of the list of consumers");
			System.exit(1);
		}
		
		allNumConsumers = new Integer[tempNumConsumers.length];
		for (int i = 0; i < tempNumConsumers.length; i++) {
			allNumConsumers[i] = Integer.parseInt(tempNumConsumers[i]);
		}
		
		groupId = (String) inputProps.get("groupid");
		
		executors = new ArrayList<ExecutorService>();
	}

	/**
	 * Entry point for ConsumerRun, calls runConsumers function.
	 * @param The input argument is a .properties file
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// In the args input file we specify the number of consumers per topic, 
		//	group id, and a list of topic for all sets of consumers
    	if (args.length != 1) {
    		System.out.println("USAGE: java -cp target/SeismicProject-X.X.X.jar "
    				+ "main.java.consumer.ConsumerRun input/consumer.input.properties");
    		System.exit(1);
    	}
    	
    	Properties inputProps = new Properties();
    	FileInputStream in = new FileInputStream(args[0]);
    	inputProps.load(in);
    	in.close();
    	
		ConsumerRun runner = new ConsumerRun(inputProps);
		
		runner.runConsumers();
	}
	
	/**
	 * Runs the consumers desired by the user. Called directly by main().
	 */
	public void runConsumers()
	{
		for (String topic : allTopics) {
			for (Integer singleNumConsumers : allNumConsumers) {
				final ExecutorService executor = Executors.newFixedThreadPool(singleNumConsumers);
				final List<ConsumerKafka> consumers = new ArrayList<>();

				for (int i = 0; i < singleNumConsumers; i++) {
					ConsumerKafka consumer = new ConsumerKafka(i, groupId, topic);
					consumers.add(consumer);
					executor.submit(consumer);
				}
				executors.add(executor);
			}
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