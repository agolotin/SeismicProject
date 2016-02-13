package main.java.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerRun {
	
   	// TODO: Make the argument a file.input and have all of the input defined there
	public static void main(String[] args) { 
		// In the args we will have to specify the number of consumers, group id, and a topic for a set of consumers
		// Or we can do a set of topics and with a single command we will spin off all of the consumers that will listen on their specific topic and partition
		  int numConsumers = 3;
		  String groupId = "seismic-events";
		  
		  String topic = "test2";
		  final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

		  final List<ConsumerKafka> consumers = new ArrayList<>();
		  for (int i = 0; i < numConsumers; i++) {
		    ConsumerKafka consumer = new ConsumerKafka(i, groupId, topic);
		    consumers.add(consumer);
		    executor.submit(consumer);
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
		}
}
