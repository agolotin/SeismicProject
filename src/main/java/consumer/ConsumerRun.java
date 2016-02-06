package main.java.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerRun {
	
	public static void main(String[] args) { 
		  int numConsumers = 1;
		  String groupId = "seismic-data";
		  
		  String topic = "test";
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
