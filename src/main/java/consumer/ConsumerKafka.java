package main.java.consumer;

import java.util.Arrays;
import java.util.Properties;

import main.java.timeseries.TimeseriesCustom;
import main.java.timeseries.SegmentCustom;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import main.java.streaming.ignite.server.IgniteCacheConfig;

import org.apache.ignite.*;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ConsumerKafka implements Runnable {

	private KafkaConsumer consumer;
    private String topic;
    private int tid;

    public ConsumerKafka(int tid, String group_id, String topic) {
    	this.tid = tid;
    	this.topic = topic;

        // Set up the consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", group_id);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "main.java.producer.decoder.TimeseriesDecoder");
        props.put("value.deserializer", "main.java.producer.decoder.TimeseriesDecoder");
        
        consumer = new KafkaConsumer<>(props);
    }
    
	@Override
    public void run() {
        // log4j writes to stdout for now
        // org.apache.log4j.BasicConfigurator.configure();
		Ignition.setClientMode(true);

        try {
        	consumer.subscribe(Arrays.asList(topic));

        	Ignite ignite = Ignition.start();
			IgniteCache<Integer, Integer> streamCache = ignite.getOrCreateCache(IgniteCacheConfig.timeseriesCache());
			IgniteDataStreamer<Integer, Integer> stmr = ignite.dataStreamer(streamCache.getName());
				
			float sampleRate = 20;
			Integer secPerWindow = 5;

			Integer windowNum, i;
			
			while (true) {

				// Later, before reseting the windowNum 
				// we'll have to query cache to see that that window has 
				// already been processed to avoid race conditions
				
				// Also, since we are writing to a single cache, we'll have to make sure
				// that multiple producers are not overriding data for a window
				
				// Maybe make the key of the cache a map....

				windowNum = 0;
				i = 0;
				 
				ConsumerRecords<String, TimeseriesCustom> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord record : records) {
					TimeseriesCustom data = (TimeseriesCustom) record.value();
					
					for (SegmentCustom segment : data.getSegments()) {
						// Overwrite the sample rate to be sure
						sampleRate = segment.getSampleRate();

						for (Integer measurement : segment.getIntegerData()) {
							if (i++ % (sampleRate * secPerWindow) == 0) {
								windowNum++;
							}
							
							System.out.printf("window number = %d, data point = %d", windowNum, measurement);
							System.out.println();

							stmr.addData(windowNum, measurement);
						}
					}
				}
			}
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
        finally {
        	consumer.close();
        }
    }
    
    public void shutdown() {
        consumer.wakeup();
    }
}
