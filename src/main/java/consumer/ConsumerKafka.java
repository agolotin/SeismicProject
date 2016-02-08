package main.java.consumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
        props.put("key.deserializer", "main.java.serialization.TimeseriesDecoder");
        props.put("value.deserializer", "main.java.serialization.TimeseriesDecoder");
        
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
			IgniteCache<Map<Integer, Integer>, Integer> streamCache = 
					ignite.getOrCreateCache(IgniteCacheConfig.timeseriesCache());

			IgniteDataStreamer<Map<Integer, Integer>, Integer> stmr = 
					ignite.dataStreamer(streamCache.getName());
				
			float sampleRate = 20;
			Integer secPerWindow = 5;

			Integer windowNum, i;
			
			while (true) {

				// Later, before reseting the windowNum 
				// we'll have to query cache to see that that window has 
				// already been processed to avoid race conditions
				
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
							
							System.out.printf("tid = %d, window number = %d, data point = %d\n", 
									tid, windowNum, measurement);

							stmr.addData(new HashMap<Integer, Integer>(tid, windowNum), measurement);
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
