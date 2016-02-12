package main.java.consumer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import main.java.timeseries.TimeseriesCustom;
import main.java.timeseries.SegmentCustom;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import main.java.streaming.ignite.server.IgniteCacheConfig;
import main.java.streaming.ignite.server.MeasurementInfo;

import org.apache.ignite.*;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.stream.StreamTransformer;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ConsumerKafka implements Runnable, Serializable {

	private KafkaConsumer consumer;
    private final String topic;
    private final int tid;

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
        props.put("auto.offset.reset", "earliest"); // added this for topic partitioning
        props.put("key.deserializer", "main.java.serialization.TimeseriesDecoder");
        props.put("value.deserializer", "main.java.serialization.TimeseriesDecoder");
        
        consumer = new KafkaConsumer<>(props);
    }
    
	@Override
    public void run() {
        // log4j writes to stdout for now
        // org.apache.log4j.BasicConfigurator.configure();
		// Ignition.setClientMode(true);

        try {
        	// Listen on a specific topic partition
        	TopicPartition par = new TopicPartition(topic, tid);
        	
        	//consumer.subscribe(Arrays.asList(topic));
        	consumer.assign(Arrays.asList(par));
        	
        	IgniteConfiguration conf = new org.apache.ignite.configuration.IgniteConfiguration();
        	conf.setGridName(String.valueOf("Grid" + tid));
        	
        	// This configuration lets multiple clients to start on the same machine
//        	TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
//        	commSpi.setLocalAddress("localhost");
//        	commSpi.setLocalPortRange(100);
//        	
//        	conf.setCommunicationSpi(commSpi);
        	// ================================= ||
        	conf.setClientMode(true);

        	Ignite ignite = Ignition.start(conf);
			IgniteCache<String, MeasurementInfo> streamCache = 
					ignite.getOrCreateCache(IgniteCacheConfig.timeseriesCache());
			
			IgniteDataStreamer<String, MeasurementInfo> stmr = 
					ignite.dataStreamer(streamCache.getName());
			
			stmr.allowOverwrite(true);
			
			stmr.receiver(new StreamTransformer<String, MeasurementInfo>() {

				@Override
				public Object process(MutableEntry<String, MeasurementInfo> e, Object... arg)
						throws EntryProcessorException {
					
					e.setValue((MeasurementInfo) arg[0]);
					
					return null;
				}
            });
			
			Integer secPerWindow = 5;
			float sampleRate = 20;

			Integer windowNum = 0, i = 0; 
			while (true) {

				sampleRate = 20;
				
				ConsumerRecords<String, TimeseriesCustom> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord record : records) {
					TimeseriesCustom data = (TimeseriesCustom) record.value();
					
					System.out.println("Record key: " + record.key());
					System.out.println("Record topic: " + record.topic());
					System.out.printf("Partitoin number = %d, tid = %d\n", record.partition(), tid);
					
					for (SegmentCustom segment : data.getSegments()) {
						// Overwrite the sample rate to be sure
						sampleRate = segment.getSampleRate();

						for (Integer measurement : segment.getIntegerData()) {
							if (i++ % (sampleRate * secPerWindow) == 0) {
								windowNum++;
							}
							
							//stmr.addData(String.valueOf(tid + "_" + i), 
							//		new MeasurementInfo(tid, windowNum, measurement));
						}
					}
				}
				//stmr.flush(); // Flush out all of the data to the cache
				//Runtime run = Runtime.getRuntime();
				//System.out.println("Memory used: " + (run.totalMemory() - run.freeMemory()));
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
