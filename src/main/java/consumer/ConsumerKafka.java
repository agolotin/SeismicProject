package main.java.consumer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import main.java.general.timeseries.SegmentCustom;
import main.java.general.timeseries.TimeseriesCustom;
import main.java.streaming.ignite.server.IgniteCacheConfig;
import main.java.streaming.ignite.server.MeasurementInfo;

import org.apache.ignite.*;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.stream.StreamTransformer;

@SuppressWarnings({"unchecked", "rawtypes", "serial"})
public class ConsumerKafka implements Runnable, Serializable {

	private final KafkaConsumer consumer;
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
        props.put("key.deserializer", "main.java.general.serialization.TimeseriesDecoder");
        props.put("value.deserializer", "main.java.general.serialization.TimeseriesDecoder");
        
        consumer = new KafkaConsumer<>(props);
    }
    
    /**
     * Generic thread function that runs the thread. Sets up the environment to send
     * data to Ignite server and cache the data received from the ProducerKafka.
     * Sets up a topic partition, an Ignite configuration for the cache,
     * starts Ignite client and opens a connection to the Ignite server.
     * At the end of the function the infinite loop is entered and the client starts to
     * wait on incoming connections.
     */
	@Override
    public void run() {
        // log4j writes to stdout for now
        // org.apache.log4j.BasicConfigurator.configure();

        try {
        	TopicPartition par = new TopicPartition(topic, tid);
        	
        	// Have consumer listen on a specific topic partition
        	consumer.assign(Arrays.asList(par));
        	consumer.seekToEnd(par);
        	
        	IgniteConfiguration conf = new IgniteConfiguration();
        	// Since multiple consumers will be running on a single node, 
        	//	we need to specify different names for them
        	conf.setGridName(String.valueOf("Grid" + tid));
        	
        	// REVIEWME: Review what communication spi does...
        	//TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        	//commSpi.setLocalAddress("localhost");
        	//commSpi.setLocalPortRange(100);
        	
        	//conf.setCommunicationSpi(commSpi);
        	conf.setClientMode(true);
        	Ignite ignite = Ignition.start(conf);
        	
			IgniteCache<String, List<MeasurementInfo>> streamCache = 
					ignite.getOrCreateCache(IgniteCacheConfig.timeseriesCache(topic));
			IgniteDataStreamer<String, List<MeasurementInfo>> dataStreamer = 
					ignite.dataStreamer(streamCache.getName());
			
			// For some reason we have to overwrite the value of 
			//	what's being put into cache...otherwise it doesn't work
			// TESTME: Try get rid of these next 15 or so lines and test Ignite query
			dataStreamer.allowOverwrite(true);
			
			dataStreamer.receiver(new StreamTransformer<String, List<MeasurementInfo>>() {

				@Override
				public Object process(MutableEntry<String, List<MeasurementInfo>> e, Object... arg)
						throws EntryProcessorException {
					
					e.setValue((List<MeasurementInfo>) arg[0]);
					
					return null;
				}
            });
			
			// TODO: This will have to be a command line parameter...probably
			Integer secPerWindow = 5;

			while (true) {
				ConsumerRecords<String, TimeseriesCustom> records = consumer.poll(Long.MAX_VALUE);
				this.sendData(records, streamCache, dataStreamer, secPerWindow);
				// Make sure the data that did not get send to the cache was sent
				dataStreamer.flush();
			}
        }
        catch(Exception e) {
        	e.printStackTrace();
        }
        finally {
        	consumer.close();
        }
    }
	
	// Sends data to the Ignite server for caching.
	// Loops through list of records and for each record
	// breaks the data up into measurements, then sends the
	// measurements to the Ignite cache
	private void sendData(ConsumerRecords<String, TimeseriesCustom> records, IgniteCache<String, List<MeasurementInfo>> streamCache, 
			IgniteDataStreamer<String, List<MeasurementInfo>> dataStreamer, Integer secPerWindow) {

		for (ConsumerRecord record : records) {
			System.out.printf("Record topic = %s, partitoin number = %d, tid = %d\n", record.topic(), record.partition(), tid);
			
			// override the window number each time new consumer record comes in
			Integer windowNum = 0; 
		
			TimeseriesCustom data = (TimeseriesCustom) record.value();
			SegmentCustom segment = data.getSegment();
			
			// Overwrite the sample rate to be sure
			final float sampleRate = segment.getSampleRate();
			
			List<MeasurementInfo> seismicDataList = new ArrayList<MeasurementInfo>();
			List mainData = segment.getMainData();
			
			for (int pos = 0; pos < mainData.size(); pos++) {
				// Add the new measurement to the list
				seismicDataList.add(new MeasurementInfo(tid, windowNum, mainData.get(pos)));
				
				// If we have added enough data for a single window 
				if (seismicDataList.size() % (sampleRate * secPerWindow) == 0) {
					// Make sure we are not overwriting existing windows in cache
					// have the thread busy wait until we can put a new window
					while (streamCache.get(tid + "_" + windowNum) != null) {
					/*
						System.out.printf("tid = %d, there is data in Ignite cache "
								+ "associated with window number = %d, i = %d\n", tid, windowNum, i);
					*/
					}
					// Once we are sure the previous window with the same number was processed 
					//	for that consumer, we put a new window with this number
					dataStreamer.addData(String.valueOf(tid + "_" + windowNum), seismicDataList);
					// Clear the list that contains all data for a single window
					seismicDataList.clear();
					// Increment the window number
					windowNum++;
				}
			}
		}
	}
    
    public void shutdown() {
        consumer.wakeup();
    }
}
