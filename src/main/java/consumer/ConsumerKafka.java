package main.java.consumer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import main.java.general.timeseries.SegmentCustom;
import main.java.general.timeseries.TimeseriesCustom;
import main.java.ignite.server.IgniteCacheConfig;
import main.java.signalprocessing.StreamIdentifier;
import main.java.signalprocessing.StreamProducer;

import org.apache.ignite.*;
import org.apache.ignite.configuration.IgniteConfiguration;

@SuppressWarnings({"unchecked", "rawtypes", "serial"})
public class ConsumerKafka implements Runnable, Serializable {

	private final KafkaConsumer consumer;
    private final String topic;
    private final int tid;
    
	// TODO: This will have to be a command line parameter...probably
    private final double sampleInterval;
    private StreamIdentifier id;

    /* I am trying to integrate StreamProducer and ConsumerKafka into one class... kind of */
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
        
        // REVIEWME: if I'm understanding it correctly, this will set up the sample interval to be 5 seconds in milliseconds...
        sampleInterval = 5000;
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
        	conf.setGridName(String.valueOf("Grid" + tid + "-" + topic));
        	
        	/* REVIEWME: Review what communication spi does...
        	TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        	commSpi.setLocalAddress("localhost");
        	commSpi.setLocalPortRange(100);
        	
        	conf.setCommunicationSpi(commSpi);
        	*/
        	conf.setClientMode(true);
        	Ignite ignite = Ignition.start(conf);
        	
			IgniteCache<String, StreamProducer> streamCache = 
					ignite.getOrCreateCache(IgniteCacheConfig.timeseriesCache(topic));
			IgniteDataStreamer<String, StreamProducer> dataStreamer = 
					ignite.dataStreamer(streamCache.getName());
			
			while (true) {
				ConsumerRecords<String, TimeseriesCustom> records = consumer.poll(Long.MAX_VALUE);
				this.streamDataToCache(records, streamCache, dataStreamer);
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
	private void streamDataToCache(ConsumerRecords<String, TimeseriesCustom> records, IgniteCache<String, StreamProducer> streamCache, 
			IgniteDataStreamer<String, StreamProducer> dataStreamer) {

		for (ConsumerRecord record : records) {
			System.out.printf("Record topic = %s, partitoin number = %d, tid = %d\n", record.topic(), record.partition(), tid);
			
			// override the window number each time new consumer record comes in
			// Integer windowNum = 0; 
		
			TimeseriesCustom data = (TimeseriesCustom) record.value();
			SegmentCustom segment = data.getSegment();
			float[] mainData = segment.getMainData();
			
			// An incoming stream might have a new id...
			id = new StreamIdentifier(data.getNetworkCode(), data.getStationCode(), data.getChannelCode(), data.getLocation());
			// Create a stream producer that will handle everything...
			StreamProducer segmentDataStream = new StreamProducer(id, mainData, segment.getStartTime(), 
					segment.getEndTime(), sampleInterval, segment.getSampleRate());
			
			// Make sure we are not overwriting existing windows in cache
			// have the thread busy wait until we can put a new window
			while (streamCache.get(String.valueOf(tid)) != null) {
			/*
				System.out.printf("tid = %d, there is data in Ignite cache "
						+ "associated with window number = %d, i = %d\n", tid, windowNum, i);
			*/
			}
			// XXX: We need to figure out what is going to be the key for our cache...
			dataStreamer.addData(String.valueOf(tid), segmentDataStream);
			
		}
	}
    
    public void shutdown() {
        consumer.wakeup();
    }
}
