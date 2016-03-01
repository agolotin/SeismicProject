package main.java.edu.byu.seismicproject.consumer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import main.java.edu.byu.seismicproject.ignite.server.IgniteCacheConfig;
import main.java.edu.byu.seismicproject.signalprocessing.CorrelationDetector;
import main.java.edu.byu.seismicproject.signalprocessing.DetectionStatistic;
import main.java.edu.byu.seismicproject.signalprocessing.DetectorHolder;
import main.java.edu.byu.seismicproject.signalprocessing.StreamIdentifier;
import main.java.edu.byu.seismicproject.signalprocessing.StreamSegment;

import org.apache.ignite.*;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.stream.StreamTransformer;

@SuppressWarnings({"unchecked", "rawtypes", "serial"})
public class ConsumerKafka implements Runnable, Serializable {
	
	//private OffsetManager offsetManager;

	private final KafkaConsumer consumer;
    private final String topic;
    private final int tid;
    
	// TODO: This will have to be a command line parameter...probably
    private double blockSize;

    public ConsumerKafka(int tid, String group_id, String topic, String externalOffsetStorage) {
    	this.tid = tid;
    	this.topic = topic;
    	//offsetManager = new OffsetManager(externalOffsetStorage);

        // Set up the consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", group_id);
        props.put("enable.auto.commit", "false");
        //props.put("auto.commit.interval.ms", "999999999");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "main.java.edu.byu.seismicproject.general.serialization.StreamSegmentDecoder");
        props.put("value.deserializer", "main.java.edu.byu.seismicproject.general.serialization.StreamSegmentDecoder");
        
        consumer = new KafkaConsumer<>(props);
    }
    
    /**
     * TODO: Fix documentation
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
        	// TODO: In the future, implement dynamic rebalancing of partitions 
        	//		That is, implement a different type of ConsumerRebalanceListener..
        	//		As of now, we are just going to have a consumer listen on a specific partition
        	//consumer.subscribe(Arrays.asList(topic));
        	//		new EventConsumerRebalanceListener(consumer, offsetManager.getStorageName()));
        	
        	
        	// Have consumer listen on a specific topic partition
        	TopicPartition par = new TopicPartition(topic, tid);
        	consumer.assign(Arrays.asList(par));
        	
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
        	
			IgniteCache<String, DetectorHolder> streamCache = 
					ignite.getOrCreateCache(IgniteCacheConfig.detectorHolderCache());
			IgniteDataStreamer<String, DetectorHolder> dataStreamer = 
					ignite.dataStreamer(streamCache.getName());
			
			dataStreamer.allowOverwrite(true);
			dataStreamer.receiver(StreamTransformer.from((e, arg) ->{ 
				// e will be the key-value pair, and arg is the new incoming list (of size 1) of new things
				// Get the detector holder for this key
				System.out.println("Length of incoming arguments for stream "
						+ "transformer is NOT one: " + String.valueOf(arg.length != 1));
				
				DetectorHolder detectors = e.getValue();
				if (detectors == null) {
					detectors = new DetectorHolder();
				}
				
				// TODO: Add more things to detector holder...
				for (Object detector : arg)
					detectors.add(((DetectorHolder) detector).getDetectors());
				
				return null; // This function should not really return anything
			}));
			
			//
			while (true) {
				ConsumerRecords<String, StreamSegment> records = consumer.poll(Long.MAX_VALUE);
				this.processIncomingData(records, par, streamCache, dataStreamer);
				// Make sure the data that was not get send to the cache was sent
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
	
	/**
	 * As of now, this method is thread safe. Sine a consumer listens on a specific topic partition,
	 * we are guaranteed at-lest-one delivery. That means that until we commit our offset, if the process
	 * fails and if we were not able to commit the last processed offset, when it consumer picks up the partition log, 
	 * it will seek to the last committed offset. It is possible that it this case the messages will be duplicated, but
	 * this method introduces fault tolerance into our consumer
	 * @param records
	 * @param topicPartition
	 * @param streamCache
	 * @param dataStreamer
	 */
	private void processIncomingData(ConsumerRecords<String, StreamSegment> records, 
									 TopicPartition topicPartition, 
									 IgniteCache<String, DetectorHolder> streamCache, 
									 IgniteDataStreamer<String, DetectorHolder> dataStreamer) {

		long currentRecordNum = 0, recordsToCommit = records.records(topicPartition).size();
		// Just see if we have more than 50 records to commit then we're 
		//		going to commit every 1/5 of total records
		if (recordsToCommit > 50) {
			recordsToCommit = records.records(topicPartition).size() / 5;
		}
		
		System.out.println("tid = " + tid + ", records to commit for this poll = " + recordsToCommit);
		
		for (ConsumerRecord record : records) {
			System.out.printf("Record topic = %s, partitoin number = %d, tid = %d, offset = %d\n", 
					record.topic(), record.partition(), tid, record.offset());
			
			StreamSegment segment = (StreamSegment) record.value();
			
			System.out.println("tid = " + tid + ", " + segment.toString());
			System.out.println();
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) { }
		
			/*
			TimeseriesCustom data = (TimeseriesCustom) record.value();
			SegmentCustom timeseriesSegment = data.getSegment();
			float[] mainData = timeseriesSegment.getMainData();
			final int secondsPerBlock = 5;
			
			// An incoming stream might have a new id
			// XXX: Seismic band is null right now, don't do that....
			id = new StreamIdentifier(data.getNetworkCode(), data.getStationCode(), data.getChannelCode(), data.getLocation(), null);
			// Create a stream producer that will handle everything
			StreamProducer segmentDataStream = new StreamProducer(id, mainData, timeseriesSegment.getStartTime(), 
					timeseriesSegment.getEndTime(), secondsPerBlock, timeseriesSegment.getSampleRate());

			
			// =========================================================== || Here comes the fun part...
		   // XXX: Need to create a function for this that will make sure it 
		   //	either takes stuff from cache or creates a new correlation detector
			double streamStart = segmentDataStream.getStartTime();
			StreamSegment previous = null;
			// Actual calculation of detection statistics
			while (segmentDataStream.hasNext()) {
				
			   StreamSegment segment = segmentDataStream.getNext();
			   System.out.println(segment.getStartTime());
			   
			   if (previous != null) {
				  StreamSegment combined = StreamSegment.combine(segment, previous);
				  streamStart = previous.getStartTime(); // FIXME: Is this right?
				  
				  // FIXME: make sure there's something in cache at all
				  for (CorrelationDetector detector : streamCache.get(segmentDataStream.toString()).getDetectors()) { 
					 if (detector.isCompatibleWith(combined)) {
					   DetectionStatistic statistic = detector.produceStatistic(combined);
					   writeStatistic( detector, statistic, streamStart);
					   // Put stuff into cache instead
					 }
				  }
			   }
			   previous = segment;
			}
			// =========================================================== ||
			
			*/
			//offsetManager.saveOffsetInExternalStore(partition.topic(), partition.partition(), lastoffset);
			
			// See if we want to commit the offset 
			if (currentRecordNum++ == recordsToCommit) { 
				//NOTE: The committed offset should always be the offset of the next message that the application will read. 
				//	Thus, when calling commitSync(offsets) we should add one to the offset of the last message processed
				currentRecordNum = 0;
				long lastoffset = record.offset() + 1; 
				consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastoffset)));
				System.out.println(consumer.committed(topicPartition));
			}
		}
		consumer.commitSync(); // Commit the offsets that have not been committed
	}
    
    private void writeStatistic(CorrelationDetector detector, DetectionStatistic statistic, double streamStart) {
		// TODO: Implement this method
	}

	public void shutdown() {
        consumer.wakeup();
    }
}
