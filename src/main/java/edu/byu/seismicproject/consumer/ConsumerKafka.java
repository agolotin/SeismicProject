package main.java.edu.byu.seismicproject.consumer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.stream.StreamVisitor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import main.java.edu.byu.seismicproject.ignite.server.IgniteCacheConfig;
import main.java.edu.byu.seismicproject.signalprocessing.CorrelationDetector;
import main.java.edu.byu.seismicproject.signalprocessing.DetectionStatistic;
import main.java.edu.byu.seismicproject.signalprocessing.DetectorHolder;
import main.java.edu.byu.seismicproject.signalprocessing.StreamSegment;

@SuppressWarnings({"unchecked", "rawtypes", "serial"})
public class ConsumerKafka implements Runnable, Serializable {
	
	//private OffsetManager offsetManager;

	private final KafkaConsumer consumer;
    private final String topic;
    private final int tid;
        
    public ConsumerKafka(int tid, String group_id, String topic, String externalOffsetStorage) {
    	this.tid = tid;
    	this.topic = topic;
    	//offsetManager = new OffsetManager(externalOffsetStorage);

        // Set up the consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", group_id);
        props.put("enable.auto.commit", "false");
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
        	
        	//detectorCache stores detection statistic, while recordCache stores the records during 
        	//processing to allow a previous window to be retrieved and processed with the current one
			IgniteCache<String, DetectorHolder> detectorCache = 
					ignite.getOrCreateCache(IgniteCacheConfig.detectorHolderCache());			
			IgniteCache<String, ConsumerRecord> recordCache = 
					ignite.getOrCreateCache(IgniteCacheConfig.recordHolderCache());
			
			while (true) {
				ConsumerRecords<String, StreamSegment> records = consumer.poll(Long.MAX_VALUE);
				this.processIncomingData(records, par, detectorCache, recordCache);
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
	 * we are guaranteed at-least-one delivery. That means that we will process a message at least once. 
	 * If the process fails and if we were not able to commit the last processed offset, when a new consumer 
	 * picks up the partition log, it will seek to the last committed offset. It is possible that in this case 
	 * the messages will be duplicated, but this way of doing things introduces some fault tolerance into our consumer.
	 * TODO: We will have to make sure our consumer group can dynamically rebalance its consumers and come up 
	 * 		 with a better way of storing latest offsets...
	 * @param records
	 * @param topicPartition
	 * @param streamCache
	 * @param detectorStreamer
	 * @param recordCache
	 */
	private void processIncomingData(ConsumerRecords<String, StreamSegment> records, 
									 TopicPartition topicPartition, 
									 IgniteCache<String, DetectorHolder> detectorCache, 
									 IgniteCache<String, ConsumerRecord> recordCache) {

		long currentRecordNum = 0, recordsToCommit = records.records(topicPartition).size();
		// If we have more than 50 records to commit, we commit every 1/5 of total records
		//   The number 50 was chosen arbitrarily...
		if (recordsToCommit > 50) {
			recordsToCommit = records.records(topicPartition).size() / 5;
		}
		
		System.out.println("tid = " + tid + ", records to commit for this poll = " + recordsToCommit);		
		
		for (ConsumerRecord currentRecord : records) {
			//Logic: We need the previous record from the cache in order to calculate stats,
			//so I first pass the current record to the getPreviousRecord method. If it returns null,
			//I cache the current record and move on. If it comes back with a value, I still cache 
			//the current record (I'll need it next time), but I do calculate stats .
			
			//Debug code
			/*******************************************/
			//System.out.println("\n\n");
			//System.out.println(record.toString());
			//System.out.println("\n\n");
			//System.out.printf("Record topic = %s, partition number = %d, tid = %d, offset = %d\n", 
			//		record.topic(), record.partition(), tid, record.offset());
			
			//StreamSegment segment = (StreamSegment) record.value();
			
			//System.out.println("NEW INCOMING WINDOW: tid = " + tid + ", " + segment.toString());

			//End debug code
			/*******************************************/
			StreamSegment currentSegment = (StreamSegment) currentRecord.value();
			ConsumerRecord previousRecord = this.getPreviousRecord(currentRecord, currentSegment, recordCache);
		
			if (previousRecord != null) {
				
				// This is where we check to see if we are duplicating the messages
				//		because the last record we cached should have a lower offset
				//		that a completely new message
				// NOTE: In case if we delete a topic, and then start it again.
				//		 it is really important that we clear the Ignite's recordCache
				//		 as well. If we don't do it, the framework will think that
				//		 all of the new messages are actually old messages. 
				//		 To account for it we just have to change this check to be more
				//		 sophisticated.
				if (previousRecord.offset() > currentRecord.offset())
					continue;
				/*
				System.out.println("PREVIOUS BLOCK: " + previousRecord.value());
				System.out.println("NEW INCOMING BLOCK: " + currentSegment);
				
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) { }
				*/
				StreamSegment previousSegment = (StreamSegment) previousRecord.value();
				if (previousSegment.isPreviousTo(currentSegment)) {
					StreamSegment combined = StreamSegment.combine(currentSegment, previousSegment);
					double streamStart = previousSegment.getStartTime();
					
					DetectorHolder detectors = detectorCache.get(String.valueOf(combined.getId().hashCode()));
					
					// If there are any detectors already in cache, compute detection statistic for each one
					if (detectors != null) {
						for (CorrelationDetector singleDetector : detectors.getDetectors()) {
							if (singleDetector.isCompatibleWith(combined)) {
								// XXX: Something's wrong with our produceStatistic logic...
								DetectionStatistic statistic = singleDetector.produceStatistic(combined);
								writeStatistic(singleDetector, statistic, streamStart);
							}
						}
					}
					else { // otherwise create a new detector and throw it into cache 
						CorrelationDetector newDetector = new CorrelationDetector(combined.getId(), combined);
						DetectorHolder detectorHolder = new DetectorHolder(newDetector);
						
						detectorCache.put(String.valueOf(combined.getId().hashCode()), detectorHolder);
					}
				}
			}
			this.cacheRecord(currentRecord, currentSegment, recordCache);			
			
			//long lastoffset = currentRecord.offset() + 1; 
			//consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastoffset)));
			
			//System.out.println("Size of cache = " + detectorCache.size(CachePeekMode.ALL));
			
			//print out the last committed offset for debugging purposes...
			//System.out.println(consumer.committed(topicPartition));
			//System.out.println();
			
			// See if we want to commit the offset 
			if (currentRecordNum++ == recordsToCommit) { 
				//NOTE: The committed offset should always be the offset of the next message that the application will read. 
				//	Thus, when calling commitSync(offsets) we should add one to the offset of the last message processed
				currentRecordNum = 0;
				long lastoffset = currentRecord.offset() + 1; 
				consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastoffset)));
				System.out.println(consumer.committed(topicPartition));
			}			
		}
		consumer.commitSync(); // Commit the offsets that have not been committed...
	}
    
    private void writeStatistic(CorrelationDetector detector, DetectionStatistic statistic, double streamStart) {
    	System.out.printf("detector stream id = %d,\n streamStart = %d,\n statistic = %d\n\n", 
    			detector.getStreamId(), streamStart, statistic.getStatistic());
	}
    
    /**
     * This method will search in an Ignite cache to find previous records that have been streamed.
     * The SeismicBand hash value from the record will be used to key the entry for caching.
     */
    private ConsumerRecord getPreviousRecord(ConsumerRecord currentRecord,
    			StreamSegment newSegment, IgniteCache<String, ConsumerRecord> recordCache) {
    	
    	// Parse out the StreamIdentifier and hash it, then convert the hash to a string 
    	// for Ignite cache key. Key also includes topic and partition
    	// streamIdentifier#-topic-partitionNum

    	String key = Integer.toString(newSegment.getId().hashCode()) + "-" +
    				currentRecord.topic() + "-" + currentRecord.partition();
		
    	ConsumerRecord previousRecord = recordCache.get(key);   	
    	
    	return previousRecord;
    }
    
    /**
     * This method will store a record in an Ignite cache so it can be used for 
     * processing with the next block as that next block (from same band) comes in.
     */
    private void cacheRecord(ConsumerRecord currentRecord, StreamSegment currentSegment, 
    		IgniteCache<String, ConsumerRecord> recordCache) {    	
    	
    	// Parse out the StreamIdentifier and hash it, then convert the hash to a string 
    	// for Ignite cache key. Key also includes topic and partition
    	// streamIdentifier#-topic-partitionNum
    	String key = Integer.toString(currentSegment.getId().hashCode()) + "-" +
    				currentRecord.topic() + "-" + currentRecord.partition();
    	//#streamIdentifier-topic-partitionNum
    	
    	recordCache.put(key, currentRecord);
    }

	public void shutdown() {
        consumer.wakeup();
    }
}
