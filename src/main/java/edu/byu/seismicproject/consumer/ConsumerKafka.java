package main.java.edu.byu.seismicproject.consumer;

import main.java.edu.byu.seismicproject.ignite.server.IgniteCacheConfig;
import main.java.edu.byu.seismicproject.signalprocessing.StreamSegment;
import main.java.edu.byu.seismicproject.signalprocessing.processing.SeismicStreamProcessor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


@SuppressWarnings({"unchecked", "rawtypes", "serial"})
public class ConsumerKafka implements Runnable, Serializable {
	
	@IgniteInstanceResource
	private final Ignite igniteInstance;
	
	private final KafkaConsumer consumer;
	private final TopicPartition topicPartition;
    private final long tid;
        
    public ConsumerKafka(Ignite ignite, String group_id, String topic, int par) {
    	this.igniteInstance = ignite;
    	this.tid = par;//Thread.currentThread().getId();
    	this.topicPartition = new TopicPartition(topic, par);

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
     * Generic thread function that runs the thread. Sets up the environment to send
     * data to Ignite cache the data received from the ProducerKafka.
     * Sets up a topic partition, an Ignite configuration for the cache,
     * starts Ignite client and opens a connection to the Ignite server.
     * At the end of the function the infinite loop is entered and the client 
     * waits on incoming connections.
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
        	OffsetAndMetadata lastOffset = consumer.committed(topicPartition);
        	consumer.assign(Arrays.asList(topicPartition));
        	consumer.seek(topicPartition, lastOffset == null ? 0 : lastOffset.offset());
        	
        	System.out.printf("tid = %s, topic = %s, partition = %d, last committed offset = %d\n", 
        			tid, topicPartition.topic(), topicPartition.partition(), lastOffset);
        	
        	//recordCache stores the records during processing to allow a previous window to 
        	// be retrieved and processed with the current one
			IgniteCache<String, ConsumerRecord> recordCache = 
					igniteInstance.getOrCreateCache(IgniteCacheConfig.recordHolderCache());
			
			SeismicStreamProcessor streamProcessor = new SeismicStreamProcessor(igniteInstance);
			
			while (true) {
				ConsumerRecords<String, StreamSegment> records = consumer.poll(Long.MAX_VALUE);
				this.processIncomingData(records, recordCache, streamProcessor);
			}
        }
        catch(Exception e) {
            Logger.getLogger(ConsumerKafka.class.getName()).log(Level.SEVERE, null, e);
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
	 * @param recordCache
	 * @param streamProcessor
	 */
	private void processIncomingData(ConsumerRecords<String, StreamSegment> records, 
									 IgniteCache<String, ConsumerRecord> recordCache,
									 SeismicStreamProcessor streamProcessor) {
		
		//System.out.println("tid = " + tid + ", records to commit for this poll = " + recordsToCommit);		
		
		for (ConsumerRecord currentRecord : records) {
			//Logic: We need the previous record from the cache in order to calculate stats,
			//so we first pass the current record to the getPreviousRecord method. If it returns null,
			//we cache the current record and move on. If it comes back with a value, we still cache 
			//the current record (we'll need it next time), but we do calculate stats .
			
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
				if (previousRecord.offset() >= currentRecord.offset())
					continue;
				
				StreamSegment previousSegment = (StreamSegment) previousRecord.value();
				if (previousSegment.isPreviousTo(currentSegment)) {
					// Analyze 2 segments together...
					streamProcessor.analyzeSegments(currentSegment, previousSegment);
				}
			}
			// Put current record to cache so we can retrieve it as a previous record later
			this.cacheRecord(currentRecord, currentSegment, recordCache);	
			
			//NOTE: The committed offset should always be the offset of the next message that the application will read. 
			//	Thus, when calling commitSync(offsets) we should add one to the offset of the last message processed
			long lastoffset = currentRecord.offset() + 1; 
			consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastoffset)));
			System.out.printf("tid = %d, commit number = %d\n", tid, consumer.committed(topicPartition).offset());
		}
	}
    
	/**
     * This method will search in an Ignite cache to find previous records that have been streamed.
     * The SeismicBand hash value from the record will be used to key the entry for caching.
	 * @param currentRecord
	 * @param newSegment
	 * @param recordCache
	 * @return previousRecord that holds the previous StreamSegment object
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
     * @param currentRecord
     * @param currentSegment
     * @param recordCache
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
