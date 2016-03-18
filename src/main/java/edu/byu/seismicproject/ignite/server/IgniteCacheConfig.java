package main.java.edu.byu.seismicproject.ignite.server;

import main.java.edu.byu.seismicproject.signalprocessing.DetectorHolder;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;

//import javax.cache.configuration.FactoryBuilder;
//import javax.cache.expiry.CreatedExpiryPolicy;
//import javax.cache.expiry.Duration;



//import java.util.concurrent.*;

/**
 * IgniteCacheConfig provides the configuration for the Ignite caches
 * used by the KafkaConsumers (which have built in Ignite clients) to define 
 * how data is stored on the server. 
 */
public class IgniteCacheConfig 
{
	/**
	 * The key passed to the cache configuration is a String with the 
	 * thread ID concatenated with the number of the window and spaced 
	 * with a dash (-).
	 * Value is a list of measurements from the streaming data typed as an array list of MeasurementInfo objects. 
	 * @param there are multiple caches, specifically one cache per topic (or station that we are pulling data from)
	 * @return new instance of the cache or the current instance of cache depending on a topic
	 */
	public static CacheConfiguration<String, DetectorHolder> detectorHolderCache() 
	{
		CacheConfiguration<String, DetectorHolder> config = new 
				CacheConfiguration<String, DetectorHolder>("detectors");
		
		// Index individual measurements ->	throws an error if values are stored off heap. we cannot index off heap values
		// config.setIndexedTypes(String.class, DetectorHolder.class);
		// Set the amount of time we want our entries to persist in cache
		// config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(TimeUnit.HOURS, 5))));
		// NOTE: If we don't set up the expiration policy, everything will be in cache always
		
		
		// Make sure the cache is partitioned over multiple nodes
		config.setCacheMode(CacheMode.PARTITIONED);
		// This allows multiple ignite clients that run on the same machine to concurrently write to cache
		config.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY);
		// This memory mode stored keys on heap and values off heap. Good for large keys and small values
		config.setMemoryMode(CacheMemoryMode.OFFHEAP_VALUES);
		// 0 means unlimited memory
		config.setOffHeapMaxMemory(0); 
		// Enable swap space storage
		// config.setSwapEnabled(true);  // TODO: Configure swap spi on server... or client, not sure which one
	
		return config;
	}
	
	/*
	 * The key passed to the cache configuration is a String with the 
	 * hashcode of the StreamIdentifier, the topic name, and the partition
	 * number from a record separated with dashes (-).
	 * 
	 * Thus the key is: streamIdentifier#-topic-partitionNum
	 */
	@SuppressWarnings("rawtypes")
	public static CacheConfiguration<String, ConsumerRecord> recordHolderCache() 
	{
		CacheConfiguration<String, ConsumerRecord> config = new 
				CacheConfiguration<String, ConsumerRecord>("records");
		
		// Make sure the cache is partitioned over multiple nodes
		config.setCacheMode(CacheMode.PARTITIONED);
		// This allows multiple ignite clients that run on the same machine to concurrently write to cache
		config.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY);
		// This memory mode stored keys on heap and values off heap. Good for large keys and small values
		config.setMemoryMode(CacheMemoryMode.OFFHEAP_VALUES);
		// 0 means unlimited memory
		config.setOffHeapMaxMemory(0); 
		// Enable swap space storage
		// config.setSwapEnabled(true);  // TODO: Configure swap spi on server... or client, not sure which one
	
		return config;
	}
}