package main.java.ignite.server;

//TODO: Remove commented code if unneeded
//import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;

//import main.java.general.timeseries.TimeseriesCustom;
//import org.apache.ignite.cache.CacheTypeFieldMetadata;
//import org.apache.ignite.cache.CacheTypeMetadata;



import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.cache.*;

import java.util.List;
import java.util.concurrent.*;

//import org.apache.ignite.cache.store.*;
//import java.sql.Types;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.LinkedHashMap;
//import java.util.Map;

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
	 * @return new instance of the cache or the currect instance of cache depending on a topic
	 */
	public static CacheConfiguration<String, List<MeasurementInfo>> timeseriesCache(String topic) 
	{
		CacheConfiguration<String, List<MeasurementInfo>> config = new 
				CacheConfiguration<String, List<MeasurementInfo>>("seismic-data-" + topic);
		
		// Index individual measurements ->	throws an error if values are stored off heap. we cannot index off heap values
		// config.setIndexedTypes(String.class, List.class);
		// Set the amount of time we want our entries to persist in cache
		config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(TimeUnit.HOURS, 5))));
		// Make sure the cache is partitioned over multiple nodes
		config.setCacheMode(CacheMode.PARTITIONED);
		// This allows multiple ignite clinets that run on the same machine to concurrently write to cache
		config.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY);
		// This memory mode stored keys on heap and values off heap. Good for large keys and small values
		config.setMemoryMode(CacheMemoryMode.OFFHEAP_VALUES);
		// 0 means unlimited memory
		config.setOffHeapMaxMemory(0); 
		// Enable swap space storage
		// config.setSwapEnabled(true);  // TODO: Configure swap spi on server... or client, not sure which one
		/*
		// Configure cache types. 
        Collection<CacheTypeMetadata> meta = new ArrayList<>();
		
        // key, or window number
        CacheTypeMetadata type = new CacheTypeMetadata();
        meta.add(type);
        
        type.setDatabaseSchema("dbseismic");
        type.setDatabaseTable("windows");
        type.setKeyType(Integer.class.getName());
        type.setValueType(Integer.class.getName());
        
        // Key fields for the key 
        Collection<CacheTypeFieldMetadata> keys = new ArrayList<>();
        keys.add(new CacheTypeFieldMetadata("ConsumerId", Types.INTEGER, "consumerid", Integer.class));
        keys.add(new CacheTypeFieldMetadata("WindowNum", Types.INTEGER, "windownum", Integer.class));
        type.setKeyFields(keys);
        
        // Value fields for the value
        Collection<CacheTypeFieldMetadata> vals = new ArrayList<>();
        vals.add(new CacheTypeFieldMetadata("Measurement", Types.INTEGER, "measurement", Integer.class));
        type.setValueFields(vals);
        
        // Query fields for students.
        Map<String, Class<?>> qryFlds = new LinkedHashMap<>();
        qryFlds.put("consumerid", Integer.class);
        qryFlds.put("windownum", Integer.class);
        qryFlds.put("measurement", Integer.class);
        
        type.setQueryFields(qryFlds);
        
        config.setTypeMetadata(meta);
        */
	
		return config;
	}
}