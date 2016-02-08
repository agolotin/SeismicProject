package main.java.streaming.ignite.server;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;

import main.java.timeseries.TimeseriesCustom;

import org.apache.ignite.cache.CacheTypeFieldMetadata;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.configuration.CacheConfiguration;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;

public class IgniteCacheConfig 
{
	// Key is the number of the window, and value is the measurement
	@SuppressWarnings("deprecation")
	public static CacheConfiguration<Map<Integer, Integer>, Integer> timeseriesCache() 
	{
		CacheConfiguration<Map<Integer, Integer>, Integer> config = new CacheConfiguration<Map<Integer, Integer>, Integer>("seismic-data");
		// Index individual words.
		config.setIndexedTypes(Map.class, Integer.class);
//		config.setReadThrough(true);
//		config.setWriteThrough(true);
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
	
		// Sliding window of 5 seconds.
		// config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1))));
		return config;
	}
}