package main.java.streaming.ignite.server;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;

import main.java.timeseries.TimeseriesCustom;

import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.concurrent.*;

public class IgniteCacheConfig 
{
	// Key is the number of the window, and value is the measurement
	public static CacheConfiguration<Integer, Integer> timeseriesCache() 
	{
		CacheConfiguration<Integer, Integer> config = new CacheConfiguration<Integer, Integer>("seismic-data");
		// Index individual words.
		config.setIndexedTypes(Integer.class, Long.class);
		
		/*
		CacheTypeMetadata type = new CacheTypeMetadata();
		type.setValueType(Integer.class.getName());
		*/
		
		// Sliding window of 5 seconds.
		// config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1))));
		return config;
	}
}