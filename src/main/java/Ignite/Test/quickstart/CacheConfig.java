package main.java.Ignite.Test.quickstart;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;

import org.apache.ignite.configuration.CacheConfiguration;
import java.util.concurrent.*;

public class CacheConfig 
{
	public static CacheConfiguration<String, Long> wordCache() 
	{
		CacheConfiguration<String, Long> config = new CacheConfiguration<String, Long>("words");
		// Index individual words.
		config.setIndexedTypes(String.class, Long.class);
		// Sliding window of 1 seconds.
		config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 5))));
		return config;
	}
}