package main.java.ignite.process;

//TODO: these commented imports can probably be removed
//import java.util.Map.Entry;
//import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;

import main.java.ignite.server.IgniteCacheConfig;
import main.java.ignite.server.MeasurementInfo;

//import org.apache.ignite.binary.BinaryObject;
//import org.apache.ignite.cache.query.QueryCursor;
//import org.apache.ignite.cache.query.ScanQuery;
//import org.apache.ignite.cache.query.SqlQuery;
//import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * IgniteQuery is used to pull data from the Ignite server cache after 
 * caching has been performed by the ConsumerKafka instances. IgniteQuery 
 * creates a connection to the cache using an Ignite Client, performs a 
 * query, and prints the result to the console. This is effectively debug 
 * code as currently implemented, although parts will be used in the full 
 * application.
 */
public class IgniteQuery {
	
	private Integer consumerID;
	private String topic;
	/**
	 * Entry point for the IgniteQuery class. No args are needed. Automatically
	 * calls runQuery
	 */
	public static void main(String[] args) throws Exception {
		IgniteQuery querier = new IgniteQuery();
		querier.runQuery();
	}
	
	public IgniteQuery() {
		consumerID = 0;
		topic = "test2";
	}

	/**
	 * Runs a query to the cache that IgniteCacheConfig.timeseriesCache configures.
	 * Starts an Ignite client, formats a general SQL query, and repeatedly queries
	 * all matching rows from the cache. These rows are printed to the console and
	 * deleted from the cache.  
	 */
	public void runQuery() {
		// Mark this cluster member as client.
		Ignition.setClientMode(true);

		try (Ignite ignite = Ignition.start()) {
			
			IgniteCache<String, List<MeasurementInfo>> streamCache = ignite
					.getOrCreateCache(IgniteCacheConfig.timeseriesCache(topic));

			// Select all of the entries for a single window depending on the
			// window number
			//SqlFieldsQuery query = new SqlFieldsQuery("select _key, _val from measurementinfo where "
			//		+ "measurementinfo.windownum = ? and measurementinfo.tid = ?");

			int i = 0;
			while (true) {
				// Execute queries.

				System.out.println("Size of cache = " + streamCache.size(CachePeekMode.ALL) + "; i = " + i);
				List<MeasurementInfo> rs = streamCache.getAndRemove(consumerID + "-" + i);
				System.out.println(rs);
				i++;
//				List<List<?>> result = streamCache.query(query.setArgs(i, consumerID)).getAll();
//
//				if (!result.isEmpty()) {
//					Set<String> toDelete = new HashSet<String>();
//					for (List<?> l : result) {
//						toDelete.add((String) l.get(0));
//					}
//					streamCache.removeAll(toDelete);
//					i++;
//				}
//
				Thread.sleep(1000);
			}
		}
		catch (InterruptedException e) {

		}
	}
}
