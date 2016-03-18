package main.java.edu.byu.seismicproject.ignite.process;

//TODO: these commented imports can probably be removed
//import java.util.Map.Entry;
//import java.util.Arrays;
import java.io.IOException;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterGroup;


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
	 * @throws IOException 
	 */
	public void runQuery() throws IOException {
		// Mark this cluster member as client.
		Ignition.setClientMode(true);

		try (Ignite ignite = Ignition.start()) {
			
/*
			final IgniteCache<String, StreamProducer> streamCache = ignite
					.getOrCreateCache(IgniteCacheConfig.timeseriesCache(topic));
			// Actual signal processing code... 
			DetectorHolder detectors = new DetectorHolder();

			int blockSizeSamps = CorrelationDetector.BLOCK_SIZE;
            StreamProducer stream = new StreamProducer(blockSizeSamps);
            double streamStart = stream.getStartTime();

			StreamSegment previous = null;

			while (stream.hasNext()) {
				
			   StreamSegment segment = stream.getNext();
			   System.out.println(segment.getStartTime());
			   
			   if (previous != null) {
			      StreamSegment combined = StreamSegment.combine(segment, previous);
			      
			      for (CorrelationDetector detector : detectors.getDetectors()) {
			         if (detector.isCompatibleWith(combined)) {
					   DetectionStatistic statistic = detector.produceStatistic(combined);
					   writeStatistic( detector, statistic, streamStart);
			         }
			      }
			   }
			   previous = segment;
			}

			
			
			
			
			

			ClusterGroup remotes = ignite.cluster().forRemotes();
			IgniteCompute compute = ignite.compute(remotes);

			for (int key = 0; key < Integer.MAX_VALUE; key++) {
			    // This closure will execute on the remote node where
			    // data with the 'key' is located.
			    compute.affinityRun(streamCache.getName(), String.valueOf(consumerID + '-' + key), () -> { 
			        // Peek is a local memory lookup.
			    	System.out.println("Size of local cache: " + streamCache.size(CachePeekMode.NEAR));
			    });
			}

			int i = 0;
			while (true) {
				// Execute queries.

				System.out.println("Size of cache = " + streamCache.size(CachePeekMode.ALL) + "; i = " + i);
				StreamProducer rs = streamCache.getAndRemove(String.valueOf(i));
				System.out.println(rs);
				i++;
				
				Thread.sleep(1000);
			}
		}
		catch (InterruptedException e) {

		}
*/
		}
	}

}
