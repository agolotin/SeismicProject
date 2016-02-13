package main.java.consumer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import main.java.streaming.ignite.server.IgniteCacheConfig;
import main.java.streaming.ignite.server.MeasurementInfo;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.lang.IgniteBiPredicate;

public class IgniteQuery
{
	public static void main(String[] args) throws Exception 
	{
		// Mark this cluster member as client.
		Ignition.setClientMode(true);

		try (Ignite ignite = Ignition.start()) 
		{
			IgniteCache<String, MeasurementInfo> stmCache = ignite.getOrCreateCache(IgniteCacheConfig.timeseriesCache());
			stmCache.clear();
			
			// Select all of the entries for a single window depending on the window number
			SqlFieldsQuery qry = new SqlFieldsQuery(
					"select _key, _val from measurementinfo where "
					+ "measurementinfo.windownum = ? and measurementinfo.tid = ?");

			int i = 1;
			while (true) 
			{
				// Execute queries.
				List<List<?>> result = stmCache.query(qry.setArgs(i, 1)).getAll();

				System.out.println(result.toString());
				System.out.println(stmCache.size(CachePeekMode.ALL));
				
				if (!result.isEmpty()) {
					Set<String> toDelete = new HashSet<String>();
					for (List<?> l : result) {
						toDelete.add((String) l.get(0));
					}
					stmCache.removeAll(toDelete);
					i++;
				}
				
				Thread.sleep(5000);
			}
		}
	}
	
}































//public class QueryWords 
//{
//	public static void main (String[] args) throws Exception
//	{
//		Ignition.setClientMode(true);
//		Ignite ignite = Ignition.start();
//		IgniteCache<String, Long> stmCache = ignite.getOrCreateCache(CacheConfig.wordCache());
//		SqlFieldsQuery top10Qry = new SqlFieldsQuery(
//				"select _val, count(_val) as cnt from String " + 
//					"group by _val " + 
//					"order by cnt desc " + 
//					"limit 10",
//					true /*collocated*/
//				);
//		
//		// Query top 10 popular numbers every 5 seconds.
//		while (true)
//		{
//			// Execute queries.
//			List<List<?>> top10 = stmCache.query(top10Qry).getAll();
//
//			// Print top 10 words.
//			System.out.println(top10);
//			Thread.sleep(5000);
//		}
//	}
//}
