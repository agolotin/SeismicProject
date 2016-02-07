package streaming.ignite;

import java.util.List;
import java.util.concurrent.TimeUnit;

import main.java.streaming.ignite.server.IgniteCacheConfig;
import main.java.timeseries.TimeseriesCustom;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;

public class IgniteQuery
{
	public static void main(String[] args) throws Exception 
	{
		// Mark this cluster member as client.
		Ignition.setClientMode(true);

		try (Ignite ignite = Ignition.start()) 
		{
			IgniteCache<Integer, Integer> stmCache = ignite.getOrCreateCache(IgniteCacheConfig.timeseriesCache());

			/*
			try (QueryCursor cursor = stmCache.query(new ScanQuery((k, p) -> p == 1))) {
				//for (Person p : cursor)
					  //System.out.println(p.toString());
				System.out.println();
			}
			*/
			// Select top 10 words.
			SqlFieldsQuery top10Qry = new SqlFieldsQuery(
					"select _key from Long limit 10");

			// Query top 10 popular words every 5 seconds.
			while (true) 
			{
				// Execute queries.
				List<List<?>> top10 = stmCache.query(top10Qry).getAll();

				System.out.println(top10.toString());

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
