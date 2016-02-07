package main.java.Ignite.Test.quickstart;

import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;

public class QueryWords 
{
	public static void main(String[] args) throws Exception 
	{
		// Mark this cluster member as client.
		Ignition.setClientMode(true);

		try (Ignite ignite = Ignition.start()) 
		{
			IgniteCache<String, Long> stmCache = ignite.getOrCreateCache(CacheConfig.wordCache());

			// Select top 10 words.
			SqlFieldsQuery top10Qry = new SqlFieldsQuery(
					"select _key, _val from Long order by _val desc limit 10");

			// Query top 10 popular words every 5 seconds.
			while (true) 
			{
				// Execute queries.
				List<List<?>> top10 = stmCache.query(top10Qry).getAll();

				// Print top 10 words.
//				ExamplesUtils.printQueryResults(top10);

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
