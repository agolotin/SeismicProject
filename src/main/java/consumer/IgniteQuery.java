package main.java.consumer;

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
			IgniteCache<Integer, MeasurementInfo> stmCache = ignite.getOrCreateCache(IgniteCacheConfig.timeseriesCache());


			while (true) {
				System.out.println(stmCache.size(CachePeekMode.ALL));
				Thread.sleep(5000);
			}
			//scanQuery(ignite, stmCache);
			/*
			
			SqlQuery qry = new SqlQuery(MeasurementInfo.class, "windowNum = 1");
			try (QueryCursor<Entry<Long, MeasurementInfo>> cursor = stmCache.query(qry)) {
				  for (Entry<Long, MeasurementInfo> e : cursor)
				    System.out.println(e.getValue().toString());
				}
			try (QueryCursor cursor = stmCache.query(new 
						ScanQuery<Integer, MeasurementInfo>((k, p) -> p.getWindowNum() == 1))) {
				while (cursor.iterator().hasNext()) {
					System.out.println(cursor.iterator().next());
				}
			}
			*/
			// Select top 10 words.
			/*
			SqlFieldsQuery top10Qry = new SqlFieldsQuery("SELECT * FROM \"seismic-data\".MeasurementInfo WHERE windowNum == 1 LIMIT 10");

			// Query top 10 popular words every 5 seconds.
			while (true) 
			{
				// Execute queries.
				List<List<?>> top10 = stmCache.query(top10Qry).getAll();

				System.out.println(top10.toString());

				Thread.sleep(5000);
			}
			*/
		}
	}
	
    @SuppressWarnings({"serial", "unchecked"})
    private static void scanQuery(Ignite ignite, IgniteCache<Integer, MeasurementInfo> stmCache) {
        IgniteCache<BinaryObject, BinaryObject> cache = ignite.cache(stmCache.getName()).withKeepBinary();

		ScanQuery<BinaryObject, BinaryObject> scan = new ScanQuery(
            new IgniteBiPredicate<BinaryObject, BinaryObject>() {
                @Override 
                public boolean apply(BinaryObject key, BinaryObject person) {
                	System.out.println(key.getClass());
                	System.out.println(person.getClass());
                    return false;//person.<Integer>field("windowNum") == 1;
                }
            }
        );

        // Execute queries for salary ranges.
        //print("People with salaries between 0 and 1000 (queried with SCAN query): ", cache.query(scan).getAll());
        System.out.println(cache.query(scan).getAll());
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
