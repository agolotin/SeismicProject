package streaming.ignite;

import java.io.IOException;
import java.util.List;

import main.java.timeseries.TimeseriesCustom;
import main.java.timeseries.SegmentCustom;

import java.text.*;
import java.util.*;

import edu.iris.dmc.criteria.*;  
import edu.iris.dmc.service.*;
import edu.iris.dmc.timeseries.model.Timeseries;

import main.java.streaming.ignite.server.IgniteCacheConfig;

import org.apache.ignite.*;
import org.apache.ignite.stream.StreamTransformer;


public class IgniteStream 
{
	
	public static void main(String[] args) {
		IgniteStream stream = new IgniteStream();
		stream.runIgniteStream();
	}
	
	public void runIgniteStream()
	{
		// Mark this cluster member as client.
		Ignition.setClientMode(true);
/*
		try (Ignite ignite = Ignition.start()) 
		{
			IgniteCache<Map<Integer, Integer>, Integer> streamCache = ignite.getOrCreateCache(IgniteCacheConfig.timeseriesCache());

			// Create a streamer to stream words into the cache.
			try (IgniteDataStreamer<Map<Integer, Integer>, Integer> stmr = ignite.dataStreamer(streamCache.getName())) 
			{
				/*
				// Allow data updates.
				stmr.allowOverwrite(true);

				// Configure data transformation to count instances of the same word.
				stmr.receiver(StreamTransformer.from((window, measurement) -> 
				{
					System.out.println("windowNum = " + window.getKey());
					System.out.println("measurement = " + measurement[0]);

					return null;
				}));

				Integer secPerWindow = 5;
				Integer windowNum = 0;
				int i = 0;
				
				TimeseriesCustom data = this.getIrisMessageTest();
				for (SegmentCustom segment : data.getSegments()) {
					// System.out.println("[IRIS DATA] Total amount of data for 10 seconds = " + segment.getIntegerData().size());
					for (Integer measurement : segment.getIntegerData()) {
						if (i++ % (20 * secPerWindow) == 0) {
							windowNum++;
						}
						
						//stmr.addData(windowNum, measurement);
					}
				}
			} 
			catch (IOException ex) 
			{
				ex.printStackTrace();
			}
		}
				*/
	}
	
	
	private TimeseriesCustom getIrisMessageTest() throws IOException {
		ServiceUtil serviceUtil = ServiceUtil.getInstance();
		serviceUtil.setAppName("SeismicEventsData");
		WaveformService waveformService = serviceUtil.getWaveformService();
		
		DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date startDate = null;
		Date endDate = null;
		try {
			startDate = dfm.parse("2015-02-17T00:00:00");
			endDate = dfm.parse("2015-02-17T00:00:10");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		WaveformCriteria criteria = new WaveformCriteria();

		String s = "IU-KBS-00-BHZ";
		
		String[] s_info = s.split("-");

		String network = s_info[0];
		String station = s_info[1];
		String channel = s_info[2];
		String loc_id = s_info[3];
		
		criteria.add(network, station, channel, loc_id, startDate, endDate);
		
		List<Timeseries> timeSeriesCollection = null;
		try {
			timeSeriesCollection = waveformService.fetch(criteria);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Timeseries timeseries = timeSeriesCollection.get(0);
		
		TimeseriesCustom ts = new TimeseriesCustom(timeseries.getNetworkCode(), timeseries.getStationCode(), timeseries.getLocation(), timeseries.getChannelCode());
		ts.setSegments(timeseries.getSegments());
		ts.setChannel(timeseries.getChannel());
		ts.setDataQuality(timeseries.getDataQuality());
		
		return ts;
	}
}
