package main.java.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.*;
import java.util.*;

import edu.iris.dmc.criteria.*;  
import edu.iris.dmc.service.*;
import edu.iris.dmc.timeseries.model.Segment;
import edu.iris.dmc.timeseries.model.Timeseries;

public class IrisTest {

	public static void main(String[] args) {
		IrisTest tester = new IrisTest();
		tester.run();
	
	}
	
	private void run() {
		ServiceUtil serviceUtil = ServiceUtil.getInstance();
		serviceUtil.setAppName("Tutorials");
		WaveformService waveformService = serviceUtil.getWaveformService();
		
		
		DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date startDate = null;
		Date endDate = null;
		try {
			startDate = dfm.parse("2005-02-17T00:00:00");
			endDate = dfm.parse("2005-02-17T00:01:00");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		WaveformCriteria criteria = new WaveformCriteria();
		criteria.add("IU", "KBS", "00", "BHZ", startDate, endDate);
		
		List<Timeseries> timeSeriesCollection = null;
		try {
			timeSeriesCollection = waveformService.fetch(criteria);
		} catch (NoDataFoundException | IOException | CriteriaException | ServiceNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		for(Timeseries timeseries:timeSeriesCollection){
			   System.out.println(timeseries.getNetworkCode() + "-" +
			   timeseries.getStationCode() + " (" + timeseries.getChannelCode() + "), loc:" +
			      timeseries.getLocation());
			      for(Segment segment:timeseries.getSegments()){
			         System.out.printf("Segment:\n");
			         System.out.printf("  Start: %s", segment.getStartTime());
			         System.out.printf("  End: %s", segment.getEndTime());
			         System.out.printf(":  %d samples exist in this segment\n",
			            segment.getSampleCount());         
			   }
		}
		
	}

	
	public int checkSize() {
		Serializable ser = null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		
		try {
			oos = new ObjectOutputStream(baos);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			oos.writeObject(ser);
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return baos.size();
	}
	

}
