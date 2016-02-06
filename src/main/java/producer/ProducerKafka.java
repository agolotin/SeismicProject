package main.java.producer;

import java.io.IOException;
import java.util.Properties;

import kafka.common.TopicExistsException;

import java.text.*;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import edu.iris.dmc.criteria.*;  
import edu.iris.dmc.service.*;
import edu.iris.dmc.timeseries.model.Segment;
import edu.iris.dmc.timeseries.model.Timeseries;

public class ProducerKafka {

    private KafkaProducer<String, String> producer;
	List<String> stationList = new ArrayList<String>();

    public ProducerKafka() {}
    
    public static void main(String[] args) {

        ProducerKafka prod = new ProducerKafka();
        try {
            prod.runKafkaProducer();
        } catch (TopicExistsException e) {
            e.printStackTrace();
        }

    }

    public void defineKafkaProducer() throws IOException {
        // Define producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
    }

    public void runKafkaProducer() {
        System.out.println("Kafka Producer starting...");
        // log4j writes to stdout for now
        org.apache.log4j.BasicConfigurator.configure();

        try {
            this.defineKafkaProducer();
        } catch (IOException e) {
            System.out.println("Could not define Kafka Producer settings");
            e.printStackTrace();
        }

        this.producer.send(new ProducerRecord<>("test", "key", "message"));
        this.producer.close();
    }

	private void getIrisMessage() throws IOException {
		ServiceUtil serviceUtil = ServiceUtil.getInstance();
		serviceUtil.setAppName("Seismic Event data");
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

		stationList.add("IU-KBS-00-BHZ");
		for (String info : stationList) {
			String[] s_info = info.split("-");

			String network = s_info[0];
			String station = s_info[1];
			String channel = s_info[2];
			String loc_id = s_info[3];
			
			criteria.add(network, station, channel, loc_id, startDate, endDate);
		}
		
		List<Timeseries> timeSeriesCollection = null;
		try {
			timeSeriesCollection = waveformService.fetch(criteria);
		} catch (NoDataFoundException | IOException | CriteriaException | ServiceNotSupportedException e) {
			e.printStackTrace();
		}
		
		for(Timeseries timeseries : timeSeriesCollection){
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
}
