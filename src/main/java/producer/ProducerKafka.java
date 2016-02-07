package main.java.producer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import kafka.common.TopicExistsException;
import main.java.timeseries.TimeseriesCustom;

import java.text.*;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import edu.iris.dmc.criteria.*;  
import edu.iris.dmc.service.*;
import edu.iris.dmc.timeseries.model.Timeseries;

public class ProducerKafka {

    private KafkaProducer producer;
	List<String> stationList = new ArrayList<String>();

    public ProducerKafka() {}
    
    public static void main(String[] args) {

        ProducerKafka prod = new ProducerKafka();
        try {
            prod.runKafkaProducer();
        } catch (TopicExistsException | IOException e) {
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
        props.put("key.serializer", "main.java.producer.encoder.TimeseriesEncoder");
        props.put("value.serializer", "main.java.producer.encoder.TimeseriesEncoder");

        this.producer = new KafkaProducer<>(props);
    }

	public void runKafkaProducer() throws IOException {
        // log4j writes to stdout for now
        org.apache.log4j.BasicConfigurator.configure();

        try {
            this.defineKafkaProducer();
        } catch (IOException e) {
            System.out.println("Could not define Kafka Producer settings");
            e.printStackTrace();
        }

        List<Timeseries> timeSeriesCollection = this.getIrisMessage();
        
        // There are 20 entries per second of IRIS data
        for (Timeseries timeseries : timeSeriesCollection) {
        	TimeseriesCustom ts = new TimeseriesCustom(timeseries.getNetworkCode(), timeseries.getStationCode(), timeseries.getLocation(), timeseries.getChannelCode());
        	ts.setSegments(timeseries.getSegments());
        	ts.setChannel(timeseries.getChannel());
        	ts.setDataQuality(timeseries.getDataQuality());
        	
			ProducerRecord<String, TimeseriesCustom> data = new ProducerRecord<String, TimeseriesCustom>("test", ts);
			this.producer.send(data);
        }
        this.producer.close();
    }

	private List<Timeseries> getIrisMessage() throws IOException {
		ServiceUtil serviceUtil = ServiceUtil.getInstance();
		serviceUtil.setAppName("SeismicEventsData");
		WaveformService waveformService = serviceUtil.getWaveformService();
		
		DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date startDate = null;
		Date endDate = null;
		try {
			startDate = dfm.parse("2005-02-17T00:00:00");
			endDate = dfm.parse("2005-02-17T00:00:10");
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		return timeSeriesCollection;
	}
}
