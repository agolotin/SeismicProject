package main.java.producer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import kafka.common.TopicExistsException;
import main.java.timeseries.TimeseriesCustom;

import java.text.*;
import java.util.*;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import edu.iris.dmc.criteria.*;  
import edu.iris.dmc.service.*;
import edu.iris.dmc.timeseries.model.Segment;
import edu.iris.dmc.timeseries.model.Timeseries;

public class ProducerKafka {

    private KafkaProducer producer;
	List<String> stationList;

    public ProducerKafka() {
    	// the constructor will take a list of stations, which will be topic later on
    	stationList = new ArrayList<String>();
    	
		stationList.add("IU-KBS-00-BHZ");
    }
    
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
        props.put("paritioner.class", "main.java.producer.KafkaPartitioner");
        props.put("key.serializer", "main.java.serialization.TimeseriesEncoder");
        props.put("value.serializer", "main.java.serialization.TimeseriesEncoder");

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

        final String topic = "test2";
        
        List<Timeseries> timeSeriesCollection = this.getIrisMessage();
        
        // There are 20 entries per second of IRIS data
        for (Timeseries timeseries : timeSeriesCollection) {
        	// Split the message into several so the whole chunk of data will go to different partitions
        	List<TimeseriesCustom> ts_list = this.getCustomTimeseriesPatition(
        			timeseries, this.producer.partitionsFor(topic).size());
        	
        	for (TimeseriesCustom ts : ts_list) {
				ProducerRecord<String, TimeseriesCustom> data = new ProducerRecord<String, TimeseriesCustom>(topic, ts);
				this.producer.send(data);
        	}
        }
        this.producer.close();
    }

	private List<TimeseriesCustom> getCustomTimeseriesPatition(Timeseries timeseries, int numPartitions) {
		List<TimeseriesCustom> _tsList = new ArrayList<TimeseriesCustom>();
		
		Collection<Segment> allSegments = timeseries.getSegments();
		Segment segment = allSegments.iterator().next();
		// implement a check to see that there's only int data....
		//List<Integer> measurements = segment.getIntData();
		List<Integer> data = segment.getIntData();
		
		// how much data to send to a single partitioner
		Double seconds = Double.valueOf(segment.getSampleCount()) / segment.getSamplerate();
		Double secondsPerPartition = seconds / Double.valueOf(numPartitions);
	
		for (int i = 0; i < numPartitions; i++) {
			TimeseriesCustom ts = new TimeseriesCustom(timeseries.getNetworkCode(), timeseries.getStationCode(), 
					timeseries.getLocation(), timeseries.getChannelCode());
			ts.setChannel(timeseries.getChannel());
			ts.setDataQuality(timeseries.getDataQuality());
			
			List<Integer> measurementsPerPartition = new ArrayList<Integer>();
			for (int k = (int) (secondsPerPartition * i * segment.getSamplerate()); 
					k < secondsPerPartition * segment.getSamplerate() * (i + 1); k++) {
				
				measurementsPerPartition.add(data.get(k));
			}
			
			// Specify which partition the message is going to go to
			ts.setPartitionNum(i);
			ts.setSegments(allSegments, measurementsPerPartition);
			
			_tsList.add(ts);
		}
        
		return _tsList;
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
			endDate = dfm.parse("2005-02-17T00:10:00");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		WaveformCriteria criteria = new WaveformCriteria();

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
