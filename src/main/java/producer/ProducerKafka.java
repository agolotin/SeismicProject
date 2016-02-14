package main.java.producer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import kafka.common.TopicExistsException;
import main.java.general.timeseries.TimeseriesCustom;

import java.text.*;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import edu.iris.dmc.criteria.*;  
import edu.iris.dmc.service.*;
import edu.iris.dmc.timeseries.model.Segment;
import edu.iris.dmc.timeseries.model.Timeseries;

/*
 * ProducerKafka requests the data stream from the IRIS database and 
 * partitions the streams to the KafkaConsumers, which then send the 
 * data to the Ignite Server caches
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ProducerKafka {

	private final String topic;
	private final List<String> stationList;

	private final KafkaProducer producer;
    
   	/**
   	 * 
   	 * Entry point for KafkaProducer. No args are required currently,
   	 * although later should take list of stations as input.
   	 * Statically creates instance of the class and calls run function.
   	 */
    public static void main(String[] args) {
    	// TODO: Make the argument a file.input and have all of the input defined there
        ProducerKafka prod = new ProducerKafka();

        try {
            prod.runKafkaProducer();
        } catch (TopicExistsException | IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Constructor for ProducerKafka. Currently takes no arguments, although it should later
     * receive the incoming list of stations are mentioned in main() comment.
     * The constructor sets the topic name for the ProducerKafka Instance and 
     * configures the Kafka properties.
     */
    public ProducerKafka() {
    	// TODO: the constructor will take a list of stations, which will be topic later on
    	stationList = new ArrayList<String>();
		stationList.add("IU-KBS-00-BHZ");

		topic = "test2";
        
        // Define producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "main.java.general.serialization.TimeseriesEncoder");
        props.put("value.serializer", "main.java.general.serialization.TimeseriesEncoder");

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Configures the log, gets a message from the IRIS service, then loops through the resulting 
     * timeSeriesCollection and redirects the data to the sendSegmentsToPartitions function   
     */
	public void runKafkaProducer() throws IOException {
        // log4j writes to stdout for now
        org.apache.log4j.BasicConfigurator.configure();
        
        List<Timeseries> timeSeriesCollection = this.getIrisMessage();
        
        for (Timeseries timeseries : timeSeriesCollection) {
        	// Split the message into several so the whole chunk of data will go to different partitions
        	this.sendSegmentsToPartitions(timeseries, this.producer.partitionsFor(topic).size());
        }
        this.producer.close();
    }

	/**
	 * This function takes a timeseries object, goes through all of the segments in a single timeseries, and 
	 * sends them all to consumers. Before sending the data it will first split a single object in the collection
	 * of segments into multiple segments in case we have multiple partitions per topic. 
	 * There can be multiple segments per timeseries object. A consumer will receive only one segment at a time 
	 * and process one segment at a time only.
	 * @param timeseries
	 * @param numPartitions
	 */
	private void sendSegmentsToPartitions(Timeseries timeseries, int numPartitions) {
		
		// The problem is that we can't put multiple segments into one. Every chunk has a time frame associated with it. 
		// I will have to send data in chunks...maybe. Consumer is configured to handle this, so maybe I will just put it
		// the way it is, which is multiple Segments. However, we will have to change the way we are putting it into cache. 
		// Mainly, the window numbers will change
		
		
		// I will also have to modify the SegmentsCustom to have just one List of generic objects
		for (Segment segment : timeseries.getSegments()) {
			
			List data = this.discoverMeasurementList(segment);
			// TODO: What if there are multiple types of data in multiple lists...
			
			// Get the number of seconds this segment holds and figure out how many sends to send to a partition
			Double seconds = Double.valueOf(segment.getSampleCount()) / segment.getSamplerate();
			Double secondsPerPartition = seconds / Double.valueOf(numPartitions);
			
			for (int i = 0; i < numPartitions; i++) {
				
				TimeseriesCustom ts = new TimeseriesCustom(timeseries.getNetworkCode(), timeseries.getStationCode(), 
						timeseries.getLocation(), timeseries.getChannelCode());
				
				ts.setChannel(timeseries.getChannel());
				ts.setDataQuality(timeseries.getDataQuality());
				
				List measurementsPerPartition = new ArrayList();
				for (int k = (int) (secondsPerPartition * i * segment.getSamplerate()); 
						k < secondsPerPartition * segment.getSamplerate() * (i + 1); k++) {
					
					measurementsPerPartition.add(data.get(k));
				}
				
				ts.setSegment(segment, measurementsPerPartition);
				
				// Send to topic @topic, partition is @i, key is null, and data is @ts
				ProducerRecord<String, TimeseriesCustom> producerData = new ProducerRecord<String, TimeseriesCustom>(topic, i, null, ts);
				this.producer.send(producerData);
			}
		}
		
	}

	
	/**
	 * This function discovers what type of data a segment holds and returns it as a generic list
	 * @param single_segment
	 * @return data associated with that segment
	 */
	private List discoverMeasurementList(Segment single_segment) {
		List data = null;

		switch(single_segment.getType()) {
			case DOUBLE:
				data = single_segment.getDoubleData();
				break;
			case INTEGER:
				data = single_segment.getIntData();
				break;
			case INT24:
				data = single_segment.getIntData();
				break;
			case FLOAT:
				data = single_segment.getFloatData();
				break;
			case SHORT:
				data = single_segment.getShortData();
				break;
		}
		
		return data;
	}

	//Gets the data streams from IRIS, based on fields set up in the constructor.
	//Returns a list of timeseries objects with the data.
	private List<Timeseries> getIrisMessage() throws IOException {
		ServiceUtil serviceUtil = ServiceUtil.getInstance();
		serviceUtil.setAppName("SeismicEventsData");
		WaveformService waveformService = serviceUtil.getWaveformService();
		
		DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		//TODO: We will want to remove the hardcoding of the time zone and time period
		dateformat.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date startDate = null;
		Date endDate = null;
		try {
			startDate = dateformat.parse("2005-02-17T00:00:00");
			endDate = dateformat.parse("2005-02-17T00:10:00");
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
