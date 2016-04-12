package main.java.edu.byu.seismicproject.producer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import kafka.common.TopicExistsException;

import java.text.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.google.common.primitives.Floats;

import main.java.edu.byu.seismicproject.general.band.SeismicBand;
import main.java.edu.byu.seismicproject.signalprocessing.StreamIdentifier;
import main.java.edu.byu.seismicproject.signalprocessing.IStreamProducer;
import edu.iris.dmc.criteria.*;  
import edu.iris.dmc.service.*;
import edu.iris.dmc.timeseries.model.Segment;
import edu.iris.dmc.timeseries.model.Timeseries;

/**
 * ProducerKafka requests the data stream from the IRIS database and 
 * partitions the streams to the KafkaConsumers, which then send the 
 * data to the Ignite Server caches
 */
@SuppressWarnings("rawtypes")
public class ProducerKafka {

	//private final String topic;
	private final String[] stationList;
	private final SeismicBand[] bands;
	private final int minutesPerBlock; // Assume 60 minutes per block

	private final KafkaProducer producer;
	
    
   	/**
   	 * Entry point for KafkaProducer. 
   	 * Statically creates instance of the class and calls run function.
	 * @param The input argument is a .properties file
   	 */
    public static void main(String[] args) {
    	if (args.length != 1) {
    		System.out.println("USAGE: java -cp target/SeismicProject-X.X.X.jar "
    				+ "main.java.producer.ProducerKafka input/producer.input.properties");
    		System.exit(1);
    	}
    	
    	ProducerKafka prod = null;
        try {
			// Parse input arguments
			Properties inputProps = new Properties();
			FileInputStream in = new FileInputStream(args[0]);
			inputProps.load(in);
			in.close();
			
			// Create a new instance of producer
			prod = new ProducerKafka(inputProps);
        
            prod.runKafkaProducer();
        } catch (Exception e) {
            Logger.getLogger(ProducerKafka.class.getName()).log(Level.SEVERE, null, e);
        }
        finally {
        	prod.producer.close();
        }
    }

    /**
     * Constructor for ProducerKafka. Currently takes no arguments, although it should later
     * receive the incoming list of stations are mentioned in main() comment.
     * The constructor sets the topic name for the ProducerKafka Instance and 
     * configures the Kafka properties.
     */
    public ProducerKafka(Properties inputProps) {
    	
    	stationList = inputProps.getProperty("stations").split(",");
    	bands = this.getBands(inputProps);
    	minutesPerBlock = Integer.parseInt(inputProps.getProperty("minutes_per_block"));
    	
    	// Set up zookeeper client to create new topics
    	String zkHost = inputProps.getProperty("zookeeper_host");
    	this.createTopics(zkHost);
        
        // Define producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("partitioner.class", "main.java.edu.byu.seismicproject.producer.StreamPartitioner");
        props.put("key.serializer", "main.java.edu.byu.seismicproject.general.serialization.StreamSegmentEncoder");
        props.put("value.serializer", "main.java.edu.byu.seismicproject.general.serialization.StreamSegmentEncoder");

        this.producer = new KafkaProducer<>(props);
    }
    
    /**
     * This will make sure all of the topics that we need exist. If some of them don't it will
     * just create a new topic with a specified number of partitions
     * @param zkHost
     */
    private void createTopics(String zkHost) { 
    	// We are going to create as many partitions as there are bands
    	int num_partitions = bands.length;
    	int replication_factor = 1; // We can make it configurable later
    	KafkaTopics topicCreator = new KafkaTopics(zkHost);
    	
    	for (int i = 0; i < stationList.length; i++) {
    		topicCreator.createNewTopic(stationList[i], 
    				num_partitions, 
    				replication_factor);
    	}
    	
    	topicCreator.close();
	}

	/*
     * Reads the input object and sets the of bands required to process streams
     */
    private SeismicBand[] getBands(Properties props) {
		String[] _bands = props.getProperty("bands").split(";");
		
		SeismicBand[] rtrn = new SeismicBand[_bands.length];
		
		for (int i = 0; i < _bands.length; i++) {
			String[] bandProps = _bands[i].split(",");
			int order = Integer.valueOf(bandProps[0]);
			double lowCorner = Double.valueOf(bandProps[1]);
			double highCorner = Double.valueOf(bandProps[2]);
			
			SeismicBand __band = new SeismicBand(i, order, lowCorner, highCorner);
			
			rtrn[i] = __band;
		}
		
		return rtrn;
    }

    /**
     * Configures the log, gets a message from the IRIS service, then loops through the resulting 
     * timeSeriesCollection and redirects the data to the sendSegmentsToPartitions function   
     * @throws InterruptedException 
     * @throws ParseException 
     */
	public void runKafkaProducer() throws IOException, InterruptedException, ParseException {
        // log4j writes to stdout for now
        org.apache.log4j.BasicConfigurator.configure();
        
		ServiceUtil serviceUtil = ServiceUtil.getInstance();
		serviceUtil.setAppName("SeismicEventsData");
		WaveformService waveformService = serviceUtil.getWaveformService();
        // Pull 10 hour blocks at a time
        Long start = new Long("1108627200000"), increment = new Long("36000000");
		
        
        for (;;) {
			Long end = start + increment;
			System.out.println("Sending data from " + ProducerKafka.getTimeString(start) + 
					" to " + ProducerKafka.getTimeString(end));
			
			List<Timeseries> timeSeriesCollection = this.getIrisMessage(waveformService, start, end);
			List<ExecutorService> producers = new ArrayList<ExecutorService>();
			for (int k = 0; k < timeSeriesCollection.size(); k++) {
				Timeseries timeseries = timeSeriesCollection.get(k);
				// Let's not worry about objects that have more than 1 segment for now...
				if (timeseries.getSegments().size() > 1) {
					continue;
				}
			
				String topic = stationList[k];
				// Split the message into several chunks so the whole chunk of data will go to different partitions
				this.sendSegmentsToPartitions(timeseries, topic, producers, this.producer.partitionsFor(topic).size());
			}

			// We are waiting for all of the runnables to finish sending messages and then we can close the producer
			for (ExecutorService producerRunnables : producers) {
				producerRunnables.shutdown();
				producerRunnables.awaitTermination(365, TimeUnit.DAYS);
			}
			
			start = end;
        }
    }
    private static String getTimeString(long time) {
        GregorianCalendar d = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
        d.clear();
        d.setTimeInMillis(time);
        int year = d.get(Calendar.YEAR);
        int doy = d.get(Calendar.DAY_OF_YEAR);
        int hour = d.get(Calendar.HOUR_OF_DAY);
        int min = d.get(Calendar.MINUTE);
        int sec = d.get(Calendar.SECOND);
        int msec = d.get(Calendar.MILLISECOND);
        return String.format("%04d-%03d %02d:%02d:%02d.%03d", year, doy, hour, min, sec, msec);
    }

	/**
	 * This function takes a timeseries object, goes through all of the segments in a single timeseries, and 
	 * sends them all to consumers. Before sending the data it will first split a single object in the collection
	 * of segments into multiple segments in case we have multiple partitions per topic. 
	 * There can be multiple segments per timeseries object. A consumer will receive only one segment at a time 
	 * and process one segment at a time only.
	 * @param timeseries
	 * @param numPartitions
	 * @throws InterruptedException 
	 */
	private void sendSegmentsToPartitions(Timeseries timeseries, String topic,
			List<ExecutorService> producers, int numPartitions) throws InterruptedException {
		
		// The problem is that we can't put multiple segments into one. Every chunk has a time frame associated with it. 
		// I will have to send data in chunks...maybe. Consumer is configured to handle this, so maybe I will just put it
		// the way it is, which is multiple Segments. However, we will have to change the way we are putting it into cache. 
		
		// We are going to create a runnable per partition per segment
		final ExecutorService producerRunnables = Executors.newFixedThreadPool(numPartitions * 
				timeseries.getSegments().size());
		
		for (Segment segment : timeseries.getSegments()) {
			// ================ GET PRELIMINARY INFO ===================== ||
			long startTime = segment.getStartTime().getTime() / 1000, 
					endTime = segment.getEndTime().getTime() / 1000;
			
			// TODO: What if there are multiple types of data in multiple lists...
			float[] data = this.discoverMeasurementData(segment);
			// =========================================================== ||
			
			// Create a new Runnable for every band
			for (SeismicBand _band : bands) {
				// Create a new id for the block depending on the band size
				StreamIdentifier id = new StreamIdentifier(timeseries.getNetworkCode(), timeseries.getStationCode(), 
						timeseries.getChannelCode(), timeseries.getLocation(), _band);
				
				// Populate the streamer so we can discard the raw data block
				IStreamProducer streamer = new AnotherStreamProducer(id, data, startTime, endTime, 
						minutesPerBlock, segment.getSamplerate());
				// Create a runnable task with a station as a topic
				ProducerRunnable task = new ProducerRunnable(streamer, producer, topic);
				// Submit the task
				producerRunnables.submit(task);
			}
		}
		producers.add(producerRunnables);
	}
	
	
	/**
	 * This function discovers what type of data a segment holds and returns it as a list of floats
	 * @param single_segment
	 * @return data associated with that segment
	 */
	private float[] discoverMeasurementData(Segment single_segment) {
		List<? extends Number> data = null;

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
		// Now convert whatever data we have to float
		float[] mainData = Floats.toArray(data);
		
		return mainData;
	}

	/**
	 * Gets the data streams from IRIS, based on fields set up in the constructor.
	 * @return Returns a list of timeseries objects with the data.
	 * @throws IOException
	 * @throws ParseException 
	 */
	public List<Timeseries> getIrisMessage(WaveformService waveformService, Long start, Long end) throws IOException, ParseException {
		//DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		//TODO: We will want to remove the hardcoding of the time zone and time period
		//dateformat.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		Date startDate = new Date(start);//dateformat.parse("2005-02-17T08:00:00");
		Date endDate = new Date(end);//dateformat.parse("2005-02-17T18:00:00");
//		this timestamp has 2 segments..
//		long startTime = Long.parseLong("1113999060000"), endTime = Long.parseLong("1114099060000"); 
		
		WaveformCriteria criteria = new WaveformCriteria();

		for (String info : stationList) {
			String[] s_info = info.split("-");

			String network = s_info[0];
			String station = s_info[1];
			String channel = s_info[2];
			String loc_id = s_info[3];
			
			criteria.add(network, station, loc_id, channel, startDate, endDate);
		}
		criteria.makeDistinctRequests(true);
		
		List<Timeseries> timeSeriesCollection = null;
		try {
			timeSeriesCollection = waveformService.fetch(criteria);
		} catch (Exception ex) {
            Logger.getLogger(ProducerKafka.class.getName()).log(Level.SEVERE, null, ex);
		}
		return timeSeriesCollection;
	}
}
