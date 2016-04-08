package main.java.edu.byu.seismicproject.signalprocessing.processing;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import main.java.edu.byu.seismicproject.general.band.SeismicBand;
import main.java.edu.byu.seismicproject.ignite.server.IgniteCacheConfig;
import main.java.edu.byu.seismicproject.signalprocessing.CorrelationDetector;
import main.java.edu.byu.seismicproject.signalprocessing.DetectionStatistic;
import main.java.edu.byu.seismicproject.signalprocessing.DetectionStatisticScanner;
import main.java.edu.byu.seismicproject.signalprocessing.DetectorHolder;
import main.java.edu.byu.seismicproject.signalprocessing.StreamIdentifier;
import main.java.edu.byu.seismicproject.signalprocessing.StreamSegment;
import main.java.edu.byu.seismicproject.signalprocessing.TriggerData;


@SuppressWarnings("serial")
public class SeismicStreamProcessor implements Serializable {

    private final DetectionStatisticScanner statisticScanner;
    private final IgniteCache<String, DetectorHolder> detectorCache;
    private DynamicCorrelationProcessor correlationProcessor;
    
    private static Set<String> cacheIds = new LinkedHashSet<String>();
    
    public static void BootstrapDetectors() {
        try {
        	Ignite ignite = Ignition.start();
        	IgniteCache<String, DetectorHolder> _detectorCache = ignite.getOrCreateCache(IgniteCacheConfig.detectorHolderCache());
			DetectorHolder bootstrapDetectors = new DetectorHolder();
			StreamIdentifier id = new StreamIdentifier("IU", "KBS", "BHZ", "00", new SeismicBand(2, 4, 2, 8));
			_detectorCache.put(String.valueOf(id.hashCode()), bootstrapDetectors);
			
			ignite.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    public SeismicStreamProcessor(Ignite igniteInstance) throws Exception {
        boolean triggerOnlyOnCorrelators = true;
        statisticScanner = new DetectionStatisticScanner(triggerOnlyOnCorrelators);
        correlationProcessor = new DynamicCorrelationProcessor();
        
        detectorCache = igniteInstance.getOrCreateCache(IgniteCacheConfig.detectorHolderCache());
    }

    public void analyzeSegments(StreamSegment currentSegment, StreamSegment previousSegment) {
        printBlockStartTime(currentSegment);
        DetectorHolder detectors = detectorCache.get(String.valueOf(currentSegment.getId().hashCode()));
        cacheIds.add(String.valueOf(currentSegment.getId().hashCode()));
        
        if (detectors != null) {
			StreamSegment combined = StreamSegment.combine(currentSegment, previousSegment);
			for (CorrelationDetector detector : detectors.getDetectors()) {
				if (detector.isCompatibleWith(combined)) {
					DetectionStatistic statistic = detector.produceStatistic(combined);
					statisticScanner.addStatistic(statistic);
				}
			}
			
			Map<Integer, Collection<TriggerData>> triggers = statisticScanner.scanForTriggers();
			if (!triggers.isEmpty()) {
				for (Map.Entry<Integer, Collection<TriggerData>> entry : triggers.entrySet()) {
					processAllTriggers(entry.getKey(), entry.getValue());
				}
			}
			else {
				Collection<CorrelationDetector> newDetector = correlationProcessor.createNewCorrelationDetector(combined);
				if (newDetector != null) {
					detectors.addNewDetectors(newDetector);
				}
				
				String key = String.valueOf(combined.getId().hashCode());
				detectorCache.put(key, detectors);
			}
        }
        else {
        	// create absolutely new detector set otherwise... 
			StreamSegment combined = StreamSegment.combine(currentSegment, previousSegment);
        	Collection<CorrelationDetector> newDetector = correlationProcessor.createNewCorrelationDetector(combined);
			if (newDetector != null) {
				DetectorHolder newDetectorSet = new DetectorHolder(newDetector);
				
				String key = String.valueOf(combined.getId().hashCode());
				detectorCache.put(key, newDetectorSet);
			}
        }
        printDetectorCacheSize(detectorCache);
        
    }
    
    private void printDetectorCacheSize(IgniteCache<String, DetectorHolder> cache) {
    	int size = 0;
    	for (DetectorHolder detectors : cache.getAll(cacheIds).values()) {
    		size += detectors.getDetectors().size();
    	}
        System.out.printf("!!!!!!!!SIZE OF THE DETECTOR CACHE = %d!!!!!!!!!!!!!!!\n", size);
        if (size % 100 == 0) {
        	try {
				writeDetectorsToDisk(cache);
			} catch (Exception e) {
				Logger.getLogger(SeismicStreamProcessor.class.getName()).log(Level.SEVERE, null, e);
			}
        }
    }

    private void writeDetectorsToDisk(IgniteCache<String, DetectorHolder> cache) throws IOException {
		Kryo kryo = new Kryo();
		kryo.register(CorrelationDetector.class, new JavaSerializer());
		
    	for (DetectorHolder detectors : cache.getAll(cacheIds).values()) {
    		for (CorrelationDetector detector : detectors.getDetectors()) {
				String name = String.format("det%06d", detector.getDetectorid());
				FileOutputStream fos = new FileOutputStream(name);

				Output output = new Output(fos);
				kryo.writeObject(output, detector);
				output.close();
    		}
    	}
	}

	private void printBlockStartTime(StreamSegment segment) {
        double time = segment.getStartTime();
        String ts = this.getTimeString(time);
        System.out.println("Processing block starting:    " + ts);
    }

    private String getTimeString(double time) {
        GregorianCalendar d = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
        d.clear();
        d.setTimeInMillis((long) (time * 1000));
        int year = d.get(Calendar.YEAR);
        int doy = d.get(Calendar.DAY_OF_YEAR);
        int hour = d.get(Calendar.HOUR_OF_DAY);
        int min = d.get(Calendar.MINUTE);
        int sec = d.get(Calendar.SECOND);
        int msec = d.get(Calendar.MILLISECOND);
        return String.format("%04d-%03d %02d:%02d:%02d.%03d", year, doy, hour, min, sec, msec);
    }
    
	/**
     * Create groups of coincident triggers, find the highest stat value in each group,
     * prune all lower values, promote highest trigger from each group to a detection.
     * Output detections to a file
	 * @param detectorID 
     * 
     * @param triggers
     */
    private void processAllTriggers(Integer detectorID, Collection<TriggerData> triggers) {
    	//TODO: need to scan triggers and identify coincident triggers (within 10 sec)
    	//1. group triggers into coincident groups
    		//loop through list of triggers
    		//for each trigger, if the next trigger is within 10 seconds, add it to the collection
    		//
    	//2. scan groups to identify highest stat value
    	//3. use that to generate new detection
    	//4. compare to detector of given ID
    	
    	//This list will hold lists, each representing a group of triggers
    	ArrayList<ArrayList<TriggerData>> triggerGroups = new ArrayList<ArrayList<TriggerData>>();
    	triggerGroups.add(new ArrayList<TriggerData>());
    	
    	TriggerData prevTrigger = null;
    	for (TriggerData currTrigger : triggers) {
    		if (prevTrigger == null) {
    			prevTrigger = currTrigger;
    		}
    		else if ((currTrigger.getTriggerTime() - 10) <= prevTrigger.getTriggerTime()) {    			
        		//If the current trigger time minus 10 seconds is greater than 
        		//the previous trigger time, a new group should be added 
    			triggerGroups.add(new ArrayList<TriggerData>());
    		}
    		triggerGroups.get(triggerGroups.size()-1).add(currTrigger);
    	}
    	//This will store the highest trigger by stat value and the start and end times of the group it came from
    	HashMap<TriggerData, ArrayList<Double>> groupSummaries = new HashMap<TriggerData, ArrayList<Double>>(); 
    	
    	for (ArrayList<TriggerData> group : triggerGroups) {
    		float maxStat = -1;
    		TriggerData maxTrigger = null;
    		for (TriggerData trigger : group) {
    			if (trigger.getStatistic() > maxStat) {
    				maxTrigger = trigger;
    				maxStat = trigger.getStatistic();
    			}
    		}
    		ArrayList<Double> timeRange = new ArrayList<Double>();
    		timeRange.add(group.get(0).getTriggerTime());
    		timeRange.add(group.get(group.size() -1).getTriggerTime());
    		groupSummaries.put(maxTrigger, timeRange);
    	}
        
        for (TriggerData trigger : groupSummaries.keySet()) {
        	System.out.println(trigger);
        	System.out.println("Group start: " + groupSummaries.get(trigger).get(0) + 
        			" Group end: " + groupSummaries.get(trigger).get(1));
        }
    }
    
    @SuppressWarnings("unused")
	private Collection<TriggerData> orderTriggers(Collection<TriggerData> triggers) {
    	return null;
    }

}