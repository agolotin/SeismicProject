package main.java.edu.byu.seismicproject.signalprocessing.processing;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

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
        
        detectorCache = igniteInstance.getOrCreateCache(IgniteCacheConfig.detectorHolderCache());
    }

    public void analyzeSegments(StreamSegment currentSegment, StreamSegment previousSegment) {
        printBlockStartTime(currentSegment);
        DetectorHolder detectors = detectorCache.get(String.valueOf(currentSegment.getId().hashCode()));
        
        if (detectors != null) {
			StreamSegment combined = StreamSegment.combine(currentSegment, previousSegment);
			for (CorrelationDetector detector : detectors.getDetectors()) {
				if (detector.isCompatibleWith(combined)) {
					DetectionStatistic statistic = detector.produceStatistic(combined);
					statisticScanner.addStatistic(statistic);
				}
			}
			
			//TODO: scanForTriggers should be modified to return a map with detector ID -> collection
			Collection<TriggerData> triggers = statisticScanner.scanForTriggers();
			if (!triggers.isEmpty()) {
				processAllTriggers(triggers);
			}
        }
        // create new detectors otherwise... I guess...
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
     * 
     * 
     * @param triggers
     */
    private void processAllTriggers(Collection<TriggerData> triggers) {
    	//TODO: need to scan triggers and identify coincident triggers (within 10 sec) 

        triggers.stream().forEach((td) -> {
            System.out.println(td);
        });
    }

}