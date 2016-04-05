package main.java.edu.byu.seismicproject.signalprocessing.processing;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import main.java.edu.byu.seismicproject.signalprocessing.CorrelationDetector;
import main.java.edu.byu.seismicproject.signalprocessing.CorrelationDetectorSpecification;
import main.java.edu.byu.seismicproject.signalprocessing.DetectorHolder;
import main.java.edu.byu.seismicproject.signalprocessing.DetectorInfo;
import main.java.edu.byu.seismicproject.signalprocessing.IDetectorSpecification;
import main.java.edu.byu.seismicproject.signalprocessing.StreamSegment;

@SuppressWarnings("serial")
public class DynamicCorrelationProcessor implements Serializable {

	public DynamicCorrelationProcessor() {}
	
	private int BLOCK_SIZE;
	private final float secPerLTA = 60;
	private final float secPerSTA = 3;
	private final int triggerThreshold = 10;
	private final int detriggerThreshold = 2;
	
	public CorrelationDetector createNewCorrelationDetector(StreamSegment combined) {
		float[] data = combined.getData();
		double dt = combined.getSampleInterval();
		BLOCK_SIZE = data.length / 2;
		int offset = BLOCK_SIZE / 2;
		Entry<Integer, Integer> detectorSpec = this.computeSTAtoLTAratio(data, dt, offset);
		if (detectorSpec != null) {
			int npts = (int) Math.round(detectorSpec.getValue() / dt);
			
			float threshold = 0.6f;
			float blackout = 3;
			IDetectorSpecification spec = new CorrelationDetectorSpecification(threshold, blackout);
			float[] templateData = new float[npts];
			int offsetToSegment = detectorSpec.getKey();
			System.arraycopy(data, offsetToSegment, templateData, 0, npts);
			
			double detectorDelayInSeconds = detectorSpec.getValue();
			int detectorid = ++DetectorHolder.detectoridSequence;
			DetectorInfo di = new DetectorInfo(detectorid, spec, detectorDelayInSeconds + combined.getStartTime());
			return new CorrelationDetector(di, combined.getId(), templateData, BLOCK_SIZE);
		}
		
		return null;
	}
	
	//http://www.crewes.org/ForOurSponsors/ConferenceAbstracts/2009/CSEG/Wong_CSEG_2009.pdf
	@SuppressWarnings("unchecked")
	private Entry<Integer, Integer> computeSTAtoLTAratio(float[] data, double dt, int offset) {
		float[] triggers = new float[BLOCK_SIZE];
		boolean thresholdExceeded = false;
		Map<Integer, Integer> windows = new TreeMap<Integer, Integer>();
		int originalOffset = offset;
		
		int windowStart = 0;
		
		int NS = (int) (secPerSTA * (1 / dt)); // number of points in a short-term window
		int NL = (int) (secPerLTA * (1 / dt)); // number of points in a long-term window
		offset -= NS + NL;
		
		for (int i = 0; i < BLOCK_SIZE - NS; i++) {
			float STA = 0, LTA = 0;
			
			for (int k = 0; k < NS; k++) {
				int iNS = offset + NL + k + i; // short term window should be in front of the long term window...
				STA += Math.pow(data[iNS], 2);
			}
			for (int k = 0; k < NL; k++) {
				int iNL = offset + k + i;
				LTA += Math.pow(data[iNL], 2);
			}
			
			STA *= 1f / NS;
			LTA *= 1f / NL;
			triggers[i] = STA / LTA;
			if (triggers[i] >= triggerThreshold && !thresholdExceeded) {
				System.out.println("Trigger threshold was exceeded at position: " + (i + originalOffset));
				windowStart = i + originalOffset;
				thresholdExceeded = true;
			}
			if (thresholdExceeded && triggers[i] <= detriggerThreshold) {
				System.out.println("Detrigger threshold was exceeded at position: " + (i + originalOffset));
				int windowEnd = i + originalOffset;
				thresholdExceeded = false;
				
				int duration = (int) ((windowEnd - windowStart) * dt);
				windows.put(windowStart, duration);
			}
		}
		
		List windList = new LinkedList<>(windows.entrySet());
		Collections.sort(windList, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});
		
		if (windList.size() != 0)
			return (Entry<Integer, Integer>) windList.get(windList.size() - 1);
		else
			return null;
	}
}
