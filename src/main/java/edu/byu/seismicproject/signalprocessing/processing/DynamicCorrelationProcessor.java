package main.java.edu.byu.seismicproject.signalprocessing.processing;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
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
	
	private final float secPerLTA = 30;
	private final float secPerSTA = 3;
	
	private final int triggerThreshold = 15;
	private final int detriggerThreshold = 5;
	private final double SNRthreshold = 0;
	
	public Collection<CorrelationDetector> createNewCorrelationDetector(StreamSegment combined) {
		float[] data = combined.getData();
		double dt = combined.getSampleInterval();
		BLOCK_SIZE = data.length / 2;
		int offset = BLOCK_SIZE / 2;
		Map<Integer, Integer> detectorSpecs = this.computeSTAtoLTAratio(data, dt, offset);
		Collection<CorrelationDetector> detectors = new LinkedList<CorrelationDetector>();
		
		if (detectorSpecs != null) {
			for (Entry<Integer, Integer> detectorSpec : detectorSpecs.entrySet()) {
				// if power detection is longer than 3 seconds, then we create a new correlation detector
				if (detectorSpec.getValue() >= 3) { 
					int npts = (int) Math.round(detectorSpec.getValue() / dt);
					
					float threshold = 0.6f;
					float blackout = 3;
					IDetectorSpecification spec = new CorrelationDetectorSpecification(threshold, blackout);
					float[] templateData = new float[npts];
					int offsetToSegment = detectorSpec.getKey();
					System.arraycopy(data, offsetToSegment, templateData, 0, npts);
					
					if (!analyzeSignalToNoise(templateData)) {
						continue;
					}
					
					double detectorDelayInSeconds = detectorSpec.getValue();
					int detectorid = ++DetectorHolder.detectoridSequence;
					DetectorInfo di = new DetectorInfo(detectorid, spec, detectorDelayInSeconds + combined.getStartTime());
					detectors.add(new CorrelationDetector(di, combined.getId(), templateData, BLOCK_SIZE));
				}
			}
		}
		
		return detectors.size() != 0 ? detectors : null;
	}
	
	private boolean analyzeSignalToNoise(float[] templateData) {
		double signalPower = 0;
		double noisePower = 0;
		double npts = templateData.length;
		
		for (int i = 0; i < npts; i++) {
			signalPower += templateData[i];
		}
		for (int i = 0; i < npts; i++) {
			noisePower += Math.pow(templateData[i] - signalPower, 2);
		}
		
		signalPower /= npts;
		noisePower = Math.sqrt(noisePower / (npts - 1.0));
		double SNR = signalPower / noisePower;
		
		return SNR > SNRthreshold ? true : false;
	}

	//http://www.crewes.org/ForOurSponsors/ConferenceAbstracts/2009/CSEG/Wong_CSEG_2009.pdf
	private Map<Integer, Integer> computeSTAtoLTAratio(float[] data, double dt, int offset) {
		float[] triggers = new float[BLOCK_SIZE];
		boolean thresholdExceeded = false;
		Map<Integer, Integer> windows = new TreeMap<Integer, Integer>();
		//int originalOffset = offset;
		
		int windowStart = 0;
		
		int NS = (int) (secPerSTA * (1 / dt)); // number of points in a short-term window
		int NL = (int) (secPerLTA * (1 / dt)); // number of points in a long-term window
		//offset -= NS + NL;
		
		for (int i = 0; i < BLOCK_SIZE; i++) {
			float STA = 0, LTA = 0;
			
			for (int k = 0; k < NL; k++) {
				// Calculate long term average for the window
				int iNL = offset - NL + k + i; // long term window will be before the short term window...
				LTA += Math.pow(data[iNL], 2);
				
				// Calculate short term average for the window
				if (k < NS) {
					int iNS = offset + k + i; // short term window should be in front of the long term window...
					STA += Math.pow(data[iNS], 2);
				}
			}
			
			STA /= NS;
			LTA /= NL;
			triggers[i] = STA / LTA;
			if (triggers[i] >= triggerThreshold && !thresholdExceeded) {
				//System.out.println("Trigger threshold was exceeded at position: " + (i + offset));
				windowStart = i + offset;
				thresholdExceeded = true;
			}
			if (thresholdExceeded && triggers[i] <= detriggerThreshold) {
				//System.out.println("Detrigger threshold was exceeded at position: " + (i + offset));
				int windowEnd = i + offset;
				thresholdExceeded = false;
				
				int duration = (int) ((windowEnd - windowStart) * dt);
				windows.put(windowStart, duration);
			}
		}
		
		return windows;
	}
}
