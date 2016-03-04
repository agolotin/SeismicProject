package main.java.edu.byu.seismicproject.signalprocessing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DetectorHolder {
	
	List<CorrelationDetector> detectors;
	
	public DetectorHolder() {
		this.detectors = new ArrayList<CorrelationDetector>();
	}
	
	public DetectorHolder(CorrelationDetector newDetector) {
		this.detectors = new ArrayList<CorrelationDetector>();
		this.detectors.add(newDetector);
	}
	
	public DetectorHolder(Collection<CorrelationDetector> newDetectors) {
		this.detectors = new ArrayList<CorrelationDetector>();
		this.detectors.addAll(newDetectors);
	}
	
	public DetectorHolder(Object[] detectors) {
		this.detectors = new ArrayList<CorrelationDetector>();
		for (Object detector : detectors) {
			this.detectors.addAll(((DetectorHolder) detector).getDetectors());
		}
	}

	public List<CorrelationDetector> getDetectors() {
		return this.detectors;
	}

	public void add(List<CorrelationDetector> other_detectors) {
		this.detectors.addAll(other_detectors);
	}

}
