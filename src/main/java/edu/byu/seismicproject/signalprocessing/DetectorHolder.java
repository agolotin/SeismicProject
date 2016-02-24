package main.java.edu.byu.seismicproject.signalprocessing;

import java.util.ArrayList;
import java.util.List;

public class DetectorHolder {
	
	List<CorrelationDetector> detectors;
	
	public DetectorHolder() {
		this.detectors = new ArrayList<CorrelationDetector>();
	}

	public List<CorrelationDetector> getDetectors() {
		return this.detectors;
	}

	public void add(List<CorrelationDetector> other_detectors) {
		this.detectors.addAll(other_detectors);
	}

}
