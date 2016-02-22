package main.java.signalprocessing;

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

}
