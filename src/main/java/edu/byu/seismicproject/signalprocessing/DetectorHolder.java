
package main.java.edu.byu.seismicproject.signalprocessing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import main.java.edu.byu.seismicproject.general.band.SeismicBand;


public class DetectorHolder {

    private final Collection<CorrelationDetector> detectors;
    public static int detectoridSequence = 0;
    
    public DetectorHolder(CorrelationDetector firstDetector) {
        detectors = new LinkedBlockingQueue<>();
        this.addNewDetector(firstDetector);
    }
    
    public DetectorHolder(Collection<CorrelationDetector> allDetectors) {
        detectors = new LinkedBlockingQueue<>();
        detectors.addAll(allDetectors);
    }

    public DetectorHolder() throws Exception {
        float threshold = 0.6f;
        float blackout = 3;
        IDetectorSpecification spec = new CorrelationDetectorSpecification(threshold, blackout);
        //TODO: Use the correct StreamIdentifier for the application
//        StreamIdentifier id = new StreamIdentifier("IU", "KBS", "BHZ", "00", new SeismicBand(2, 4, 2, 8)); // this line is for our framework
        StreamIdentifier id = new StreamIdentifier("IU", "KBS", "BHZ", "00", new SeismicBand(1, 4, 2, 8)); // this line is for SimpleFramework
        detectors = new LinkedBlockingQueue<>();
        // These data are already filtered into the stream pass band of 2 - 8 Hz.
        
        try (InputStream in = getClass().getResourceAsStream("/main/resources/detector/template1.txt")) {
            detectors.add(makeDetector(in, 5, 15, spec, id));
        }

        try (InputStream in = getClass().getResourceAsStream("/main/resources/detector/template2.txt")) {
            detectors.add(makeDetector(in, 5, 30, spec, id));
        }
        try (InputStream in = getClass().getResourceAsStream("/main/resources/detector/template3.txt")) {
            detectors.add(makeDetector(in, 7, 30, spec, id));
        }
    }

    private CorrelationDetector makeDetector(InputStream is, double startSecond, double duration, IDetectorSpecification spec, StreamIdentifier id) throws IOException {
        double dt = 0.05; // This is just an example which is correct for KBS BHZ
        ArrayList<String> lines = new ArrayList<>();
        try (InputStreamReader isr = new InputStreamReader(is)) {
            try (BufferedReader br = new BufferedReader(isr)) {
                String line = br.readLine();
                while (line != null) {
                    lines.add(line);
                    line = br.readLine();
                }
            }
        }
        
        float[] tmpArray = new float[lines.size()];
        for (int j = 0; j < lines.size(); ++j) {
            tmpArray[j] = Float.parseFloat(lines.get(j));
        }

        int offset = (int) Math.round(startSecond / dt);
        int npts = (int) Math.round(duration / dt);

        int blockSizeSamps = 72000;
        float[] templateData = new float[npts];
        System.arraycopy(tmpArray, offset, templateData, 0, npts);
        double detectorDelayInSeconds = duration;
        int detectorid = ++detectoridSequence;
        DetectorInfo di = new DetectorInfo(detectorid, spec, detectorDelayInSeconds);
        return new CorrelationDetector(di, id, templateData, blockSizeSamps);
    }

    public Collection<CorrelationDetector> getDetectors() {
        return new ArrayList<>(detectors);
    }
    
    public void addNewDetector(CorrelationDetector newDetector) {
    	this.detectors.add(newDetector);
    }

}
