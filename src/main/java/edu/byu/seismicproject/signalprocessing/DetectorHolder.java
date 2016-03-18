
package main.java.edu.byu.seismicproject.signalprocessing;

import main.java.edu.byu.seismicproject.general.band.SeismicBand;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;


public class DetectorHolder {

    private final Collection<CorrelationDetector> detectors;
    private static int detectoridSequence = 0;

    public DetectorHolder() throws Exception {
        float threshold = 0.6f;
        float blackout = 3;
        DetectorSpecification spec = new CorrelationDetectorSpecification(threshold, blackout);
        StreamIdentifier id = new StreamIdentifier("IU", "KBS", "BHZ", "00", new SeismicBand(1, 4, 2, 8));
        detectors = new ArrayList<>();
        // These data are already filtered into the stream pass band of 2 - 8 Hz.
        try (InputStream in = getClass().getResourceAsStream("/detector/template1.txt")) {
            detectors.add(makeDetector(in, 5, 15, spec, id));
        }

        try (InputStream in = getClass().getResourceAsStream("/detector/template2.txt")) {
            detectors.add(makeDetector(in, 5, 30, spec, id));
        }
        try (InputStream in = getClass().getResourceAsStream("/detector/template3.txt")) {
            detectors.add(makeDetector(in, 7, 30, spec, id));
        }
    }

    private CorrelationDetector makeDetector(InputStream is, double startSecond, double duration, DetectorSpecification spec, StreamIdentifier id) throws IOException {
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

        float[] templateData = new float[npts];
        System.arraycopy(tmpArray, offset, templateData, 0, npts);
        double detectorDelayInSeconds = duration;
        int detectorid = ++detectoridSequence;
        DetectorInfo di = new DetectorInfo(detectorid, spec, detectorDelayInSeconds);
        return new CorrelationDetector(di, id, templateData);
    }

    public Collection<CorrelationDetector> getDetectors() {
        return new ArrayList<>(detectors);
    }

}
