
package edu.byu.seismicproject.signalprocessing;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;


public class DetectionStatisticWriter {
    Map<Integer, PrintWriter> detectorPrinterMap;
    public DetectionStatisticWriter( DetectorHolder detectors) throws FileNotFoundException{
        detectorPrinterMap = openStatisticsFiles(detectors);
    }

    private Map<Integer, PrintWriter> openStatisticsFiles(DetectorHolder detectors) throws FileNotFoundException {
        Map<Integer, PrintWriter> map = new HashMap<>();
        for (CorrelationDetector detector : detectors.getDetectors()) {
            String name = String.format("det%04d", detector.getDetectorid());
            PrintWriter pw = new PrintWriter(name);
            map.put(detector.getDetectorid(), pw);
        }
        return map;
    }

    public void writeStatistic( CorrelationDetector detector, DetectionStatistic statistic, double streamStart) {
        PrintWriter pw = detectorPrinterMap.get(detector.getDetectorid());
        float[] stat = statistic.getStatistic();
        double sdt = statistic.getSampleInterval();
        double stime = statistic.getStartTime();
        for( int j = 0; j < stat.length; ++j){
            double sampleTime = stime + j * sdt - streamStart;
            pw.println(sampleTime + "  " + stat[j]);
        }
    }
    
    public void close()
    {
         for (PrintWriter pw : detectorPrinterMap.values()) {
                pw.close();
            }
    }
    
    
}
