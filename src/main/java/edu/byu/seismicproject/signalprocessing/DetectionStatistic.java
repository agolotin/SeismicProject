package main.java.edu.byu.seismicproject.signalprocessing;

import java.util.Map;
import java.util.TreeMap;

public class DetectionStatistic {

    private final double startTime;
    private final double sampleInterval;
    private final float[] statistic;
    private final StreamIdentifier streamId;
    private final DetectorInfo detectorInfo;

    public DetectionStatistic(StreamIdentifier id,
            double startTime,
            double sampleInterval,
            float[] statistic,
            DetectorInfo detectorInfo) {
        this.streamId = id;
        this.startTime = startTime;
        this.sampleInterval = sampleInterval;
        this.statistic = statistic;
        this.detectorInfo = detectorInfo;
    }

    public int size() {
        return statistic.length;
    }

    public double getStartTime() {
        return startTime;
    }

    public double getSampleInterval() {
        return sampleInterval;
    }

    public float[] getStatistic() {
        return statistic.clone();
    }

    public StreamIdentifier getStreamId() {
        return streamId;
    }

    public int getDetectorid() {
        return detectorInfo.getDetectorid();
    }

    public double getSampleRate() {
        return 1.0 / sampleInterval;
    }

    public static DetectionStatistic combine(DetectionStatistic[] statistics) {
        Map<Double, DetectionStatistic> timeMap = new TreeMap<>();
        int totalLength = 0;
        for (DetectionStatistic ds : statistics) {
            timeMap.put(ds.startTime, ds);
            totalLength += ds.size();
        }

        DetectionStatistic first = null;
        for (Double v : timeMap.keySet()) {
            DetectionStatistic ds = timeMap.get(v);
            if (first == null) {
                first = ds;
                break;
            }
        }
        float[] array = new float[totalLength];
        int offset = 0;
        for (Double v : timeMap.keySet()) {
            DetectionStatistic ds = timeMap.get(v);
            System.arraycopy(ds.statistic, 0, array, offset, ds.size());
            offset += ds.size();
        }
        return new DetectionStatistic(first.getStreamId(), 
                first.startTime,
                first.sampleInterval, array, first.getDetectorInfo());
    }

    /**
     * @return the detectorInfo
     */
    public DetectorInfo getDetectorInfo() {
        return detectorInfo;
    }

}
