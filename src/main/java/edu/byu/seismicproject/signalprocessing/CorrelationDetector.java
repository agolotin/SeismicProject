package main.java.edu.byu.seismicproject.signalprocessing;

public class CorrelationDetector {

    private final double templateAutoCorrelation;
    private final float[] templateData;
    private final StreamIdentifier streamId;
    private final DetectorInfo detectorInfo;

    public final int BLOCK_SIZE;


    public CorrelationDetector(DetectorInfo di, StreamIdentifier id, float[] templateData, int blockSize) {
        this.templateData = templateData.clone();
        double tmp = 0;
        for (float v : templateData) {
            tmp += v * v;
        }
        templateAutoCorrelation = tmp;
        this.streamId = id;
        this.detectorInfo = di;
        this.BLOCK_SIZE = blockSize;
    }

    public DetectionStatistic produceStatistic(StreamSegment segment) {
        float[] data = segment.getData();
        float[] statistic = produceStatistic(data);
        int offset = BLOCK_SIZE / 2;
        double dt = segment.getSampleInterval();
        double newStart = segment.getStartTime() + dt * offset;
        return new DetectionStatistic(segment.getId(), newStart, dt, statistic, detectorInfo);
    }

    private float[] produceStatistic(float[] data) {

        float[] result = new float[BLOCK_SIZE];
        int offset = BLOCK_SIZE / 2;
        for (int j = 0; j < BLOCK_SIZE; ++j) {
            double dataAutoCorrelation = 0;
            double crossCorrelation = 0;
            for (int k = 0; k < templateData.length; ++k) {
                int m = j + k + offset;
                dataAutoCorrelation += data[m] * data[m];
                crossCorrelation += data[m] * templateData[k];
            }
            double denom = Math.sqrt(dataAutoCorrelation * templateAutoCorrelation);
            double cc = denom == 0 ? 0 : crossCorrelation / Math.sqrt(dataAutoCorrelation * templateAutoCorrelation);
            result[j] = (float) (cc * cc);

        }
        return result;
    }

    public int getDetectorid() {
        return detectorInfo.getDetectorid();
    }

    public boolean isCompatibleWith(StreamSegment combined) {
        return combined.getId().equals(streamId);
    }

    public StreamIdentifier getStreamId() {
        return streamId;
    }

}
