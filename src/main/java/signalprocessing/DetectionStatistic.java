package main.java.signalprocessing;


public class DetectionStatistic {
    private final double startTime;
    private final double sampleInterval;
    private final float[] statistic;
    private final StreamIdentifier streamId;
    private final int detectorid;

    public DetectionStatistic(StreamIdentifier id, int detectorid, double startTime, double sampleInterval, float[] statistic) {
        this.streamId = id;
        this.detectorid = detectorid;
        this.startTime = startTime;
        this.sampleInterval = sampleInterval;
        this.statistic = statistic;
    }

	public double getStartTime() {
		return startTime;
	}

	public double getSampleInterval() {
		return sampleInterval;
	}

	public float[] getStatistic() {
		return statistic;
	}

	public StreamIdentifier getStreamId() {
		return streamId;
	}

	public int getDetectorid() {
		return detectorid;
	}
    
}