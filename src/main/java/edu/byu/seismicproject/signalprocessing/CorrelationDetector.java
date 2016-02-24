package main.java.edu.byu.seismicproject.signalprocessing;

public class CorrelationDetector {

	// FIXME: What do I do with block size...
	public static final int BLOCK_SIZE = 72000;
    private final double templateAutoCorrelation;

	private final float[] templateData;
    private final StreamIdentifier streamId;
    private final int detectorid;
    
    
//    public CorrelationDetector(SACFile sacTemplate, StreamIdentifier id, double startSecond, double duration) {
    public CorrelationDetector(StreamIdentifier id, double startSecond, double duration) {

    	// REVIEWME: I'm not sure what in this case delta should represent... is it sample interval as well?
        double dt = Double.MAX_VALUE;//sacTemplate.getHeader().delta;
        int offset = (int) Math.round(startSecond / dt);
        int npts = (int) Math.round(duration / dt);
        
        // REVIEWME: What does tmpArray suppose to hold? Is it the data block that we are computing our detection statistic against or is it data within a detector? 
        float[] tmpArray = new float[Float.MAX_EXPONENT];//sacTemplate.getData();
        templateData = new float[npts];
        
        //Compute autocorrelation…
		System.arraycopy(tmpArray, offset, templateData, 0, npts);
        double tmp = 0;
        for (float v : templateData) {
            tmp += v * v;
        }
        templateAutoCorrelation = tmp;

        this.streamId = id;
        this.detectorid = Integer.MAX_VALUE; 
        // REVIEWME: The next line... What is detectoridSequence? I couldn't find it anywhere
        // detectorid = ++ detectoridSequence; 
    }
    
    
    // This actually calculated the detection statistic
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
            result[j] = (float) (cc*cc);

        }
        return result;
    }

    public DetectionStatistic produceStatistic(StreamSegment segment ){
        float[] data = segment.getData();
        float[] statistic = produceStatistic(data);
        int offset = BLOCK_SIZE / 2;
        double dt = segment.getSampleInterval();
        double newStart = segment.getStartTime() + dt * offset;
        return new DetectionStatistic(segment.getId(), detectorid, newStart, dt, statistic);
    }

	public boolean isCompatibleWith(StreamSegment combined) {
		return combined.getId().equals(this.streamId);
	}
	
    
	public double getTemplateAutoCorrelation() {
		return templateAutoCorrelation;
	}

	public float[] getTemplateData() {
		return templateData;
	}

	public StreamIdentifier getStreamId() {
		return streamId;
	}

	public int getDetectorid() {
		return detectorid;
	}

}
