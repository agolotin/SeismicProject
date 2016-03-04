package main.java.edu.byu.seismicproject.signalprocessing;

public class CorrelationDetector {

	public final int BLOCK_SIZE;
    private final double templateAutoCorrelation; // this is the autocorrelation for two consecutive blocks

	private final float[] combinedData; // this represents data for two consecutive blocks
    private final StreamIdentifier streamId;
    private final int detectorid; // REVIEWME: What is detectorid?
    
    
//    public CorrelationDetector(StreamIdentifier id, long startSecond, double duration, double sampleInterval) {
    public CorrelationDetector(StreamIdentifier id, StreamSegment combined) {

    	// REVIEWME: I'm not sure what in this case delta should represent... is it sample interval as well?
        //double dt = combined.getSampleInterval();//Double.MAX_VALUE;//sacTemplate.getHeader().delta;
        //int offset = (int) Math.round(startSecond / dt);
    	//int npts = (int) Math.round(duration / dt);
        
        combinedData = combined.getData();
        BLOCK_SIZE = combinedData.length / 2;
        
        //float[] tmpArray = new float[Float.MAX_EXPONENT];//sacTemplate.getData();
        //templateData = new float[npts];
        
        //Compute autocorrelation…
		//System.arraycopy(tmpArray, offset, templateData, 0, npts);
        double tmp = 0;
        for (float v : combinedData) {
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
            
            for (int k = 0; k < combinedData.length; ++k) {
                int m = j + k + offset;
                dataAutoCorrelation += data[m] * data[m];
                crossCorrelation += data[m] * combinedData[k];
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
		return combinedData;
	}

	public StreamIdentifier getStreamId() {
		return streamId;
	}

	public int getDetectorid() {
		return detectorid;
	}

}
