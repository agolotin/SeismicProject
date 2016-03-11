package main.java.edu.byu.seismicproject.signalprocessing;

public class CorrelationDetector {

	private final int BLOCK_SIZE;
    private final double templateAutoCorrelation; // this is the autocorrelation for two consecutive blocks

	private final float[] templateData; // this represents data for two consecutive blocks
    private final StreamIdentifier streamId;
    private final int detectorid; // REVIEWME: What is detectorid?
    
    
//    public CorrelationDetector(StreamIdentifier id, long startSecond, double duration, double sampleInterval) {
    public CorrelationDetector(StreamIdentifier id, StreamSegment combined) {

        //double dt = combined.getSampleInterval();
        //int offset = (int) Math.round(startSecond / dt);
    	//int npts = (int) Math.round(duration / dt);
        
        //templateData = combined.getData();
        //BLOCK_SIZE = templateData.length / 2;
        
        float[] tmpData = combined.getData();
        BLOCK_SIZE = tmpData.length / 2;
        
        templateData = new float[BLOCK_SIZE];
        int offset =  BLOCK_SIZE / 2;
        System.arraycopy(tmpData, offset, templateData, 0, BLOCK_SIZE);
        
        //float[] tmpArray = sacTemplate.getData();
        //templateData = new float[npts];
        
        //Compute autocorrelation…
		//System.arraycopy(tmpArray, offset, templateData, 0, npts);
        double tmp = 0;
        for (float v : templateData) {
            tmp += v * v;
        }
        templateAutoCorrelation = tmp;

        this.streamId = id;
        this.detectorid = Integer.MAX_VALUE; 
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
                int m = j + k + offset; // this is the movement of our two-block signal
                
                dataAutoCorrelation += data[m] * data[m];
                crossCorrelation += data[m] * templateData[k];
            }
            
            double denom = Math.sqrt(dataAutoCorrelation * templateAutoCorrelation);
            double cc = denom == 0 ? 0 : crossCorrelation / Math.sqrt(dataAutoCorrelation * templateAutoCorrelation);
            result[j] = (float) (cc*cc);
        }
        return result;
    }
    
    public DetectionStatistic produceStatistic(StreamSegment segment) {
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
