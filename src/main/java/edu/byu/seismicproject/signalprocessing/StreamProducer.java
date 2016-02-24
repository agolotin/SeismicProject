package main.java.edu.byu.seismicproject.signalprocessing;

import com.oregondsp.signalProcessing.filter.iir.Butterworth;
import com.oregondsp.signalProcessing.filter.iir.PassbandType;


public class StreamProducer {

    private final Butterworth filter;
    private final float[] dataArray;
    private final int blockSizeSamps;
    private final int numBlocks;
    private int currentBlock;

    private final double startTime;
    private final double sampleInterval;
    private final StreamIdentifier id;
    
	public StreamProducer(StreamIdentifier id, float[] mainData, long startTime, long endTime, 
			int secondsPerBlock, float sampleRate) {

    	//We are getting our data from a SAC file. Read SAC file entirely, 
    	//	but process it in 72000-sample-long blocks to simulate the way the real system processes data...
		//SACFile longData = new SACFile(longFile);
		//SACHeader dataHeader = longData.getHeader();
    	
		int dataLength = mainData.length;// REVIEWME: I'm assuming data length is the size of our full stream
		this.startTime = startTime;
		//double dt = 1.0/ Math.round(1.0 / dataHeader.delta); 
		
		// NOTE: https://courses.engr.illinois.edu/ece110/content/courseNotes/files/?samplingAndQuantization#SAQ-SMP
		this.sampleInterval = 1.0 / sampleRate;
		this.dataArray = mainData;//longData.getData();

		this.blockSizeSamps = (int) (secondsPerBlock * sampleRate);//blockSizeSamps;

		this.numBlocks = dataLength / blockSizeSamps;
		this.currentBlock = 0;
		
		this.id = id;
		
		// Set up IIR filter that will be used to filter all data blocks into the 2-8 Hz band
		int order = 4;
		float lowCorner = 2;
		float highCorner = 8;
		
		filter = new Butterworth(order, PassbandType.BANDPASS, lowCorner, highCorner, sampleInterval);

    }
    
    public boolean hasNext() {
        return currentBlock < numBlocks - 1;
    }

	public StreamSegment getNext() {
		float[] block = new float[blockSizeSamps];
		int offset = blockSizeSamps * currentBlock++;
		System.arraycopy(dataArray, offset, block, 0, blockSizeSamps);
				
		// Filter new data block
		filter.filter(block);
		return new StreamSegment(id, startTime + offset * sampleInterval, sampleInterval, block);
	}
	
    
	public Butterworth getFilter() {
		return filter;
	}

	public float[] getDataArray() {
		return dataArray;
	}

	public int getBlockSizeSamps() {
		return blockSizeSamps;
	}

	public int getNumBlocks() {
		return numBlocks;
	}

	public int getCurrentBlock() {
		return currentBlock;
	}

	public double getStartTime() {
		return startTime;
	}

	public double getSampleInterval() {
		return sampleInterval;
	}

	public StreamIdentifier getId() {
		return id;
	}
}