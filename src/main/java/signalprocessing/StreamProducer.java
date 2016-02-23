package main.java.signalprocessing;

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
    		double sampleInterval, float sampleRate) {

    	//We are getting our data from a SAC file. Read SAC file entirely, 
    	//	but process it in 72000-sample-long blocks to simulate the way the real system processes data...
		//SACFile longData = new SACFile(longFile);
		//SACHeader dataHeader = longData.getHeader();
    	
		int dataLength = mainData.length;//dataHeader.npts; REVIEWME: I'm assuming data length is the size of our full stream
		this.startTime = startTime;//dataHeader.getReferenceTime();
		//double dt = 1.0/ Math.round(1.0 / dataHeader.delta); REVIEWME: From what I understand, delta in our case is our sample interval (secods per block)
		
		this.sampleInterval = sampleInterval;//dt;
		this.dataArray = mainData;//longData.getData();

		// REVIEWME: I am assuming the way we calculate the size of a block is seconds per block times sample rate
		this.blockSizeSamps = (int) (sampleInterval * sampleRate);//blockSizeSamps;

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