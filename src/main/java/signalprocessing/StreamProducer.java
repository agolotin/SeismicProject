package main.java.signalprocessing;

import main.java.butterworth.Butterworth;


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
    	
    	// FIXME: I'm not sure I changed that I've made are correct...
    	
		// REVIEWME: A lot of things in this function were modified by me.. So it's really worth checking
		int dataLength = mainData.length;//dataHeader.npts;
		this.startTime = startTime;//dataHeader.getReferenceTime();
		//double dt = 1.0/ Math.round(1.0 / dataHeader.delta);
		
		this.sampleInterval = sampleInterval;//dt;
		this.dataArray = mainData;//longData.getData();

		// REVIEWME: I am assuming we will determine the size of a block depending on the interval and sample rate
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