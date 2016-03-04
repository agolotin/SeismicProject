package main.java.edu.byu.seismicproject.signalprocessing;

import java.io.Serializable;
import java.util.Arrays;

@SuppressWarnings("serial")
public class StreamSegment implements Serializable {

	/// REVIEWME: Are the start time and sample interval in seconds or milliseconds? 
	///				Right now the program assumes seconds
	private final long startTime;  
	private final long endTime;
    private final double sampleInterval; 
    private final float[] data;
    private final StreamIdentifier id;

    public StreamSegment(StreamIdentifier id, long startTime, long endTime, double sampleInterval, float[] data) {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
        this.sampleInterval = sampleInterval;
        this.data = data;
    }

    // TODO: Make sure all the logic is correct here
    private boolean isCompatible(StreamSegment other) {
    	// Should verify compatible (possibly not identical) sample intervals and end time of 
    	//	first is one sample less than start time of second.
    	
    	// The IDs have to be the same for streams to be compatible
        if (!this.id.equals(other.id)) 
        	return false;
        // The bands also have to be the same for streams to be compatible... just double checking
        if (!this.id.getBand().equals(other.getId().getBand())) 
        	return false;
        if (this.startTime != (other.endTime)) // make sure this check is the same as described above...
        	return false;
        
        if (this.equals(other)) // If the streams are exactly the same it's bad...right?
        	return false;
        
        return true;
    }
    
    @Override
	public String toString() {
		return "StreamSegment [startTime=" + startTime + ", endTime=" + endTime + ", sampleInterval=" + sampleInterval
				+ ", blockSize = " + data.length + ", id=" + id + "]";
	}

	public static StreamSegment combine(StreamSegment segment1, StreamSegment segment2) {
    	if (!segment1.isCompatible(segment2)) {
    	   throw new IllegalStateException("Segments are incompatible!");
    	}
    	
    	float[] block1 = segment1.getData();
    	float[] block2 = segment2.getData();
    	int blockSizeSamps = block1.length;
    	
    	if (block2.length != blockSizeSamps) {
    	   throw new IllegalStateException("Blocks have different lengths!");
    	}

    	float[] combinedBlock = new float[2 * blockSizeSamps];
    	//Copy block1 data into block0 position (right-shift)
    	System.arraycopy(block2, 0, combinedBlock, blockSizeSamps, blockSizeSamps);

        //Copy new block into position 0
    	System.arraycopy(block1, 0, combinedBlock, 0, blockSizeSamps);
    	return new StreamSegment(segment1.id, segment1.startTime, segment2.endTime, segment1.sampleInterval, combinedBlock);
    }
	
	/**
	 * Make sure that the two objects follow each other
	 * @param nextSegment - theoretical next segment
	 * @return Boolean representing whether to segments follow each other
	 */
	public boolean isPreviousTo(StreamSegment nextSegment) {
		return this.endTime == nextSegment.startTime;
	}
    

    // ======== GETTERS ========= ||

    public long getStartTime() {
		return startTime;
	}
    
    public long getEndTime() {
    	return endTime;
    }

	public double getSampleInterval() {
		return sampleInterval;
	}

	public float[] getData() {
		return data;
	}

	public StreamIdentifier getId() {
		return id;
	}
	
    // ========================== ||

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(data);
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		long temp;
		temp = Double.doubleToLongBits(sampleInterval);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(startTime);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StreamSegment other = (StreamSegment) obj;
		if (!Arrays.equals(data, other.data))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (Double.doubleToLongBits(sampleInterval) != Double.doubleToLongBits(other.sampleInterval))
			return false;
		if (Double.doubleToLongBits(startTime) != Double.doubleToLongBits(other.startTime))
			return false;
		return true;
	}

}
