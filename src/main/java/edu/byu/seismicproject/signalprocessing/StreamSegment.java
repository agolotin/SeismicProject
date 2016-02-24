package main.java.edu.byu.seismicproject.signalprocessing;

import java.util.Arrays;

public class StreamSegment {

	/// REVIEWME: Are the start time and sample interval in seconds or milliseconds? 
	///				Right now the program assumes milliseconds, but it will probably fail...
	private final double startTime;  
    private final double sampleInterval; 
    private final float[] data;
    private final StreamIdentifier id;

    public StreamSegment(StreamIdentifier id, double startTime, double sampleInterval, float[] data) {
        this.id = id;
        this.startTime = startTime;
        this.sampleInterval = sampleInterval;
        this.data = data;
    }

    // TODO: Finish logic here based on the comment
    private boolean isCompatible(StreamSegment other) {
    	// Should verify compatible (possibly not identical) sample intervals and end time of 
    	//	first is one sample less than start time of second. At least be sure the IDs match.
        return this.id.equals(other.id);
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
    	return new StreamSegment(segment1.id, segment1.startTime, segment1.sampleInterval, combinedBlock);
    }

    // ======== GETTERS ========= ||

    public double getStartTime() {
		return startTime;
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
