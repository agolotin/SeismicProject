package main.java.signalprocessing;

import java.util.Arrays;

public class StreamSegment {

	private final double startTime; // This is milliseconds !! 
    private final double sampleInterval; // REVIEWME: This is milliseconds..... ?
    private final float[] data;
    private final StreamIdentifier id;

    public StreamSegment(StreamIdentifier id, double startTime, double sampleInterval, float[] data) {
        this.id = id;
        this.startTime = startTime;
        this.sampleInterval = sampleInterval;
        this.data = data;
    }

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
