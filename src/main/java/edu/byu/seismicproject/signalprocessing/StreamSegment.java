
package main.java.edu.byu.seismicproject.signalprocessing;


public class StreamSegment {

    private final double startTime;
    private final double sampleInterval;
    private final float[] data;
    private final StreamIdentifier id;
    private final double endTime;

    public StreamSegment(StreamIdentifier id, double startTime, double sampleInterval, float[] data) {
        this.id = id;
        this.startTime = startTime;
        this.sampleInterval = sampleInterval;
        this.data = data;
        this.endTime = startTime + (data.length - 1) * sampleInterval;
    }

    public double getStartTime() {
        return startTime;
    }

    public double getSampleInterval() {
        return sampleInterval;
    }

    public float[] getData() {
        return data.clone();
    }

    private boolean isCompatible(StreamSegment other) {
        // Should verify compatible (possibly not identical) sample intervals and end time of first is one sample less than start time of second.
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

    public StreamIdentifier getId() {
        return id;
    }

    /**
     * Make sure that the two objects follow each other. Next sample should
     * start 1 sample interval after time of last sample in first segment.
     * Because of roundoff and sampling jitter an exact comparison is likely to
     * fail, so test within 1/2 sample of expected.
     *
     * @param nextSegment - theoretical next segment
     * @return Boolean representing whether two segments follow each other
     */
    public boolean isPreviousTo(StreamSegment nextSegment) {
        return Math.abs(endTime + sampleInterval - nextSegment.startTime) < sampleInterval / 2;
    }

    public double getEndTime() {
        return endTime;
    }

}
