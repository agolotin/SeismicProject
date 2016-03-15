/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.byu.seismicproject.producer;

import com.oregondsp.signalProcessing.filter.iir.Butterworth;
import com.oregondsp.signalProcessing.filter.iir.PassbandType;
import edu.byu.seismicproject.signalprocessing.StreamIdentifier;
import edu.byu.seismicproject.signalprocessing.StreamProducer;
import edu.byu.seismicproject.signalprocessing.StreamSegment;

/**
 *
 * @author dodge1
 */
public class AnotherStreamProducer implements StreamProducer {

    private final Butterworth filter;
    private float[] dataArray;
    private final int blockSizeSamps;
    private final int numBlocks;
    private int currentBlock;

    private final double startTime;
    private final double sampleInterval;
    private final StreamIdentifier id;
    private final StreamSegment[] filteredDataBlocks;

    public AnotherStreamProducer(StreamIdentifier id, float[] rawData,
            long startTime, long endTime,
            int secondsPerBlock, float sampleRate) {

        int dataLength = rawData.length;
        this.startTime = startTime;

        // NOTE: https://courses.engr.illinois.edu/ece110/content/courseNotes/files/?samplingAndQuantization#SAQ-SMP
        this.sampleInterval = 1.0 / sampleRate; // this is how many seconds there are per sample

        this.blockSizeSamps = (int) (secondsPerBlock * sampleRate);//blockSizeSamps;

        this.numBlocks = dataLength / blockSizeSamps;
        this.currentBlock = 0;

        this.id = id;

        // Set up IIR filter that will be used to filter all data blocks into a specific band
        int order = id.getBand().getOrder();
        double lowCorner = id.getBand().getLowCorner();
        double highCorner = id.getBand().getHighCorner();

        filter = new Butterworth(order, PassbandType.BANDPASS, lowCorner, highCorner, sampleInterval);

        filteredDataBlocks = new StreamSegment[numBlocks];
        this.createFilteredBlocksFromRawData(rawData);
    }

    /**
     * This function generates a list of StreamSegment blocks that will be
     * processed by the Ignite client. This is done in order for us to discard
     * the raw data and keep only the filtered data stream in blocks FIXME: Make
     * sure we are setting the start and end time here correctly....it seems
     * like we are not...
     *
     * @param rawData raw data stream of incoming data
     */
    private void createFilteredBlocksFromRawData(float[] rawData) {
        for (int i = 0; i < numBlocks; i++) {

            float[] block = new float[blockSizeSamps];
            int offset = blockSizeSamps * i;
            System.arraycopy(rawData, offset, block, 0, blockSizeSamps);

            // Filter new data block
            filter.filter(block);

            // TODO: make sure this double to long conversion won't screw things up
            double blockStartTime = startTime + (offset * sampleInterval);
            filteredDataBlocks[i] = new StreamSegment(id, blockStartTime, sampleInterval, block);
        }
    }

    @Override
    public boolean hasNext() {
        return currentBlock < numBlocks - 1;
    }

    @Override
    public StreamSegment getNext() {
        float[] block = new float[blockSizeSamps];
        int offset = blockSizeSamps * currentBlock++;
        System.arraycopy(dataArray, offset, block, 0, blockSizeSamps);
        //Filter new data block...
        filter.filter(block);
        return new StreamSegment(id, startTime + offset * sampleInterval, sampleInterval, block);
    }

    @Override
    public double getStartTime() {
        return startTime;
    }

    @Override
    public StreamIdentifier getId() {
        return id;
    }

}
