/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main.java.edu.byu.seismicproject.producer;

import com.oregondsp.signalProcessing.filter.iir.Butterworth;
import com.oregondsp.signalProcessing.filter.iir.PassbandType;

import main.java.edu.byu.seismicproject.signalprocessing.IStreamProducer;
import main.java.edu.byu.seismicproject.signalprocessing.StreamIdentifier;
import main.java.edu.byu.seismicproject.signalprocessing.StreamSegment;

/**
 *
 * @author dodge1
 */
public class AnotherStreamProducer implements IStreamProducer {

    private final Butterworth filter;
    private float[] dataArray;
    private final int blockSizeSamps;
    private final int numBlocks;
    private int currentBlock;

    private final double startTime;
    private final double sampleInterval;
    private final StreamIdentifier id;

    public AnotherStreamProducer(StreamIdentifier id, float[] rawData,
            long startTime, long endTime,
            int minutesPerBlock, float sampleRate) {

    	dataArray = rawData;
        int dataLength = rawData.length;
        this.startTime = startTime;

        // NOTE: https://courses.engr.illinois.edu/ece110/content/courseNotes/files/?samplingAndQuantization#SAQ-SMP
        this.sampleInterval = 1.0 / sampleRate;

        this.blockSizeSamps = (int) (minutesPerBlock * 60 * sampleRate);

        this.numBlocks = dataLength / blockSizeSamps;
        this.currentBlock = 0;

        this.id = id;

        // Set up IIR filter that will be used to filter all data blocks into a specific band
        int order = id.getBand().getOrder();
        double lowCorner = id.getBand().getLowCorner();
        double highCorner = id.getBand().getHighCorner();

        filter = new Butterworth(order, PassbandType.BANDPASS, lowCorner, highCorner, sampleInterval);
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
