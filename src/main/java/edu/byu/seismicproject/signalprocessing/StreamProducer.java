
package edu.byu.seismicproject.signalprocessing;

import com.oregondsp.signalProcessing.filter.iir.Butterworth;
import com.oregondsp.signalProcessing.filter.iir.PassbandType;
import edu.byu.seismicproject.general.band.SeismicBand;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class StreamProducer {

    private final Butterworth filter;
    private float[] dataArray;
    private final int blockSizeSamps;
    private final int numBlocks;
    private int currentBlock;

    private final double startTime;
    private final double sampleInterval;
    private final StreamIdentifier id;
    private StreamSegment[] filteredDataBlocks;

    public StreamProducer(StreamIdentifier id, float[] rawData,
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

    public StreamProducer(int blockSizeSamps) throws IOException {
        int order = 4;
        float lowCorner = 2;
        float highCorner = 8;
        id = new StreamIdentifier("IU", "KBS", "BHZ", "00", new SeismicBand(1, order, lowCorner, highCorner));
        ArrayList<String> lines = new ArrayList<>();
        try (InputStream in = getClass().getResourceAsStream("/detector/KBS.BHZ00.2007.298.01.13.03.txt")) {

            try (InputStreamReader isr = new InputStreamReader(in)) {
                try (BufferedReader br = new BufferedReader(isr)) {
                    String line = br.readLine();
                    while (line != null) {
                        lines.add(line);
                        line = br.readLine();
                    }

                }
            }
            dataArray = new float[lines.size()];
            for (int j = 0; j < lines.size(); ++j) {
                dataArray[j] = Float.parseFloat(lines.get(j));
            }
            int dataLength = dataArray.length;
            startTime = 1.193274783E9;
            double dt = 0.05;
            sampleInterval = dt;

            this.blockSizeSamps = blockSizeSamps;

            numBlocks = dataLength / blockSizeSamps;
            currentBlock = 0;
            // Set up IIR filter that will be used to filter all data blocks into the 2-8 Hz band...
            filter = new Butterworth(order,
                    PassbandType.BANDPASS,
                    lowCorner,
                    highCorner,
                    dt);

        }

    }

    public boolean hasNext() {
        return currentBlock < numBlocks - 1;
    }

    public StreamSegment getNext() {
        float[] block = new float[blockSizeSamps];
        int offset = blockSizeSamps * currentBlock++;
        System.arraycopy(dataArray, offset, block, 0, blockSizeSamps);
        //Filter new data block...
        filter.filter(block);
        return new StreamSegment(id, startTime + offset * sampleInterval, sampleInterval, block);
    }

    public double getStartTime() {
        return startTime;
    }

    public StreamIdentifier getId() {
        return id;
    }
}
