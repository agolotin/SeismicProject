
package main.java.edu.byu.seismicproject.signalprocessing;

import com.oregondsp.signalProcessing.filter.iir.Butterworth;
import com.oregondsp.signalProcessing.filter.iir.PassbandType;
import main.java.edu.byu.seismicproject.general.band.SeismicBand;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * The ToyStreamProducer class is intended to emulate the mechanics of a system that acquires continuous
 * data and makes it available in blocks for a StreamProcessor. This implementation stores the
 * entire amount to be processed in an internal array and makes it available in blocks
 * @author dodge1
 */
public class ToyStreamProducer  implements StreamProducer{

 private final Butterworth filter;
    private final float[] dataArray;
    private final int blockSizeSamps;
    private final int numBlocks;
    private int currentBlock;

    private final double startTime;
    private final double sampleInterval;
    private final StreamIdentifier id;

    /**
     * This is the constructor for use in the SimpleFramework. It reads in several hours of data from an ASCII text file 
     * stored as a Maven resource. The time and sample rate are hard-coded just to keep things simple. Block size is set by 
     * the application.
     * @param blockSizeSamps
     * @throws IOException 
     */
    public ToyStreamProducer(int blockSizeSamps) throws IOException {
        int order = 4;
        float lowCorner = 2;
        float highCorner = 8;
        id = new StreamIdentifier("IU", "KBS", "BHZ", "00", new SeismicBand(1, order, lowCorner, highCorner));
        ArrayList<String> lines = new ArrayList<>();
        try (InputStream in = getClass().getResourceAsStream("/main/resources/detector/KBS.BHZ00.2007.298.01.13.03.txt")) {

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
