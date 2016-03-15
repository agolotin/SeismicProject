/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.byu.seismicproject.signalprocessing;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author dodge1
 */
public class SimpleFramework {

    public static void main(String[] args) {
        SimpleFramework producer = new SimpleFramework();
        producer.run();
    }

    private void run() {

        try {
            //The detector holder simply creates a few correlation detectors by reading the templates
            //From ASCII files.
            DetectorHolder detectors = new DetectorHolder();

            //The stream producer simulates a continuous stream of data. In this case a single channel.
            //Although the entire stream is being held in memory internally, a real implementation would
            //get data as it became available. To simulate, that, the producer emits only blocks of data
            // of length blockSizeSamps.
            int blockSizeSamps = CorrelationDetector.BLOCK_SIZE;
            StreamProducer stream = new StreamProducer(blockSizeSamps);
            StreamProcessor aStreamProcessor = new StreamProcessor(detectors, stream.getId());
            
            while (stream.hasNext()) {
                StreamSegment segment = stream.getNext();
                aStreamProcessor.pushSegment(segment);
            }
            aStreamProcessor.close();
        } catch (Exception ex) {
            Logger.getLogger(SimpleFramework.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
