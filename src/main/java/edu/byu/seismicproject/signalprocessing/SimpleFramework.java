
package main.java.edu.byu.seismicproject.signalprocessing;

import java.util.logging.Level;
import java.util.logging.Logger;


public class SimpleFramework {

    public static void main(String[] args) {
        SimpleFramework producer = new SimpleFramework();
        producer.run();
    }

    private void run() {

        try {
            //The detector holder simply creates a few correlation detectors by reading the templates
            //From ASCII files. Note that in the distributed application all detectors are asumed to be held in
            // the shared cluster memory. As new detectors are created they are added to that collection.
            // 
            DetectorHolder detectors = new DetectorHolder();

            //The stream producer simulates a continuous stream of data: In this case a single channel.
            //Although the entire stream is being held in memory internally, a real implementation would
            //get data as it became available. To simulate, that, the producer emits only blocks of data
            // of length blockSizeSamps.
            int blockSizeSamps = 72000; // At a sample interval of .05 s this is one hour. 
            // The correlation detector needs to know the block size so it offsets properly in the blocks.
            CorrelationDetector.setBlockSize(blockSizeSamps);
            
            
            ToyStreamProducer stream = new ToyStreamProducer(blockSizeSamps);
            
            //The StreamProcessor coordinates the process of calculating detection statistics, looking for triggers,
            // screening triggers, creating detections, creating new detectors. Each stream has its own StreamProcessor
            // and each StreamProcessor must be able to access the collection of detectors. Here I am just passing in
            // the collection of detectors.
            
                       
            // Outputting detection statistics is useful for debugging, but in general is not desirable. For the
            // present, output is on. This statis method must be called prior to instantiating any StreamProcessors.
            StreamProcessor.writeDetectionStatistics(true);
 
            StreamProcessor aStreamProcessor = new StreamProcessor(detectors, stream.getId());
            
            
            // In the real application, this is effectively an infinite loop.
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
