
package edu.byu.seismicproject.signalprocessing;


public interface StreamProducer {
     boolean hasNext();

     StreamSegment getNext();

     double getStartTime();

     StreamIdentifier getId();
}
