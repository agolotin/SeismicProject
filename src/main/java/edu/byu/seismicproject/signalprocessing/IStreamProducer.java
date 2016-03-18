
package main.java.edu.byu.seismicproject.signalprocessing;


public interface IStreamProducer {
     boolean hasNext();

     StreamSegment getNext();

     double getStartTime();

     StreamIdentifier getId();
}
