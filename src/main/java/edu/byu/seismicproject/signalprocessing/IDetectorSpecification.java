package main.java.edu.byu.seismicproject.signalprocessing;


public interface IDetectorSpecification {

    public DetectorType            getDetectorType();                      // instance of enumeration denoting type of detector

    public float                   getThreshold();                         // threshold on detection statistic for declaring triggers

    public float                   getBlackoutPeriod();                    // blackout period (seconds) is period over which triggers are suppressed following 
    
    public int                     getNumChannels();                       // returns the number of channels

    public TriggerPositionType     getTriggerPositionType();               // specifies whether triggers are formed on threshold crossings or at maximum of the statistic

    public boolean                 spawningEnabled();                      // returns "true" if spawning is enabled, false otherwise - always false for subspace detectors
}
