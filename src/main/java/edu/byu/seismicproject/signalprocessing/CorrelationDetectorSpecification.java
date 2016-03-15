
package edu.byu.seismicproject.signalprocessing;

public class CorrelationDetectorSpecification implements DetectorSpecification {

    private final float threshold;
    private final float blackoutPeriod;
    
    public CorrelationDetectorSpecification(float threshold, float blackoutPeriod)
    {
        this.threshold = threshold;
        this.blackoutPeriod = blackoutPeriod;
    }

    @Override
    public DetectorType getDetectorType() {
        return DetectorType.CORRELATION;
    }

    @Override
    public float getThreshold() {
        return threshold;
    }

    @Override
    public float getBlackoutPeriod() {
        return blackoutPeriod;
    }

    @Override
    public int getNumChannels() {
        return 1;
    }

    @Override
    public TriggerPositionType getTriggerPositionType() {
        return TriggerPositionType.STATISTIC_MAX;
    }

    @Override
    public boolean spawningEnabled() {
        return false;
    }
    
}
