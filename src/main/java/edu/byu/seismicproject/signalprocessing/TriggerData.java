
package edu.byu.seismicproject.signalprocessing;


public class TriggerData {
    private final DetectorInfo detectorInfo;
    private final int index;
    private final double triggerTime;
    private final float statistic;

    public TriggerData(DetectorInfo detectorInfo, int index, double triggerTime, float statistic) {
        this.detectorInfo = detectorInfo;
        this.index = index;
        this.triggerTime = triggerTime;
        this.statistic = statistic;
        
    }

    @Override
    public String toString() {
        return "TriggerData{" + "detectorInfo=" + detectorInfo + ", index=" + index + ", triggerTime=" + triggerTime + ", statistic=" + statistic + '}';
    }

    /**
     * @return the detectorInfo
     */
    public DetectorInfo getDetectorInfo() {
        return detectorInfo;
    }

    /**
     * @return the index
     */
    public int getIndex() {
        return index;
    }

    /**
     * @return the triggerTime
     */
    public double getTriggerTime() {
        return triggerTime;
    }

    /**
     * @return the statistic
     */
    public float getStatistic() {
        return statistic;
    }
    
}
