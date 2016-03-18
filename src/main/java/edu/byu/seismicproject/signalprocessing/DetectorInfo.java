
package main.java.edu.byu.seismicproject.signalprocessing;

public class DetectorInfo {

    private final int detectorid;

    private final DetectorSpecification specification;
    private final double detectorDelayInSeconds;

    public DetectorInfo(int detectorid,
            DetectorSpecification specification,
            double detectorDelayInSeconds) {
        this.detectorid = detectorid;
        this.specification = specification;
        this.detectorDelayInSeconds = detectorDelayInSeconds;
   }

    @Override
    public String toString() {
        return "DetectorInfo{" + "detectorid=" + detectorid +  ", detectorDelayInSeconds=" + detectorDelayInSeconds+'}';
    }

    /**
     * @return the detectorid
     */
    public int getDetectorid() {
        return detectorid;
    }


    /**
     * @return the triggerPositionType
     */
    public TriggerPositionType getTriggerPositionType() {
        return specification.getTriggerPositionType();
    }

    /**
     * @return the threshold
     */
    public double getThreshold() {
        return specification.getThreshold();
    }

    /**
     * @return the blackoutInterval
     */
    public float getBlackoutInterval() {
        return specification.getBlackoutPeriod();
    }

    /**
     * @return the detectorDelayInSeconds
     */
    public double getDetectorDelayInSeconds() {
        return detectorDelayInSeconds;
    }

    /**
     * @return the detectorType
     */
    public DetectorType getDetectorType() {
        return specification.getDetectorType();
    }

}
