package main.java.edu.byu.seismicproject.signalprocessing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("serial")
public class DetectionStatisticScanner implements Serializable {

    Map<Integer, Queue<DetectionStatistic>> detectorStatisticMap;
    Map<Integer, Integer> detectorOverRunMap;
    private final boolean triggerOnlyOnCorrelators;

    public boolean isTriggerOnlyOnCorrelators() {
        return triggerOnlyOnCorrelators;
    }

    public DetectionStatisticScanner(boolean triggerOnlyOnCorrelators) {
        detectorStatisticMap = new ConcurrentHashMap<>();
        detectorOverRunMap = new ConcurrentHashMap<>();
        this.triggerOnlyOnCorrelators = triggerOnlyOnCorrelators;
    }

    public void addStatistic(DetectionStatistic statistic) {
        Queue<DetectionStatistic> statisticPair = detectorStatisticMap.get(statistic.getDetectorInfo().getDetectorid());
        if (statisticPair == null) {
            statisticPair = new ArrayBlockingQueue<>(2);
            detectorStatisticMap.put(statistic.getDetectorInfo().getDetectorid(), statisticPair);
            detectorOverRunMap.put(statistic.getDetectorInfo().getDetectorid(), 0);
        }
        if (statisticPair.size() == 2) { //Remove the oldest statistic to make room for the new
            statisticPair.remove();
        }
        statisticPair.add(statistic);
    }

    public Collection<TriggerData> scanForTriggers() {
        Collection<TriggerData> result = new ArrayList<>();
        detectorStatisticMap.keySet().stream().forEach((detectorid) -> {
            int lastBlockOverrun = detectorOverRunMap.get(detectorid);
            Queue<DetectionStatistic> statisticPair = detectorStatisticMap.get(detectorid);
            DetectionStatistic[] statistics = statisticPair.toArray(new DetectionStatistic[1]);
            if (statistics.length == 2) {
                DetectionStatistic combined = DetectionStatistic.combine(statistics);
                DetectorInfo detectorInfo = combined.getDetectorInfo();
                if (!(triggerOnlyOnCorrelators && (detectorInfo.getDetectorType() != DetectorType.SUBSPACE && detectorInfo.getDetectorType() != DetectorType.CORRELATION))) {
                    int blackoutSamples = (int) (detectorInfo.getBlackoutInterval() * combined.getSampleRate());
                    TriggerPositionType triggerPositionType = detectorInfo.getTriggerPositionType();
                    
                    float[] statistic = combined.getStatistic();
                    int blockSize = statistics[0].size();
                    int index = blockSize / 2 + lastBlockOverrun;
                    int finish = index + blockSize;
                    while (index < finish) {
                        if (statistic[index] > detectorInfo.getThreshold()) {
                            int initialTriggerIndex = index;
                            float maxDetStat = statistic[index];
                            
                            int maxIndex = index;
                            int blackOutCount = 0;
                            while (index < finish && (blackOutCount < blackoutSamples || statistic[index] > detectorInfo.getThreshold())) {
                                if (statistic[index] > maxDetStat) {
                                    maxDetStat = statistic[index];
                                    maxIndex = index;
                                }
                                index++;
                                blackOutCount++;
                            }
                            int overRun = index - finish;
                            if (overRun < 0) {
                                overRun = 0;
                            }
                            
                            detectorOverRunMap.put(detectorid, overRun);
                            
                            int correctedIndex = triggerPositionType == TriggerPositionType.STATISTIC_MAX ? maxIndex : initialTriggerIndex;
                            double triggerTime = getCorrectedTriggerTime(combined, correctedIndex);
                            TriggerData trigger = new TriggerData(detectorInfo, correctedIndex, triggerTime, maxDetStat);
                            result.add(trigger);
                        } else {
                            ++index;
                        }
                    }
                }
            }
        });
        return result;
    }

    private double getCorrectedTriggerTime(DetectionStatistic combined, int index) {
        double uncorrectedTriggerTime = combined.getStartTime() + index / combined.getSampleRate();
        return uncorrectedTriggerTime - combined.getDetectorInfo().getDetectorDelayInSeconds();
    }

}
