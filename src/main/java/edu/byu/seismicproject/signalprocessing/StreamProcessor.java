/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.byu.seismicproject.signalprocessing;

import java.io.FileNotFoundException;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 *
 * @author dodge1
 */
public class StreamProcessor {

    private final StreamIdentifier id;
    StreamSegment previous = null;
    double streamStart = -9999999;
    private final DetectorHolder detectors;
    private final DetectionStatisticWriter statisticWriter;
    private final DetectionStatisticScanner statisticScanner;
    private static boolean writeStatistics = false;

    StreamProcessor(DetectorHolder detectors, StreamIdentifier id) throws FileNotFoundException {
        this.id = id;
        this.detectors = detectors;
        statisticWriter = writeStatistics ? new DetectionStatisticWriter(detectors) : null;
        boolean triggerOnlyOnCorrelators = true;
        statisticScanner = new DetectionStatisticScanner(triggerOnlyOnCorrelators);
    }

    public void pushSegment(StreamSegment segment) {
        printBlockStartTime(segment);
        if (previous != null) {
            StreamSegment combined = StreamSegment.combine(segment, previous);
            for (CorrelationDetector detector : detectors.getDetectors()) {
                if (detector.isCompatibleWith(combined)) {
                    DetectionStatistic statistic = detector.produceStatistic(combined);
                    if (writeStatistics) {
                        statisticWriter.writeStatistic(detector, statistic, streamStart);
                    }
                    statisticScanner.addStatistic(statistic);
                }
            }
            Collection<TriggerData> triggers = statisticScanner.scanForTriggers();
            if (!triggers.isEmpty()) {
                processAllTriggers(triggers);
            }
        } else {
            streamStart = segment.getStartTime();
        }

        previous = segment;
    }

    public void close() {
        if (writeStatistics) {
            statisticWriter.close();
        }
    }

    private void printBlockStartTime(StreamSegment segment) {
        double time = segment.getStartTime();
        String ts = this.getTimeString(time);
        System.out.println("Processing block starting:    " + ts);
    }

    private String getTimeString(double time) {
        GregorianCalendar d = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
        d.clear();
        d.setTimeInMillis((long) (time * 1000));
        int year = d.get(Calendar.YEAR);
        int doy = d.get(Calendar.DAY_OF_YEAR);
        int hour = d.get(Calendar.HOUR_OF_DAY);
        int min = d.get(Calendar.MINUTE);
        int sec = d.get(Calendar.SECOND);
        int msec = d.get(Calendar.MILLISECOND);
        return String.format("%04d-%03d %02d:%02d:%02d.%03d", year, doy, hour, min, sec, msec);
    }

    private void processAllTriggers(Collection<TriggerData> triggers) {
        triggers.stream().forEach((td) -> {
            System.out.println(td);
        });
    }

    public static void writeDetectionStatistics(boolean doWrite) {
        writeStatistics = doWrite;
    }

}
