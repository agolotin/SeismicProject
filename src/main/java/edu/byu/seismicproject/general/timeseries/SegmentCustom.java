package main.java.edu.byu.seismicproject.general.timeseries;

import java.sql.Timestamp;

import edu.iris.dmc.timeseries.model.Segment;

/**
 * Segment class to go with our implementation of the Timeseries class.
 */
@SuppressWarnings("serial")
public class SegmentCustom extends java.lang.Object implements java.io.Serializable {

	/*
	 * Default constructor
	 */
	public SegmentCustom() {
	}

	/*
	 * Constructor with type of segment (double, float, int, or short data) 
	 * and the sample rate
	 * @param type
	 * @param sampleRate
	 */
	public SegmentCustom(Segment.Type type, float sampleRate) {
		this.type = type;
		this.sampleRate = sampleRate;
	}

	// =========================== ||
	private Segment.Type type;
	private float sampleRate;

	//private List<Double> doubleData;
	//private List<Float> floatData;
	//private List<Integer> integerData;
	//private List<Short> shortData;

	// Once we discover what type of data we have, we just put it in a generic list
	private float[] mainData;

	// TODO: Consumer these times are not necessarily be
	// correct per consumer, so don't rely on them
	// To fix you need to go to ProducerKafka and make sure that when
	// you are sending data you override the timestamps
	private long startTime;
	private long endTime;
	private Timestamp expectedNextSampleTime;
	private int sampleCount;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (endTime ^ (endTime >>> 32));
		result = prime * result + ((expectedNextSampleTime == null) ? 0 : expectedNextSampleTime.hashCode());
		result = prime * result + ((mainData == null) ? 0 : mainData.hashCode());
		result = prime * result + sampleCount;
		result = prime * result + Float.floatToIntBits(sampleRate);
		result = prime * result + (int) (startTime ^ (startTime >>> 32));
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}


	@Override
	public String toString() {
		return "SegmentCustom [type=" + type + ", sampleRate=" + sampleRate + ", mainData=" + mainData
				+ ", endTime=" + endTime + ", expectedNextSampleTime=" + expectedNextSampleTime + "]";
	}

	public Segment.Type getType() {
		return type;
	}

	public float getSampleRate() {
		return sampleRate;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public Timestamp getExpectedNextSampleTime() {
		return expectedNextSampleTime;
	}

	public int getSampleCount() {
		return sampleCount;
	}

	public float[] getMainData() {
		return mainData;
	}

	public void setType(Segment.Type type) {
		this.type = type;
	}

	public void setSampleRate(float sampleRate) {
		this.sampleRate = sampleRate;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SegmentCustom other = (SegmentCustom) obj;
		if (endTime != other.endTime)
			return false;
		if (expectedNextSampleTime == null) {
			if (other.expectedNextSampleTime != null)
				return false;
		} else if (!expectedNextSampleTime.equals(other.expectedNextSampleTime))
			return false;
		if (mainData == null) {
			if (other.mainData != null)
				return false;
		} else if (!mainData.equals(other.mainData))
			return false;
		if (sampleCount != other.sampleCount)
			return false;
		if (Float.floatToIntBits(sampleRate) != Float.floatToIntBits(other.sampleRate))
			return false;
		if (startTime != other.startTime)
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public void setExpectedNextSampleTime(Timestamp expectedNextSampleTime) {
		this.expectedNextSampleTime = expectedNextSampleTime;
	}

	public void setSampleCount(int sampleCount) {
		this.sampleCount = sampleCount;
	}

	public void setMainData(float[] measurementsPerPartition) {
		this.mainData = measurementsPerPartition;
	}

}
