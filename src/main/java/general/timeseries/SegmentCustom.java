package main.java.general.timeseries;

import java.sql.Timestamp;
import java.util.List;

import edu.iris.dmc.timeseries.model.Segment;

/**
 * Segment class to go with our implementation of the Timeseries class.
 */
@SuppressWarnings({ "rawtypes", "serial" })
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
	private List mainData;

	// TODO: Consumer these times are not necessarily be
	// correct per consumer, so don't rely on them
	// To fix you need to go to ProducerKafka and make sure that when
	// you are sending data you override the timestamps
	private Timestamp startTime;
	private Timestamp endTime;
	private Timestamp expectedNextSampleTime;
	private int sampleCount;

	// =========================== ||
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((endTime == null) ? 0 : endTime.hashCode());
		result = prime * result + ((expectedNextSampleTime == null) ? 0 : expectedNextSampleTime.hashCode());
		result = prime * result + Float.floatToIntBits(sampleRate);
		result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		SegmentCustom other = (SegmentCustom) obj;
		if (endTime == null) {
			if (other.endTime != null) {
				return false;
			}
		}
		else {
			if (!endTime.equals(other.endTime)) {
				return false;
			}
		}
		if (expectedNextSampleTime == null) {
			if (other.expectedNextSampleTime != null) {
				return false;
			}
		}
		else {
			if (!expectedNextSampleTime.equals(other.expectedNextSampleTime)) {
				return false;
			}
		}
		if (Float.floatToIntBits(sampleRate) != Float.floatToIntBits(other.sampleRate)) {
			return false;
		}
		if (startTime == null) {
			if (other.startTime != null) {
				return false;
			}
		}
		else {
			if (!startTime.equals(other.startTime)) {
				return false;
			}
		}
		if (type != other.type) {
			return false;
		}
		return true;
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

	public Timestamp getStartTime() {
		return startTime;
	}

	public Timestamp getEndTime() {
		return endTime;
	}

	public Timestamp getExpectedNextSampleTime() {
		return expectedNextSampleTime;
	}

	public int getSampleCount() {
		return sampleCount;
	}

	public List getMainData() {
		return mainData;
	}

	public void setType(Segment.Type type) {
		this.type = type;
	}

	public void setSampleRate(float sampleRate) {
		this.sampleRate = sampleRate;
	}

	public void setStartTime(Timestamp startTime) {
		this.startTime = startTime;
	}

	public void setEndTime(Timestamp timestamp) {
		this.endTime = timestamp;
	}

	public void setExpectedNextSampleTime(Timestamp expectedNextSampleTime) {
		this.expectedNextSampleTime = expectedNextSampleTime;
	}

	public void setSampleCount(int sampleCount) {
		this.sampleCount = sampleCount;
	}

	public void setMainData(List mainData) {
		this.mainData = mainData;
	}

}
