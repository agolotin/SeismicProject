package main.java.timeseries;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.iris.dmc.fdsn.station.model.Channel;
import edu.iris.dmc.timeseries.model.Segment;


/* Google Kryo does not serialize the java.util.login.Logger that is present in both Timeseries and Segment classes
 * This class almost fully implements the functionality of the Timseries class
 */
public class TimeseriesCustom extends java.lang.Object implements java.io.Serializable {

	public TimeseriesCustom() { }
	
	public TimeseriesCustom(String networkCode, String stationCode, String location, String channelCode) {
		this.networkCode = networkCode;
		this.stationCode = stationCode;
		this.location = location;
		this.channelCode = channelCode;
	}
	
	// ================================== ||
	private String networkCode;
	private String stationCode;
	private String location;
	private String channelCode;
	
	private Channel channel;
	private char dataQuality;
	
	private Collection<SegmentCustom> segments;
	// ================================== ||

	public String getNetworkCode() {
		return networkCode;
	}

	public String getStationCode() {
		return stationCode;
	}

	public String getLocation() {
		return location;
	}

	public String getChannelCode() {
		return channelCode;
	}

	public Channel getChannel() {
		return channel;
	}

	public char getDataQuality() {
		return dataQuality;
	}

	public Collection<SegmentCustom> getSegments() {
		return segments;
	}

	public void setNetworkCode(String networkCode) {
		this.networkCode = networkCode;
	}

	public void setStationCode(String stationCode) {
		this.stationCode = stationCode;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public void setChannelCode(String channelCode) {
		this.channelCode = channelCode;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public void setDataQuality(char dataQuality) {
		this.dataQuality = dataQuality;
	}

	public void setSegments(Collection<Segment> segments) {
		List<SegmentCustom> scCollection = new ArrayList<SegmentCustom>();
		for (Segment s : segments) {
			SegmentCustom sc = new SegmentCustom(s.getType(), s.getSamplerate());
			sc.setDoubleData(s.getDoubleData());
			sc.setFloatData(s.getFloatData());
			sc.setIntegerData(s.getIntData());
			sc.setEndTime(s.getEndTime());
			sc.setExpectedNextSampleTime(s.getExpectedNextSampleTime());
			sc.setStartTime(s.getStartTime());
			scCollection.add(sc);
//		public SegmentCustom(Segment.Type type, float sampleRate) {
		}
		this.segments = scCollection;
	}

	@Override
	public String toString() {
		return "TimeseriesCustom [networkCode=" + networkCode + ", stationCode=" + stationCode + ", location="
				+ location + ", channelCode=" + channelCode + ", channel=" + channel + ", dataQuality=" + dataQuality
				+ ", segments=" + segments + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((channel == null) ? 0 : channel.hashCode());
		result = prime * result + ((channelCode == null) ? 0 : channelCode.hashCode());
		result = prime * result + dataQuality;
		result = prime * result + ((location == null) ? 0 : location.hashCode());
		result = prime * result + ((networkCode == null) ? 0 : networkCode.hashCode());
		result = prime * result + ((segments == null) ? 0 : segments.hashCode());
		result = prime * result + ((stationCode == null) ? 0 : stationCode.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TimeseriesCustom other = (TimeseriesCustom) obj;
		if (channel == null) {
			if (other.channel != null)
				return false;
		} else if (!channel.equals(other.channel))
			return false;
		if (channelCode == null) {
			if (other.channelCode != null)
				return false;
		} else if (!channelCode.equals(other.channelCode))
			return false;
		if (dataQuality != other.dataQuality)
			return false;
		if (location == null) {
			if (other.location != null)
				return false;
		} else if (!location.equals(other.location))
			return false;
		if (networkCode == null) {
			if (other.networkCode != null)
				return false;
		} else if (!networkCode.equals(other.networkCode))
			return false;
		if (segments == null) {
			if (other.segments != null)
				return false;
		} else if (!segments.equals(other.segments))
			return false;
		if (stationCode == null) {
			if (other.stationCode != null)
				return false;
		} else if (!stationCode.equals(other.stationCode))
			return false;
		return true;
	}
	
	public class SegmentCustom extends java.lang.Object implements java.io.Serializable {//, Comparable<SegmentCustom> {
		
		public SegmentCustom() { }
		
		public SegmentCustom(Segment.Type type, float sampleRate) {
			this.type = type;
			this.sampleRate = sampleRate;
		}
		
		// =========================== ||
		private Segment.Type type;
		private float sampleRate;
		
		private List<Double> doubleData;
		private List<Float> floatData;
		private List<Integer> integerData;
		
		private Timestamp startTime;
		private Timestamp endTime;
		private Timestamp expectedNextSampleTime;
		// =========================== ||
/*
		@Override
		public int compareTo(SegmentCustom o) {
			// TODO Auto-generated method stub
			return 0;
		}
*/
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((doubleData == null) ? 0 : doubleData.hashCode());
			result = prime * result + ((endTime == null) ? 0 : endTime.hashCode());
			result = prime * result + ((expectedNextSampleTime == null) ? 0 : expectedNextSampleTime.hashCode());
			result = prime * result + ((floatData == null) ? 0 : floatData.hashCode());
			result = prime * result + ((integerData == null) ? 0 : integerData.hashCode());
			result = prime * result + Float.floatToIntBits(sampleRate);
			result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
			result = prime * result + ((type == null) ? 0 : type.hashCode());
			return result;
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
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (doubleData == null) {
				if (other.doubleData != null)
					return false;
			} else if (!doubleData.equals(other.doubleData))
				return false;
			if (endTime == null) {
				if (other.endTime != null)
					return false;
			} else if (!endTime.equals(other.endTime))
				return false;
			if (expectedNextSampleTime == null) {
				if (other.expectedNextSampleTime != null)
					return false;
			} else if (!expectedNextSampleTime.equals(other.expectedNextSampleTime))
				return false;
			if (floatData == null) {
				if (other.floatData != null)
					return false;
			} else if (!floatData.equals(other.floatData))
				return false;
			if (integerData == null) {
				if (other.integerData != null)
					return false;
			} else if (!integerData.equals(other.integerData))
				return false;
			if (Float.floatToIntBits(sampleRate) != Float.floatToIntBits(other.sampleRate))
				return false;
			if (startTime == null) {
				if (other.startTime != null)
					return false;
			} else if (!startTime.equals(other.startTime))
				return false;
			if (type != other.type)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "SegmentCustom [type=" + type + ", sampleRate=" + sampleRate + ", doubleData=" + doubleData
					+ ", floatData=" + floatData + ", integerData=" + integerData + ", startTime=" + startTime
					+ ", endTime=" + endTime + ", expectedNextSampleTime=" + expectedNextSampleTime + "]";
		}

		public Segment.Type getType() {
			return type;
		}

		public float getSampleRate() {
			return sampleRate;
		}

		public List<Double> getDoubleData() {
			return doubleData;
		}

		public List<Float> getFloatData() {
			return floatData;
		}

		public List<Integer> getIntegerData() {
			return integerData;
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

		public void setType(Segment.Type type) {
			this.type = type;
		}

		public void setSampleRate(float sampleRate) {
			this.sampleRate = sampleRate;
		}

		public void setDoubleData(List<Double> doubleData) {
			this.doubleData = doubleData;
		}

		public void setFloatData(List<Float> floatData) {
			this.floatData = floatData;
		}

		public void setIntegerData(List<Integer> integerData) {
			this.integerData = integerData;
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

		private TimeseriesCustom getOuterType() {
			return TimeseriesCustom.this;
		}
		
	}
	
}
