package main.java.general.timeseries;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.iris.dmc.fdsn.station.model.Channel;
import edu.iris.dmc.timeseries.model.Segment;


/* Google Kryo does not serialize the java.util.login.Logger that is present in both Timeseries and Segment classes
 * This class almost fully implements the functionality of the Timseries class defined by IRIS libraries
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
	
	private SegmentCustom segment;
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

	public SegmentCustom getSegment() {
		return segment;
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

	public void setSegment(Segment segment, List measurementsPerPartition) {
//		for (Segment s : segments) {
		SegmentCustom sc = new SegmentCustom(segment.getType(), segment.getSamplerate());
//			sc.setDoubleData(s.getDoubleData());
//			sc.setFloatData(s.getFloatData());
//			sc.setShortData(s.getShortData());
//			sc.setIntegerData(measurementsPerPartition);
		sc.setMainData(measurementsPerPartition); /// this is the main data
		sc.setSampleCount(segment.getSampleCount());
		sc.setType(segment.getType());
		sc.setEndTime(segment.getEndTime());
		sc.setExpectedNextSampleTime(segment.getExpectedNextSampleTime());
		sc.setStartTime(segment.getStartTime());
//		public SegmentCustom(Segment.Type type, float sampleRate) {
//		}
		this.segment = sc;
	}

	@Override
	public String toString() {
		return "TimeseriesCustom [networkCode=" + networkCode + ", stationCode=" + stationCode + ", location="
				+ location + ", channelCode=" + channelCode + ", channel=" + channel + ", dataQuality=" + dataQuality
				+ ", segment=" + segment + "]";
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
		result = prime * result + ((segment == null) ? 0 : segment.hashCode());
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
		if (segment == null) {
			if (other.segment != null)
				return false;
		} else if (!segment.equals(other.segment))
			return false;
		if (stationCode == null) {
			if (other.stationCode != null)
				return false;
		} else if (!stationCode.equals(other.stationCode))
			return false;
		return true;
	}
	
}
