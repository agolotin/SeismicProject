package main.java.edu.byu.seismicproject.general.timeseries;

import edu.iris.dmc.fdsn.station.model.Channel;
import edu.iris.dmc.timeseries.model.Segment;

/**
 * The reason we have to create our own Timeseries and Segment classes is because Google Kryo (or anything)
 * does not serialize the java.util.login.Logger that is present in both Timeseries and Segment classes.
 * This class almost fully implements the functionality of the Timseries class defined by IRIS library in order to 
 * allow us to control serialization of the logger.
 */
@SuppressWarnings("serial")
public class TimeseriesCustom extends java.lang.Object implements java.io.Serializable {

	public TimeseriesCustom() {
	}

	/**
	 * Constructor with the necessary fields
	 * 
	 * @param networkCode
	 * @param stationCode
	 * @param location
	 * @param channelCode
	 */
	public TimeseriesCustom(String networkCode, String stationCode, String location, String channelCode) {
		this.networkCode = networkCode;
		this.stationCode = stationCode;
		this.location = location;
		this.channelCode = channelCode;
	}

	// ================================== ||
	/*
	 * Code for the network of interest
	 */
	private String networkCode;
	/*
	 * Code for the station whose data we are streaming
	 */
	private String stationCode;

	/*
	 * Name of the location we are streaming
	 */
	private String location;

	/*
	 * Code for the channel we are streaming
	 */
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

	/*
	 * sets the segment for this timeseries
	 * 
	 * @param segment
	 * 
	 * @param measurementsPerPartition
	 */
	public void setSegment(float[] measurementsPerPartition, int numSamples, Segment.Type type, 
			float samplerate, long startTime, long endTime) {
		SegmentCustom sc = new SegmentCustom(type, samplerate);
		
		sc.setMainData(measurementsPerPartition); /// this is the main data
		sc.setSampleCount(numSamples);
		sc.setType(type);
		sc.setEndTime(endTime);
		//sc.setExpectedNextSampleTime(segment.getExpectedNextSampleTime());
		sc.setStartTime(startTime);
		
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
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		TimeseriesCustom other = (TimeseriesCustom) obj;
		if (channel == null) {
			if (other.channel != null) {
				return false;
			}
		}
		else {
			if (!channel.equals(other.channel)) {
				return false;
			}
		}
		if (channelCode == null) {
			if (other.channelCode != null) {
				return false;
			}
		}
		else {
			if (!channelCode.equals(other.channelCode)) {
				return false;
			}
		}
		if (dataQuality != other.dataQuality) {
			return false;
		}
		if (location == null) {
			if (other.location != null) {
				return false;
			}
		}
		else
			if (!location.equals(other.location)) {
				return false;
			}
		if (networkCode == null) {
			if (other.networkCode != null) {
				return false;
			}
		}
		else
			if (!networkCode.equals(other.networkCode)) {
				return false;
			}
		if (segment == null) {
			if (other.segment != null) {
				return false;
			}
		}
		else {
			if (!segment.equals(other.segment)) {
				return false;
			}
		}
		if (stationCode == null) {
			if (other.stationCode != null) {
				return false;
			}
		}
		else {
			if (!stationCode.equals(other.stationCode)) {
				return false;
			}
		}
		return true;
	}

}
