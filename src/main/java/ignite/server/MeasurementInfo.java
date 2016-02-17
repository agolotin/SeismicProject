package main.java.ignite.server;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Object for wrapping measurement info so it can be put into cache.
 */
@SuppressWarnings("serial")
public class MeasurementInfo implements java.io.Serializable {

	/**
	 * Thread ID, used for SQL queries from the cache
	 */
	@QuerySqlField
	private int tid;
	
	/**
	 * Window number, used with tid for SQL queries from the cache
	 */
	@QuerySqlField
	private Integer windowNum;
	
	/**
	 * Measurement, the real data from IRIS queries
	 */
	@QuerySqlField
	private Object measurement;
	
	/**
	 * Default constructor
	 */
	public MeasurementInfo() { }
	
	/**
	 * Constructor with fields for the thread ID, window number, and 
	 * measurement of data from the incoming stream. The measurement data type 
	 * is not always consistent, so an Object is used for storage
	 * @param tid is the consumer ID data has to go to
	 * @param windowNum
	 * @param measurement actual measurement
	 */
	public MeasurementInfo(int tid, Integer windowNum, Object measurement) {
		this.tid = tid;
		this.windowNum = windowNum;
		this.measurement = measurement;
	}
	
	public int getTid() {
		return tid;
	}

	public Integer getWindowNum() {
		return windowNum;
	}

	public Object getMeasurement() {
		return measurement;
	}

	public void setTid(int tid) {
		this.tid = tid;
	}

	public void setWindowNum(Integer windowNum) {
		this.windowNum = windowNum;
	}

	public void setMeasurement(Integer measurement) {
		this.measurement = measurement;
	}
	
	@Override
	public String toString() {
		return "MeasurementInfo [tid=" + tid + ", windowNum=" + windowNum + ", measurement=" + measurement + "]";
	}

}
