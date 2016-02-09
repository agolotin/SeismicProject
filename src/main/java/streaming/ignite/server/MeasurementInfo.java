package main.java.streaming.ignite.server;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class MeasurementInfo implements java.io.Serializable {

	@QuerySqlField
	private int tid;
	
	@QuerySqlField
	private Integer windowNum;
	
	@QuerySqlField
	private Integer measurement;
	
	public MeasurementInfo() { }
	
	public MeasurementInfo(int tid, Integer windowNum, Integer measurement) {
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

	public Integer getMeasurement() {
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
