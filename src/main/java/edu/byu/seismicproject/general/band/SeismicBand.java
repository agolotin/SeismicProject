package main.java.edu.byu.seismicproject.general.band;

import java.io.Serializable;


@SuppressWarnings("serial")
public class SeismicBand implements Serializable {
	
	private final String name;
	private final int order;
	private final double lowCorner;
	private final double highCorner;
	
	public SeismicBand(int bandNum, int order, double lowCorner, double highCorner) {
		this.name = "band" + bandNum;
		this.order = order;
		this.lowCorner = lowCorner;
		this.highCorner = highCorner;
	}
	
	public String getName() {
		return name;
	}

	public int getOrder() {
		return order;
	}

	public double getLowCorner() {
		return lowCorner;
	}

	public double getHighCorner() {
		return highCorner;
	}

	@Override
	public String toString() {
		return "SeismicBand [name=" + name + ", order=" + order + ", lowCorner=" + lowCorner + ", highCorner="
				+ highCorner + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(highCorner);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(lowCorner);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + order;
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
		SeismicBand other = (SeismicBand) obj;
		if (Double.doubleToLongBits(highCorner) != Double.doubleToLongBits(other.highCorner))
			return false;
		if (Double.doubleToLongBits(lowCorner) != Double.doubleToLongBits(other.lowCorner))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (order != other.order)
			return false;
		return true;
	}


}