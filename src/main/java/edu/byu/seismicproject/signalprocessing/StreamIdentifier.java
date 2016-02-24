package main.java.edu.byu.seismicproject.signalprocessing;


public class StreamIdentifier {

	private final String net;
    private final String sta;
    private final String chan;
    private final String location;

    public StreamIdentifier(String net, String sta, String chan, String location) {
        this.net = net;
        this.sta = sta;
        this.chan = chan;
        this.location = location;
    }

	public String getNet() {
		return net;
	}

	public String getSta() {
		return sta;
	}

	public String getChan() {
		return chan;
	}

	public String getLocation() {
		return location;
	}

	@Override
	public String toString() {
		return "StreamIdentifier [net=" + net + ", sta=" + sta + ", chan=" + chan + ", location=" + location + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((chan == null) ? 0 : chan.hashCode());
		result = prime * result + ((location == null) ? 0 : location.hashCode());
		result = prime * result + ((net == null) ? 0 : net.hashCode());
		result = prime * result + ((sta == null) ? 0 : sta.hashCode());
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
		StreamIdentifier other = (StreamIdentifier) obj;
		if (chan == null) {
			if (other.chan != null)
				return false;
		} else if (!chan.equals(other.chan))
			return false;
		if (location == null) {
			if (other.location != null)
				return false;
		} else if (!location.equals(other.location))
			return false;
		if (net == null) {
			if (other.net != null)
				return false;
		} else if (!net.equals(other.net))
			return false;
		if (sta == null) {
			if (other.sta != null)
				return false;
		} else if (!sta.equals(other.sta))
			return false;
		return true;
	}

}