package main.java.serialization;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Map;
import java.util.zip.InflaterInputStream;

import main.java.timeseries.TimeseriesCustom;
import main.java.timeseries.SegmentCustom;

import org.apache.kafka.common.serialization.Deserializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

public class TimeseriesDecoder implements Deserializer<TimeseriesCustom>{
	
	public TimeseriesDecoder() { }
	
	@Override
	public TimeseriesCustom deserialize(String arg0, byte[] ts) {
		if (ts == null) return null;
		
		Kryo kryo = new Kryo();
		kryo.register(TimeseriesCustom.class, new JavaSerializer());
		kryo.register(SegmentCustom.class, new JavaSerializer());
		kryo.register(Collection.class, new JavaSerializer());
		
		ByteArrayInputStream in = new ByteArrayInputStream(ts);
		InflaterInputStream in_stream = new InflaterInputStream(in);
		
		Input input = new Input(in_stream);
		Object obj = new Object();
		TimeseriesCustom timeseries = new TimeseriesCustom();
		
		try {
			obj = kryo.readObject(input, TimeseriesCustom.class);
			timeseries = (TimeseriesCustom) obj;
		}
		catch (Exception e){
			e.printStackTrace();
		}
		
		return timeseries;
	}

	@Override
	public void close() { }

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) { }
}
