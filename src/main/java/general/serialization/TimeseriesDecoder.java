package main.java.general.serialization;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.zip.InflaterInputStream;

import org.apache.kafka.common.serialization.Deserializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import main.java.general.timeseries.SegmentCustom;
import main.java.general.timeseries.TimeseriesCustom;

/*
 * In order to use the TimeseriesCustom class to pass data from the ProducerKafka
 * to the ConsumerKafka, we need a custom Serializer and a custom Deserializer.
 * This TimeseriesDecoder is the custom Deserializer, used to deserialize timeseries 
 * data as it is received by the ConsumerKafka 
 */
public class TimeseriesDecoder implements Deserializer<TimeseriesCustom>{
	
	public TimeseriesDecoder() { }
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.kafka.common.serialization.Deserializer#deserialize(java.lang.String, byte[])
	 * @param arg0
	 * @param ts
	 */
	@Override
	public TimeseriesCustom deserialize(String arg0, byte[] ts) {
		if (ts == null) {
			return null;
		}
		
		Kryo kryo = new Kryo();
		kryo.register(TimeseriesCustom.class, new JavaSerializer());
		kryo.register(SegmentCustom.class, new JavaSerializer());
		
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
