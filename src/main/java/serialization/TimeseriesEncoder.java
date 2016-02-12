package main.java.serialization;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;

import main.java.timeseries.TimeseriesCustom;
import main.java.timeseries.SegmentCustom;

import org.apache.kafka.common.serialization.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

public class TimeseriesEncoder implements Serializer<TimeseriesCustom> {
	
	public TimeseriesEncoder() { }
	
	@Override
	public byte[] serialize(String arg0, TimeseriesCustom ts) {
		if (ts == null) return null;
		
		Kryo kryo = new Kryo();
		kryo.register(TimeseriesCustom.class, new JavaSerializer());
		kryo.register(SegmentCustom.class, new JavaSerializer());
		kryo.register(Collection.class, new JavaSerializer());
		

		ByteArrayOutputStream out = new ByteArrayOutputStream(16384);
		DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(out);
		
		Output output = new Output(deflaterOutputStream);
		kryo.writeObject(output, ts);
		output.close();
		
		return out.toByteArray();
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) { }

	@Override
	public void close() { }
}
