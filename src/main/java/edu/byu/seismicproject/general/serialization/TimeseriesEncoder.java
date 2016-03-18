package main.java.edu.byu.seismicproject.general.serialization;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;

import org.apache.kafka.common.serialization.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import main.java.edu.byu.seismicproject.general.timeseries.SegmentCustom;
import main.java.edu.byu.seismicproject.general.timeseries.TimeseriesCustom;

/*
 * REVIEWME: This class is useless right now
 */
/**
 * In order to use the TimeseriesCustom class to pass data from the ProducerKafka
 * to the ConsumerKafka, we need a custom Serializer and a custom Deserializer.
 * This TimeseriesEncoder is the custom Serializer, used to Serialize timeseries 
 * data before sending it from the ProducerKafka to the ConsumerKafka 
 */
public class TimeseriesEncoder implements Serializer<TimeseriesCustom> {
	
	public TimeseriesEncoder() { }
	
	/**
	 * @see org.apache.kafka.common.serialization.Serializer#serialize(java.lang.String, java.lang.Object)
	 * @param arg0
	 * @param ts
	 */
	@Override
	public byte[] serialize(String arg0, TimeseriesCustom ts) {
		if (ts == null) {
			return null;
		}
		
		Kryo kryo = new Kryo();
		kryo.register(TimeseriesCustom.class, new JavaSerializer());
		kryo.register(SegmentCustom.class, new JavaSerializer());
		

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
