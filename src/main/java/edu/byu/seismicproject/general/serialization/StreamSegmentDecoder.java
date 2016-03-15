package edu.byu.seismicproject.general.serialization;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.zip.InflaterInputStream;

import org.apache.kafka.common.serialization.Deserializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import edu.byu.seismicproject.signalprocessing.StreamSegment;


/**
 * In order to use the TimeseriesCustom class to pass data from the ProducerKafka
 * to the ConsumerKafka, we need a custom Serializer and a custom Deserializer.
 * This TimeseriesDecoder is the custom Deserializer, used to deserialize timeseries 
 * data as it is received by the ConsumerKafka 
 */
public class StreamSegmentDecoder implements Deserializer<StreamSegment>{
	
	public StreamSegmentDecoder() { }
	
	/**
	 * @see org.apache.kafka.common.serialization.Deserializer#deserialize(java.lang.String, byte[])
	 * @param arg0
	 * @param ts
	 */
	@Override
	public StreamSegment deserialize(String arg0, byte[] streamSeg) {
		if (streamSeg == null) {
			return null;
		}
		
		Kryo kryo = new Kryo();
		kryo.register(StreamSegment.class, new JavaSerializer());
		
		ByteArrayInputStream in = new ByteArrayInputStream(streamSeg);
		InflaterInputStream in_stream = new InflaterInputStream(in);
		
		Input input = new Input(in_stream);
		Object obj = new Object();
		StreamSegment streamSegment = null;
		
		try {
			obj = kryo.readObject(input, StreamSegment.class);
			streamSegment = (StreamSegment) obj;
		}
		catch (Exception e){
			e.printStackTrace();
		}
		
		return streamSegment;
	}

	@Override
	public void close() { }

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) { }
}
