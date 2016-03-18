package main.java.edu.byu.seismicproject.general.serialization;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;

import org.apache.kafka.common.serialization.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import main.java.edu.byu.seismicproject.signalprocessing.StreamSegment;


public class StreamSegmentEncoder implements Serializer<StreamSegment> {
	
	public StreamSegmentEncoder() { }
	
	/**
	 * @see org.apache.kafka.common.serialization.Serializer#serialize(java.lang.String, java.lang.Object)
	 * @param arg0
	 * @param ts
	 */
	@Override
	public byte[] serialize(String arg0, StreamSegment streamSeg) {
		if (streamSeg == null) {
			return null;
		}
		
		Kryo kryo = new Kryo();
		kryo.register(StreamSegment.class, new JavaSerializer());

		ByteArrayOutputStream out = new ByteArrayOutputStream(16384);
		DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(out);
		
		Output output = new Output(deflaterOutputStream);
		kryo.writeObject(output, streamSeg);
		output.close();
		
		return out.toByteArray();
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) { }

	@Override
	public void close() { }
}