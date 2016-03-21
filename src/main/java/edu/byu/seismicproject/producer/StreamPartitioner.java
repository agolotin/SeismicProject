package main.java.edu.byu.seismicproject.producer;

import java.util.Map;

import main.java.edu.byu.seismicproject.signalprocessing.StreamSegment;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class StreamPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) { }

	@Override
	public void close() { }

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, 
			Object value, byte[] valueBytes, Cluster clusterMetadata) {
		
		int bandNum = ((StreamSegment) value).getId().getBand().getBandNum();
		int numPartitions = clusterMetadata.partitionCountForTopic(topic);
		
		return bandNum % numPartitions;
	}
	
}