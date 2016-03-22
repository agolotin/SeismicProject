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
		
		/* Technically, such easy of a logic does not require us to have a separate
		 * class for our partitioner. This class is implemented for our convenience 
		 * later when we might decide to have less partitions than we have bands.
		 * With this logic, however, the number of partitions should never exceed 
		 * the number of bands we have, because in that case we will have partitions 
		 * that do not have any data associated with them
		 */
		
		int bandNum = ((StreamSegment) value).getId().getBand().getBandNum();
		int numPartitions = clusterMetadata.partitionCountForTopic(topic);
		
		return bandNum % numPartitions;
	}
	
}