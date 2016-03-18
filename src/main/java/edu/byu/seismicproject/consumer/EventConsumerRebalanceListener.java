package main.java.edu.byu.seismicproject.consumer;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/*
 * REVIEWME: This class is useless right now
 */

@SuppressWarnings("rawtypes")
public class EventConsumerRebalanceListener implements ConsumerRebalanceListener {
	
	private OffsetManager offsetManager;
	private Consumer consumer;
	
	public EventConsumerRebalanceListener(Consumer consumer, String fileStorageName) {
		this.consumer = consumer;
		this.offsetManager = new OffsetManager(fileStorageName);
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, offsetManager.readOffsetFromExternalStore(partition.topic(), partition.partition()));
        }
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsetManager.saveOffsetInExternalStore(partition.topic(), partition.partition(), consumer.position(partition));
        }
	}
	
}