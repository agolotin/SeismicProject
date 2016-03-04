package main.java.edu.byu.seismicproject.producer;

import java.util.concurrent.ExecutionException;

import main.java.edu.byu.seismicproject.signalprocessing.StreamProducer;
import main.java.edu.byu.seismicproject.signalprocessing.StreamSegment;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@SuppressWarnings({"rawtypes","unchecked"})
class ProducerRunnable implements Runnable {
	
	private final StreamProducer streamer;
	
	private final KafkaProducer producer;
	private final String topic;
	private final int partitionNum;
	
	
	public ProducerRunnable(StreamProducer streamer, KafkaProducer producer, String topic, int partitionNum) {
		this.streamer = streamer;
		this.producer = producer;
		this.topic = topic;
		this.partitionNum = partitionNum;
	}

	@Override
	public void run() {
		try {
			while (streamer.hasNext()) {
				ProducerRecord<String, StreamSegment> producerData = 
						new ProducerRecord<String, StreamSegment>(topic, partitionNum, null, streamer.getNext());
				this.producer.send(producerData).get(); // Right now let's block on a send...
			}
		} 
		catch (InterruptedException | ExecutionException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	
}