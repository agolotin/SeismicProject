package main.java.edu.byu.seismicproject.producer;

import main.java.edu.byu.seismicproject.signalprocessing.IStreamProducer;
import main.java.edu.byu.seismicproject.signalprocessing.StreamSegment;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@SuppressWarnings({"rawtypes","unchecked"})
class ProducerRunnable implements Runnable {
	
	private final IStreamProducer streamer;
	
	private final KafkaProducer producer;
	private final String topic;
	private final int partitionNum;
	
	
	public ProducerRunnable(IStreamProducer streamer, KafkaProducer producer, String topic, int partitionNum) {
		this.streamer = streamer;
		this.producer = producer;
		this.topic = topic;
		this.partitionNum = partitionNum;
	}

	@Override
	public void run() {
		try {
			while (streamer.hasNext()) {
				// TODO: We need to block here before we send depending on the size of our cache and stuff...
				ProducerRecord<String, StreamSegment> producerData = 
						new ProducerRecord<>(topic, partitionNum, null, streamer.getNext());
				this.producer.send(producerData); 
			}
		} 
		catch (Exception e) {
            Logger.getLogger(ProducerKafka.class.getName()).log(Level.SEVERE, null, e);
			Thread.currentThread().interrupt();
		}
	}
	
	
}