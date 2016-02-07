package main.java.consumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import main.java.timeseries.TimeseriesCustom;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import edu.iris.dmc.timeseries.model.Timeseries;

public class ConsumerKafka implements Runnable {

	private KafkaConsumer consumer;
    private String topic;
    private int tid;

    public ConsumerKafka(int tid, String group_id, String topic) {
    	this.tid = tid;
    	this.topic = topic;

        // Set up the consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", group_id);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "main.java.producer.decoder.TimeseriesDecoder");
        props.put("value.deserializer", "main.java.producer.decoder.TimeseriesDecoder");
        
        consumer = new KafkaConsumer<>(props);
    }
    
	@Override
    public void run() {
        // log4j writes to stdout for now
        org.apache.log4j.BasicConfigurator.configure();

        try {
        	consumer.subscribe(Arrays.asList(topic));
        
			while (true) {
				ConsumerRecords<String, TimeseriesCustom> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord record : records) {
					TimeseriesCustom timeseries = (TimeseriesCustom) record.value();
					// This is what has to be integrated with Ignite....
					
					System.out.println("offset = " + record.offset());
					System.out.println(timeseries.toString());
					System.out.println("Size of segments: " + timeseries.getSegments().size());
					System.out.println("Size of intData in segments: " + timeseries.getSegments().iterator().next().getIntegerData().size());
				}
			}
        }
        catch(WakeupException e) {
        	// ignore
        }
        finally {
        	consumer.close();
        }
    }
    
    public void shutdown() {
        consumer.wakeup();
    }
}
