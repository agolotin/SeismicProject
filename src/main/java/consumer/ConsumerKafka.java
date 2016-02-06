package main.java.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ConsumerKafka implements Runnable{

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
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
    }
    
	@Override
    public void run() {
        System.out.println("Kafka Consumer starting...");
        
        // log4j writes to stdout for now
        org.apache.log4j.BasicConfigurator.configure();

        try {
        	consumer.subscribe(Arrays.asList(topic));
        
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("offset = %d, value = %s", record.offset(), record.value());
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
