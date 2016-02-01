package edu.byu.kafka_messaging;

//import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import kafka.common.TopicExistsException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ProducerKafka {

//	private ProducerConfig config;
    private KafkaProducer<String, String> producer;

    public ProducerKafka() {
    }

    public static void main(String[] args) {

        ProducerKafka prod = new ProducerKafka();
        try {
            prod.defineKafkaProducer();
            prod.runKafkaProducer();
        } catch (TopicExistsException | IOException e) {
            e.printStackTrace();
        }

    }

    public void defineKafkaProducer() throws IOException {
        // Define producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Define actual producer
        // first String is partition key, and the second is the type of the message
        this.producer = new KafkaProducer<>(props);
    }

    public void runKafkaProducer() {
        System.out.println("Kafka Producer starting...");
        // log4j writes to stdout for now
        org.apache.log4j.BasicConfigurator.configure();

        try {
            this.defineKafkaProducer();
        } catch (IOException e) {
            System.out.println("Could not define Kafka Producer settings");
            e.printStackTrace();
        }

        // This is a fake message
        this.producer.send(new ProducerRecord<>("test", "key", "value"));
        this.producer.close();
    }

}
