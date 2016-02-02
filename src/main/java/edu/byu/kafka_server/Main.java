package edu.byu.kafka_server;

import edu.byu.kafka_messaging.ConsumerKafka;
import edu.byu.kafka_messaging.ProducerKafka;
import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        ProducerKafka prod = new ProducerKafka();
        ConsumerKafka cons = new ConsumerKafka();
        try {
            cons.runKafkaConsumers();
            prod.runKafkaProducer();
        } catch (IOException e) {
            System.out.println("Could not run Kafka Producer OR Kafka Consumer");
            e.printStackTrace(System.err);
        }
    }
}
