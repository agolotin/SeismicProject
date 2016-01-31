package kafka_messaging;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
 
public class ConsumerRunnable implements Runnable {

	private KafkaStream m_stream;
    private int m_threadNumber;
 
	public ConsumerRunnable(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }
 
	public void run() {
		System.out.println("Starting a separate thread for a consumer");
		
        ConsumerIterator<byte[], byte[]> it = this.m_stream.iterator();
		System.out.println(it.size());
		
        while (it.hasNext())
            System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
