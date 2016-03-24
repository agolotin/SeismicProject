package main.java.edu.byu.seismicproject.ignite.server;

//import java.io.File;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.support.ClassPathXmlApplicationContext;
//TODO: Remove commented code if unneeded
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;


/**
 * This is the Ignite server where data caches are stored. In a fully scaled build of the Seismic Grouper,
 * there will be numerous Ignite servers communicating with multiple incoming streams (from KafkaConsumers). 
 */
public class IgniteServer 
{
	/**
	 * Entry point for IgniteServer. 
	 * Will eventually require grid names as args, currently no args are required. 
	 */
	public static void main(String[] args) 
	{
		//ApplicationContext context = new ClassPathXmlApplicationContext("ignite.xml");
		//IgniteConfiguration conf = (IgniteConfiguration) context.getBean("igniteConfig");
		
		try {
        //	IgniteConfiguration conf = new IgniteConfiguration();
		//	
        //	TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        //	commSpi.setLocalAddress("localhost");
        //	commSpi.setLocalPortRange(100);
        //	
        //	conf.setCommunicationSpi(commSpi);
			Ignition.start();
		}
		catch (IgniteException e) {
			System.err.println("Failed to start Ignite server node");
			e.printStackTrace();
		}
	}
}