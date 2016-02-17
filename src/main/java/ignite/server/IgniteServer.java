package main.java.ignite.server;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
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
		// TODO: Add grid names to every single ignite instance
		try {
        //	IgniteConfiguration conf = new IgniteConfiguration();
		//	
        //	TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        //	commSpi.setLocalAddress("localhost");
        //	commSpi.setLocalPortRange(100);
        //	
        //	conf.setCommunicationSpi(commSpi);
        //	conf.setClientMode(false);
			Ignition.start();
		}
		catch (IgniteException e) {
			System.err.println("Failed to start Ignite server node");
			e.printStackTrace();
		}
	}
}