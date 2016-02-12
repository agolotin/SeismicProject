package main.java.streaming.ignite.server;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

public class IgniteServer 
{
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
        //	
			Ignition.start();
		}
		catch (IgniteException e) {
			System.err.println("Failed to start Ignite server node");
			e.printStackTrace();
		}
	}
}