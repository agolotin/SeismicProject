package main.java.streaming.ignite.server;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

public class ServerNode 
{
	public static void main(String[] args) 
	{
		try {
			Ignition.start();
		}
		catch (IgniteException e) {
			System.err.println("Failed to start Ignite server node");
			e.printStackTrace();
		}
	}
}