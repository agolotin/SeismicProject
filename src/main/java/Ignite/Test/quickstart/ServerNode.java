package main.java.Ignite.Test.quickstart;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

public class ServerNode 
{
	public static void main(String[] args) throws IgniteException 
	{
		Ignition.start();
	}
}