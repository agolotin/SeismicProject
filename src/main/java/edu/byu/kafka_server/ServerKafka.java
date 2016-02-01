package edu.byu.kafka_server;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;

import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import kafka.utils.ZKStringSerializer$;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
 
 
@SuppressWarnings("unused")
public class ServerKafka {
	
 
	private static KafkaServerStartable kafka;
	private static ZooKeeperLocal zookeeper;
	
	
	public ServerKafka(Properties kafkaProperties, Properties zkProperties) throws IOException, InterruptedException{
		setUpZookeeper(zkProperties);
		setUpKafkaServer(kafkaProperties);
	}
	
	public static void setUpZookeeper(Properties zkProp) throws FileNotFoundException, IOException {
		// Start local Zookeeper
		System.out.println("Starting local zookeeper...");
		zookeeper = new ZooKeeperLocal(zkProp);
		System.out.println("Done");
	}
	
	public static void setUpKafkaServer(Properties kafkaProp) {
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProp);
		// Start local kafka broker
		kafka = new KafkaServerStartable(kafkaConfig);
		System.out.println("Starting local kafka broker...");
		kafka.startup();
		System.out.println("Done");
	}
	
	
	public void stop() {
		// Stop kafka broker
		System.out.println("Stopping kafka...");
		kafka.shutdown();
		System.out.println("Done");

		/*
		System.out.println("Stopping zookeeper...");
		zookeeper.stop();
		System.out.println("Done");
		*/
		
		kafka = null;
		zookeeper = null;
	}
	
	public static class ZooKeeperLocal {
		
		private ZooKeeperServerMain zkServer;
		//private Thread zkRunnable;
		
		// Class instance is invoked in kafka_server.KafkaServerLocal
		public ZooKeeperLocal(Properties zkProperties) throws FileNotFoundException, IOException{
			QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
			try {
				quorumConfiguration.parseProperties(zkProperties);
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
	 
			zkServer = new ZooKeeperServerMain();
			final ServerConfig configuration = new ServerConfig();
			configuration.readFrom(quorumConfiguration);
			
			new Thread() {
				public void run() {
					try {
						zkServer.runFromConfig(configuration);
					} catch (IOException e) {
						System.out.println("ZooKeeper start failed");
						e.printStackTrace(System.err);
					}
				}
			   
			}.start();
		}
		
		/*
		public void stop() {
			try {
				Method shutdown = ZooKeeperServerMain.class.getDeclaredMethod("shutdown");
				shutdown.setAccessible(true);
				shutdown.invoke(zkRunnable);
				System.out.println("ZooKeeper server stopped successfully");
				
			} catch (Exception e) {
				System.out.println("ZooKeeper stop failed");
				e.printStackTrace(System.err);
			}
		}

		public void createZKTopic(String topicName) {
			// Create a ZooKeeper client
			int sessionTimeoutMs = 10000;
			int connectionTimeoutMs = 10000;
			
			ZkClient zkClient = new ZkClient("127.0.0.1:2181", sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
			ZkUtils zkUtils = new ZkUtils(zkClient, null, false);
			
			// Create a topic named "whatever_name" with 1 partition and a replication factor of 1
			int numPartitions = 1;
			int replicationFactor = 1;
			AdminUtils.createTopic(zkUtils, topicName, numPartitions, replicationFactor, new Properties());
		}
		*/
	}
}