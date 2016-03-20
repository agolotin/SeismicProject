package main.java.edu.byu.seismicproject.producer;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

public class KafkaTopics {
	
	private ZkUtils zkUtils = null;;
	
	public KafkaTopics(String zkHost) {
		setupZookeeperClient(zkHost);
	}

	public void setupZookeeperClient(String zkHost) {
		if (zkUtils == null) {
			ZkClient zkClient = ZkUtils.createZkClient(zkHost, 3000, 3000);
			ZkConnection conn = new ZkConnection(zkHost);
			zkUtils = new ZkUtils(zkClient, conn, false);
		}
	}
	
	public void createNewTopic(String topic, int partitions, int replication_factor) {
		
		if (zkUtils == null) {
			throw new IllegalStateException("Initialize the Zookeeper client first");
		}
		
    	if (!AdminUtils.topicExists(zkUtils, topic)) {
			AdminUtils.createTopic(zkUtils, topic, partitions, replication_factor, new Properties());
    	}
	}
	
	public void close() {
		if (zkUtils != null) {
			zkUtils.close();
			zkUtils = null;
		}
	}
	
	public ZkUtils getZkUtils() {
		return zkUtils;
	}
}