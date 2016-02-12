package main.java.producer;


import java.util.Collection;
import java.util.Iterator;

import main.java.timeseries.TimeseriesCustom;
import edu.iris.dmc.timeseries.model.Segment;
import edu.iris.dmc.timeseries.model.Timeseries;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class KafkaPartitioner implements Partitioner {

    public KafkaPartitioner() { }
	
    public KafkaPartitioner(VerifiableProperties props) { }
    
    public int partition(Object key, int a_numPartitions) {
    	TimeseriesCustom ts = (TimeseriesCustom) key;
    	System.out.printf("[INFO] This chunk is going to partition %d\n", ts.getPartitionNum());
    	// return the number of the partition the message goes to
    	return ts.getPartitionNum();
  }
}