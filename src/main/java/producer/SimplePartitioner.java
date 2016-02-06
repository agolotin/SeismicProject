package main.java.producer;


import java.util.Collection;
import java.util.Iterator;

import edu.iris.dmc.timeseries.model.Segment;
import edu.iris.dmc.timeseries.model.Timeseries;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner {//implements Partitioner {

    public SimplePartitioner() { }
	
    public SimplePartitioner(VerifiableProperties props) { }
    
    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        //Timeseries timeseries = (Timeseries) key;
        //Iterator<Segment> iter_segments = timeseries.getSegments().iterator();
        //Segment segment = iter_segments.next();
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
           partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
        }
       return partition;
  }
}