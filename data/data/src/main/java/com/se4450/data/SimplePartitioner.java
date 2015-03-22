package com.se4450.data;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
 
public class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) {
 
    }
 
    public int partition(Object key, int a_numPartitions) {
    	Random rnd = new Random();
    	return rnd.nextInt(a_numPartitions);
  }
 
}