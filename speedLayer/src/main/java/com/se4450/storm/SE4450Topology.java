package com.se4450.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;



public class SE4450Topology {

	public static String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
	public static String HBASE_CONFIGURATION_HBASE_MASTER = "hbase.master";
	
	// Entry point for the topology
	public static void main(String[] args) throws Exception {
		// Used to build the topology
		TopologyBuilder builder = new TopologyBuilder();

		// Set up the kafka Spout
		BrokerHosts hosts = new ZkHosts("129.100.224.244:9092");	    
	    
	    SpoutConfig spoutConfig = new SpoutConfig(hosts, "sensorData", "/"+ "sensorData", UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		// Add the spout, with a name of 'spout'
		// and parallelism hint of 5 executors
		builder.setSpout("spout", kafkaSpout, 1);

		// Add the SplitSentence bolt, with a name of 'split'
		// and parallelism hint of 8 executors
		// shufflegrouping subscribes to the spout, and equally distributes
		// tuples (sentences) across instances of the SplitSentence bolt
		builder.setBolt("singleSensorBolt",
				new FormatSingleSensorReadingBolt(), 5)
				.shuffleGrouping("spout");

		// new configuration
		Config conf = new Config();
		conf.setDebug(true);
			
		// hbase configuration
		Map<String, Object> hbConf = new HashMap<String, Object>();
		if (args.length > 0) {
			hbConf.put("hbase.rootdir", args[0]);
		}
		conf.put("hbase.conf", hbConf);

		// Create the hbase mapper
		SimpleHBaseMapper mapper = new SimpleHBaseMapper()
				.withRowKeyField("key").withCounterFields(new Fields("value"))
				.withColumnFamily("d");

		// Create the hbase bolt with some config sutff
		HBaseBolt hbase = new HBaseBolt("SensorValuesSpeedLayer", mapper)
				.withConfigKey("hbase.conf");

		// Add it to the topology, listening for single readings
		builder.setBolt("HBase", hbase, 10)
				.fieldsGrouping("singleSensorBolt", "hbasestream",
						new Fields("key", "value")).setNumTasks(10);
		

		// If there are arguments, we are running on a cluster
		if (args != null && args.length > 0) {
			// parallelism hint to set the number of workers
			conf.setNumWorkers(3);
			// submit the topology
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		}
		// Otherwise, we are running locally
		else {
			// Cap the maximum number of executors that can be spawned
			// for a component to 3
			conf.setMaxTaskParallelism(3);
			// LocalCluster is used to run locally
			LocalCluster cluster = new LocalCluster();
			// submit the topology
			cluster.submitTopology("speedLayer", conf, builder.createTopology());
			// sleep
			Thread.sleep(10000);
			// shut down the cluster
			cluster.shutdown();
		}
	}
}