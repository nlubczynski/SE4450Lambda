package com.se4450.storm;

import java.util.HashMap;
import java.util.Map;

import com.se4450.shared.Consts;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SE4450Topology {

	// Entry point for the topology
	public static void main(String[] args) throws Exception {

		// Create configuration
		Config conf = new Config();
		conf.setDebug(true);

		// hbase configuration
		Map<String, Object> hbConf = new HashMap<String, Object>();
		if (args.length > 0) {
			hbConf.put("hbase.rootdir", args[0]);
		}
		conf.put("hbase.conf", hbConf);

		// Create builder
		TopologyBuilder builder = new TopologyBuilder();

		// Add Kafka spout
		// outputs <string> to "incomingData"
		builder.setSpout("spout", StormFactory.getKafkaSpout(),
				Consts.KAFKA_SPOUT_PARALLELISM);

		// Add FormatBolt to split/format incoming Kafka stream
		// reads from "incomingData"
		// outputs <int:sensorID, int:sensorValue, DateTime:timestamp> to "formattedData"
		builder.setBolt("formattedData", new ParseBolt(),
				Consts.FORMAT_BOLT_PARALLELISM).shuffleGrouping("incomingData");		
		

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