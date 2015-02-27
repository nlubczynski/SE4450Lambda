package com.se4450.storm;

import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;

import com.se4450.shared.Consts;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class SE4450Topology {

	public static final String KAFKA_SPOUT = "incomingData";

	public static final String PARSING_BOLT = "parsedData";
	public static final String PARSING_BOLT_STREAM = "parsedStream";
	public static final String PARSING_BOLT_ID = "sensorID";
	public static final String PARSING_BOLT_VALUE = "sensorValue";
	public static final String PARSING_BOLT_TIME = "sensorTimestamp";

	public static final String FORMAT_SENSOR_TO_HBASE_BOLT = "formatSensorToHbase";
	public static final String FORMAT_SENSOR_TO_HBASE_BOLT_STREAM = "formatSensorToHbaseStream";
	public static final String FORMAT_SENSOR_TO_HBASE_BOLT_KEY = "formatSensorToHbaseKey";
	public static final String FORMAT_SENSOR_TO_HBASE_BOLT_VALUE = "formatSensorToHbaseValue";

	public static final String HBASE_SENSOR_BOLT = "hbaseSensor";

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

		// Add serialization for DateTime
		conf.registerSerialization(DateTime.class);

		// If there are arguments, we are running on a cluster
		if (args != null && args.length > 0) {
			// parallelism hint to set the number of workers
			conf.setNumWorkers(Consts.STORM_NUMBER_OF_WORKERS);
			// submit the topology
			StormSubmitter.submitTopology(args[0], conf, createTopology());
		}
		// Otherwise, we are running locally
		else {
			// Cap the maximum number of executors that can be spawned
			// for a component to 3
			conf.setMaxTaskParallelism(3);
			// LocalCluster is used to run locally
			LocalCluster cluster = new LocalCluster();
			// submit the topology
			cluster.submitTopology("speedLayer", conf, createTopology());
			// sleep
			Thread.sleep(10000);
			// shut down the cluster
			cluster.shutdown();
		}
	}

	public static StormTopology createTopology() {
		// Create builder
		TopologyBuilder builder = new TopologyBuilder();

		// Add Kafka spout
		// outputs raw strings from the Kafka queue to KAFKA_SPOUT
		builder.setSpout(KAFKA_SPOUT, StormFactory.getKafkaSpout(),
				Consts.STOME_KAFKA_SPOUT_PARALLELISM);

		// Add ParseBolt to split/format incoming Kafka stream
		// reads from KAFKA_SPOUT
		// outputs parsed data in the form (int:sensorID, int:sensorValue,
		// DateTime:timestamp) to PARSING_BOLT
		builder.setBolt(PARSING_BOLT, new ParseBolt(),
				Consts.STORM_FORMAT_BOLT_PARALLELISM).shuffleGrouping(
				KAFKA_SPOUT);

		// Add SenstorToHBase to format sensor data for HBase
		// reads from PARSING_BOLT : PARSING_BOLT_STREAM
		// outputs parsed data in the form (string:key, string:value) to
		// FORMAT_SENSOR_TO_HBASE_BOLT
		builder.setBolt(FORMAT_SENSOR_TO_HBASE_BOLT, new SensorToHBase(),
				Consts.STORM_HBASE_SENSOR_BOLT_PARALLELISM).shuffleGrouping(
				PARSING_BOLT, PARSING_BOLT_STREAM);

		// Add HBaseBolt to right raw sensor data to HBase table
		// reads from PARSING_BOLT
		// outputs nothing
		builder.setBolt(HBASE_SENSOR_BOLT,
				StormFactory.getSensorDataHBaseBolt(),
				Consts.STORM_HBASE_SENSOR_BOLT_PARALLELISM)
				.shuffleGrouping(FORMAT_SENSOR_TO_HBASE_BOLT,
						FORMAT_SENSOR_TO_HBASE_BOLT_STREAM);

		return builder.createTopology();
	}
}