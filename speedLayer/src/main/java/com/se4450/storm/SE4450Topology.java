package com.se4450.storm;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

import com.se4450.shared.Consts;
import com.se4450.shared.HBaseUtils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class SE4450Topology {

	public static final String KAFKA_SPOUT = "kafkaSpout";

	public static final String HDFS_BOLT = "hdfsBolt";

	public static final String PARSING_BOLT = "parsedData";
	public static final String PARSING_BOLT_STREAM = "parsedStream";
	public static final String PARSING_BOLT_ID = "sensorID";
	public static final String PARSING_BOLT_VALUE = "sensorValue";
	public static final String PARSING_BOLT_TIME = "sensorTimestamp";

	public static final String FORMAT_SENSOR_TO_HBASE_BOLT = "formatSensorToHbase";
	public static final String FORMAT_SENSOR_TO_HBASE_BOLT_STREAM = "formatSensorToHbaseStream";
	public static final String FORMAT_SENSOR_TO_HBASE_BOLT_KEY = "formatSensorToHbaseKey";
	public static final String FORMAT_SENSOR_TO_HBASE_BOLT_VALUE = "val";

	public static final String HBASE_SENSOR_BOLT = "hbaseBolt";

	public static final String HDFS_PARSE_BOLT = "hdfsParseBolt";
	public static final String HDFS_PARSE_BOLT_ID = "hdfsParseBoltId";
	public static final String HDFS_PARSE_BOLT_VALUE = "hdfsParseBoltVavlue";
	public static final String HDFS_PARSING_BOLT_STREAM = "hdfsParseBoltStream";

	/**
	 * The main entry point of the storm topology
	 * 
	 * @param args
	 *            args[0] The XML file to open to load the hbase information,
	 *            generally hbase-site.xml
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// Create configuration
		Config conf = new Config();
		conf.setDebug(true);

		// hbase configuration
		Map<String, Object> hbConf = new HashMap<String, Object>();
		if (args.length > 0 && HBaseUtils.LoadHBaseSiteData(args[0], hbConf))
			conf.put(Consts.STORM_HBASE_CONF_FILE, hbConf);

		// Add serialization for DateTime
		conf.registerSerialization(DateTime.class);

		// If there are arguments, we are running on a cluster
		if (args != null && args.length > 0) {
			// parallelism hint to set the number of workers
			conf.setNumWorkers(Consts.STORM_NUMBER_OF_WORKERS);
			// submit the topology
			StormSubmitter.submitTopology(Consts.STORM_TOPOLOGY_NAME, conf,
					createTopology());
		}
		// Otherwise, we are running locally
		else {
			// Cap the maximum number of executors that can be spawned
			// for a component to 3
			conf.setMaxTaskParallelism(3);
			// LocalCluster is used to run locally
			LocalCluster cluster = new LocalCluster();
			// submit the topology
			cluster.submitTopology(Consts.STORM_TOPOLOGY_NAME, conf,
					createTopology());
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
		builder.setSpout(KAFKA_SPOUT, TopologyUtilities.getKafkaSpout(),
				Consts.STORM_KAFKA_SPOUT_PARALLELISM);

		// Add ParseBolt to split/format incoming Kafka stream
		// reads from KAFKA_SPOUT
		// outputs parsed data in the form (int:sensorID, int:sensorValue,
		// long:timestamp) to PARSING_BOLT
		builder.setBolt(PARSING_BOLT, new ParseBolt(),
				Consts.STORM_FORMAT_BOLT_PARALLELISM).shuffleGrouping(
				KAFKA_SPOUT);

		// Add HDFS parse bolt to split/format incoming Kafka stream
		// reads from KAFKA_SPOUT
		// outputs parsed data in the form (string: id, long:timestamp) to PARSING_BOLT
		builder.setBolt(HDFS_PARSE_BOLT, new HdfsParseBolt(),
				Consts.STORM_FORMAT_BOLT_PARALLELISM).shuffleGrouping(
				KAFKA_SPOUT);

		// Add SensorToHDFS to format sensor data for HDFS
		// reads from PARSING_BOLT : PARSING_BOLT_STREAM
		builder.setBolt(HDFS_BOLT, TopologyUtilities.getHdfsBolt(),
				Consts.STORM_HDFS_BOLT_PARALLELISM).shuffleGrouping(
				HDFS_PARSE_BOLT, HDFS_PARSING_BOLT_STREAM);

		// Add SenstorToHBase to format sensor data for HBase
		// reads from PARSING_BOLT : PARSING_BOLT_STREAM
		// outputs parsed data in the form (string:key, string:value) to
		// FORMAT_SENSOR_TO_HBASE_BOLT
		builder.setBolt(FORMAT_SENSOR_TO_HBASE_BOLT, new SensorToHBase(),
				Consts.STORM_HBASE_SENSOR_BOLT_PARALLELISM).shuffleGrouping(
				PARSING_BOLT, PARSING_BOLT_STREAM);

		// Add HBaseBolt to write raw sensor data to HBase table
		// reads from PARSING_BOLT
		// outputs nothing
		builder.setBolt(HBASE_SENSOR_BOLT,
				TopologyUtilities.getSensorDataHBaseBolt(),
				Consts.STORM_HBASE_SENSOR_BOLT_PARALLELISM)
				.shuffleGrouping(FORMAT_SENSOR_TO_HBASE_BOLT,
						FORMAT_SENSOR_TO_HBASE_BOLT_STREAM);

		return builder.createTopology();
	}
}