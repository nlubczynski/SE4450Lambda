package com.se4450.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A Bolt that takes incoming Kafka strings, and parses them into
 * SE4450Topology.HDFS_PARSE_BOLT_ID, SE4450Topology.HDFS_PARSE_BOLT_VALUE
 * 
 * @author NikLubz
 *
 */
public class HdfsParseBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1353253736861496000L;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// Get the string
		String input = tuple.getString(0);

		// Split and make sure it's the right length
		String[] inputs = input.split(" ");
		if (inputs.length != 4)
			return;

		int sensorID, sensorValue, buildingID;
		long timestamp;

		// Parse the data
		try {
			sensorID = Integer.parseInt(inputs[0]);
			sensorValue = Integer.parseInt(inputs[1]);
			timestamp = Long.parseLong(inputs[2]);
			buildingID = Integer.parseInt(inputs[3]);
		} catch (Exception e) {
			// if there are any errors parsing, throw out the data
			return;
		}

		// Emit values
		collector.emit(SE4450Topology.HDFS_PARSING_BOLT_STREAM, new Values(sensorID + "-" + timestamp, sensorValue, buildingID));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(SE4450Topology.HDFS_PARSING_BOLT_STREAM, new Fields(
				SE4450Topology.HDFS_PARSE_BOLT_ID,
				SE4450Topology.HDFS_PARSE_BOLT_VALUE,
				SE4450Topology.HDFS_PARSE_BOLT_BID));
	}
}