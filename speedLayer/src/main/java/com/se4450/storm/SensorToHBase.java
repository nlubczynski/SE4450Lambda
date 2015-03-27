package com.se4450.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SensorToHBase extends BaseBasicBolt {

	private static final long serialVersionUID = 7345000568936359797L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		// Get sensorID
		int sensorID = input.getInteger(0);
		// Get value
		int sensorValue = input.getInteger(1);
		// Get Time stamp
		long timestamp = input.getLong(2);
		
		// Make a nice string
		String key = sensorID + "-" + timestamp;

		collector.emit(SE4450Topology.FORMAT_SENSOR_TO_HBASE_BOLT_STREAM, new Values(key,
				String.valueOf(sensorValue)));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(SE4450Topology.FORMAT_SENSOR_TO_HBASE_BOLT_STREAM, new Fields(
				SE4450Topology.FORMAT_SENSOR_TO_HBASE_BOLT_KEY,
				SE4450Topology.FORMAT_SENSOR_TO_HBASE_BOLT_VALUE));
	}

}
