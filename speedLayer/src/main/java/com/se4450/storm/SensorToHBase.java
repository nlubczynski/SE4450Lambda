package com.se4450.storm;

import org.joda.time.DateTime;

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
		DateTime timestamp = new DateTime(input.getInteger(2) * 1000L);
		// Make a nice string
		String key = sensorID + "-" + timestamp;

		collector.emit(SE4450Topology.HBASE_SENSOR_STREAM, new Values(key,
				sensorValue));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(SE4450Topology.HBASE_SENSOR_STREAM, new Fields(
				SE4450Topology.HBASE_SENSOR_KEY,
				SE4450Topology.HBASE_SENSOR_VALUE));
	}

}
