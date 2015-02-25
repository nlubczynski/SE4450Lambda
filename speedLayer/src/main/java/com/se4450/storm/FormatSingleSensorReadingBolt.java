package com.se4450.storm;

import org.joda.time.DateTime;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
public class FormatSingleSensorReadingBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1353253736861496000L;

	// Execute is called to process tuples
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String input = tuple.getString(0);
		String[] inputs = input.split(" ");
		// Get sensorID
		int sensorID = Integer.parseInt(inputs[0]);
		// Get value
		int value = Integer.parseInt(inputs[1]);
		// Get Time stamp
		DateTime timestamp = new DateTime(Long.parseLong(inputs[2]) * 1000L);
		// Make a nice string
		String key = sensorID + "-" + timestamp;
		
		// Emit values
		collector.emit("hbasestream", new Values(key, value));
	}

	// Declare that emitted tuples will contain a word field
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("hbasestream",new Fields("key", "value"));
	}
}