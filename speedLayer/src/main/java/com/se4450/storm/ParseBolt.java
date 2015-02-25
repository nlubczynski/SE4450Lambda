package com.se4450.storm;

import org.joda.time.DateTime;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A Bolt that takes incoming Kafka strings, and parses them into sensorID,
 * sensorValue, and timestamp
 * 
 * @author NikLubz
 *
 */
public class ParseBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1353253736861496000L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// Get the string
		String input = tuple.getString(0);

		// Split and make sure it's the right length
		String[] inputs = input.split(" ");
		if (inputs.length != 3)
			return;
		
		int sensorID, sensorValue;
		DateTime timestamp;

		// Parse the data
		try {
			sensorID = Integer.parseInt(inputs[0]);
			sensorValue = Integer.parseInt(inputs[1]);
			timestamp = new DateTime(Long.parseLong(inputs[2]) * 1000L);
		} catch (Exception e) {
			// if there are any errors parsing, throw out the data			
			return;
		}
		
		// Emit values
		collector.emit("formattedStream", new Values(sensorID, sensorValue, timestamp));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("formattedStream", new Fields("sensorID",
				"sensorValue", "timestamp"));
	}
}