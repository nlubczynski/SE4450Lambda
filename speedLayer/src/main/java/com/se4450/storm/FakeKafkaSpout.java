package com.se4450.storm;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FakeKafkaSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9147841936800448004L;
	boolean isDistributed;
	SpoutOutputCollector collector;

	public FakeKafkaSpout() {
		this(true);
	}

	public FakeKafkaSpout(boolean isDistributed) {
		this.isDistributed = isDistributed;
	}

	public boolean isDistributed() {
		return this.isDistributed;
	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void close() {

	}

	public void nextTuple() {
		final Random rand = new Random();
		int id = rand.nextInt(10) + 1;
		int value = rand.nextInt(1000) + 1;
		int time = rand.nextInt(10000000);
		
		this.collector.emit(new Values(new String( id + " " + value + " " + time)), UUID.randomUUID());
		Thread.yield();
	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public void activate() {
	}

	public void deactivate() {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}