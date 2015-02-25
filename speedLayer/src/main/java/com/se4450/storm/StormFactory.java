package com.se4450.storm;

import java.util.UUID;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.common.ColumnList;

import com.se4450.shared.Consts;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * Factory for creating various spouts and bolts, building project specific
 * configuration in.
 * 
 * @author Nik Lubczynski
 *
 */
public class StormFactory {

	/**
	 * 
	 * @return KafkaSpout A KafkaSpout configured to communicate with the Kafka
	 *         host configured in com.se4450.shared.Consts
	 */
	public static KafkaSpout getKafkaSpout() {
		BrokerHosts hosts = new ZkHosts(Consts.KAFKA_HOST_IP
				+ Consts.KAFKA_HOST_PORT);

		SpoutConfig spoutConfig = new SpoutConfig(hosts, Consts.KAFKA_TOPIC,
				"/" + Consts.KAFKA_TOPIC, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		return new KafkaSpout(spoutConfig);
	}

	public static HBaseBolt getSensorDataHBaseBolt() {

		SimpleHBaseMapper mapper = new SimpleHBaseMapper()
				.withColumnFields(new Fields("key"))
				.withColumnFields(new Fields("value"))
				.withColumnFamily("d");
		
		HBaseBolt hbase = new HBaseBolt("WordCount", mapper);

		return null;
	}
}
