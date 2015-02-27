package com.se4450.storm;

import java.util.UUID;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

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
	 * @return A KafkaSpout configured to communicate with the Kafka host
	 *         configured in com.se4450.shared.Consts
	 */
	public static KafkaSpout getKafkaSpout() {
		BrokerHosts hosts = new ZkHosts(Consts.KAFKA_HOST_IP
				+ Consts.KAFKA_HOST_PORT);

		SpoutConfig spoutConfig = new SpoutConfig(hosts, Consts.KAFKA_TOPIC,
				"/" + Consts.KAFKA_TOPIC, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		return new KafkaSpout(spoutConfig);
	}

	/**
	 * 
	 * @return An HBaseBolt configured to write to the Sensor values HBase table
	 *         set in com.se4450.shared.Consts. It's configured to expect the
	 *         values from emitted from a SensorToHBase bolt.
	 */
	public static HBaseBolt getSensorDataHBaseBolt() {

		// Create a SimpleHBaseMapper with the values from a SensorToHBase bolt
		SimpleHBaseMapper mapper = new SimpleHBaseMapper()
				.withColumnFields(new Fields(SE4450Topology.FORMAT_SENSOR_TO_HBASE_BOLT_KEY))
				.withColumnFields(new Fields(SE4450Topology.FORMAT_SENSOR_TO_HBASE_BOLT_VALUE))
				.withColumnFamily(Consts.HBASE_COLUMN_FAMILY_SPEED_LAYER);

		return new HBaseBolt(Consts.HBASE_TABLE_NAME_SENSORS_SPEED_LAYER,
				mapper).withConfigKey("hbase.conf");
	}
}
