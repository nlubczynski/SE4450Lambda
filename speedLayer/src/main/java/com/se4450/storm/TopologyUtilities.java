package com.se4450.storm;

import java.util.UUID;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

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
public class TopologyUtilities {

	/**
	 * 
	 * @return A KafkaSpout configured to communicate with the Kafka host
	 *         configured in com.se4450.shared.Consts
	 */
	public static KafkaSpout getKafkaSpout() {
		BrokerHosts hosts = new ZkHosts(Consts.Storm_Zookeeper1 + ":"
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
	 *         values emitted from a SensorToHBase bolt.
	 */
	public static HBaseBolt getSensorDataHBaseBolt() {

		// Create a SimpleHBaseMapper with the values from a SensorToHBase bolt
		SimpleHBaseMapper mapper = new SimpleHBaseMapper()
				.withRowKeyField(SE4450Topology.FORMAT_SENSOR_TO_HBASE_BOLT_KEY)
				.withColumnFields(
						new Fields(
								SE4450Topology.FORMAT_SENSOR_TO_HBASE_BOLT_VALUE))
				.withColumnFamily(Consts.HBASE_COLUMN_FAMILY_SPEED_LAYER);

		return new HBaseBolt(Consts.HBASE_TABLE_NAME_SENSORS_SPEED_LAYER,
				mapper).withConfigKey(Consts.STORM_HBASE_CONF_FILE);
	}

	public static HdfsBolt getHdfsBolt() {

		// use "|" instead of "," for field delimiter
		RecordFormat format = new DelimitedRecordFormat()
				.withFieldDelimiter(" ");
		
		// sync the filesystem after every 1k tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(1);

		// rotate files when they reach 5MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f,
				Units.MB);

		FileNameFormat fileNameFormat = new DefaultFileNameFormat()
				.withPath(Consts.HDFS_PATH);

		return new HdfsBolt().withFsUrl(Consts.HDFS_URL)
				.withFileNameFormat(fileNameFormat).withRecordFormat(format)
				.withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);
	}
}
