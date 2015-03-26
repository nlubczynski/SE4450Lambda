package com.se4450.shared;

/**
 * Constants class for Lambda Architecture
 * 
 * @author NikLubz
 *
 */
public final class Consts {
	
	/* Hosts */
	public static final String Storm_Zookeeper1 = "zookeeper1";
	public static final String Storm_Zookeeper2 = "zookeeper2";
	public static final String Storm_Zookeeper3 = "zookeeper3";
	public static final String Storm_Nimubs = "nimbus1";
	public static final String Storm_Supervisor1 = "supervisor1";
	public static final String Storm_Supervisor2 = "supervisor2";
	public static final String Storm_Supervisor3 = "supervisor3";
	public static final String Storm_Supervisor4 = "supervisor4";
	public static final String Storm_Supervisor5 = "supervisor5";
	public static final String Storm_Kafka1 = "kafka1";
	public static final String Storm_Kafka2 = "kakfa2";
	public static final String HBase_Master = "master";
	public static final String HBase_Zookeeper1 = "zookeeper-1";
	public static final String HBase_Zookeeper2 = "zookeeper-2";
	public static final String HBase_Zookeeper3 = "zookeeper-3";
	public static final String HBase_Slave1 = "slave-1";
	public static final String HBase_Slave2 = "slave-2";
	public static final String HBase_Slave3 = "slave-3";

	/* Kafka */
	// Connection data
	public static final String KAFKA_HOST_PORT = "2181";
	public static final String KAFKA_TOPIC = "sensorData";

	/* Storm */
	// Topology configuration
	public static final String STORM_TOPOLOGY_NAME = "SpeedLayer";
	public static final String STORM_HBASE_CONF_FILE = "hbase.conf";
	public static final String STORM_HBASE_SITE_XML = "conf/hbase-site.xml";			
	public static final int STORM_KAFKA_SPOUT_PARALLELISM = 2;
	public static final int STORM_FORMAT_BOLT_PARALLELISM = 5;
	public static final int STORM_HBASE_SENSOR_BOLT_PARALLELISM = 10;
	public static final int STORM_HDFS_BOLT_PARALLELISM = 5;
	public static final int STORM_NUMBER_OF_WORKERS = 5;
	
	/*HDFS */
	public static final String HDFS_PORT = "8020";
	public static final String HDFS_URL = "hdfs://" + HBase_Master + ":" + HDFS_PORT;
	public static final String HDFS_PATH = "/sensorData/";
	
	/* HBase*/
	public static final String HBASE_TABLE_NAME_SENSORS_SPEED_LAYER = "SensorValuesSpeedLayer";
	public static final String HBASE_TABLE_NAME_SENSORS_SPEED_LAYER2 = "SensorValuesSpeedLayer2";
	public static final String HBASE_COLUMN_FAMILY_SPEED_LAYER = "d";
	
}
