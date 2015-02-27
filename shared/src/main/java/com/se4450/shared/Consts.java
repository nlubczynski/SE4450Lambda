package com.se4450.shared;

/**
 * Constants class for Lambda Architecture
 * 
 * @author NikLubz
 *
 */
public final class Consts {

	/* Kafka */
	// Connection data
	public static final String KAFKA_HOST_NAME = "Kafka";
	public static final String KAFKA_HOST_IP = "129.100.224.244";
	public static final String KAFKA_HOST_PORT = "9092";
	public static final String KAFKA_TOPIC = "sensorData";

	/* Storm */
	// Topology configuration
	public static final int STOME_KAFKA_SPOUT_PARALLELISM = 5;
	public static final int STORM_FORMAT_BOLT_PARALLELISM = 5;
	public static final int STORM_HBASE_SENSOR_BOLT_PARALLELISM = 10;
	public static final int STORM_NUMBER_OF_WORKERS = 3;
	
	/* HBase*/
	public static final String HBASE_TABLE_NAME_SENSORS_SPEED_LAYER = "SensorValuesSpeedLayer";
	public static final String HBASE_COLUMN_FAMILY_SPEED_LAYER = "d";
}
