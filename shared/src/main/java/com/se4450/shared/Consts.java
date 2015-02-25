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
	// Topology config
	public static final int KAFKA_SPOUT_PARALLELISM = 5;
	public static final int FORMAT_BOLT_PARALLELISM = 5;
}
