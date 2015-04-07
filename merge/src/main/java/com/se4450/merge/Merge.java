package com.se4450.merge;

import java.io.IOException;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

public final class Merge {

	private Merge() {

	}

	/**
	 * Merges results from serving layer and both speed layers
	 * 
	 * @param servingLayer
	 *            results from serving layer
	 * @param speedLayer
	 *            results from speed layer
	 * @param speedLayer2
	 *            results form speed layer 2
	 * @return a Set of the results
	 * @throws IOException
	 */
	public static Set<Reading> mergeSpeedAndServing(ResultScanner servingLayer,
			ResultScanner speedLayer, ResultScanner speedLayer2)
			throws IOException {

		Set<Reading> resultSet = new HashSet<Reading>();

		for (Result result = servingLayer.next(); (result != null); result = servingLayer
				.next()) {

			// gets rowkey
			String rowKey = Bytes.toString(result.getRow());

			String[] rowKeySplit = rowKey.split("-");

			try {
				if (rowKeySplit.length != 2)
					throw new Exception();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("did not split correctly!");

				return null;
			}

			String servingLayerSensorId = rowKeySplit[0];
			String servingLayerTimestamp = rowKeySplit[1];

			// String rowKeySplit = rowKey.
			String servingLayerValue = Bytes.toString(result.getValue(
					Bytes.toBytes("d"), Bytes.toBytes("val")));

			// Create Reading object
			Reading sensorReading = new Reading(servingLayerSensorId,
					servingLayerTimestamp, servingLayerValue);

			// Add to set
			resultSet.add(sensorReading);
		}

		// look up id then look up timestamp if not there add to map. otherwise
		// skip
		for (Result result = speedLayer.next(); (result != null); result = speedLayer
				.next()) {

			// gets rowkey
			String rowKey = Bytes.toString(result.getRow());

			String[] rowKeySplit = rowKey.split("-");

			try {
				if (rowKeySplit.length != 2)
					throw new Exception();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("did not split correctly!");

				return null;
			}

			String speedLayerSensorId = rowKeySplit[0];
			String speedLayerTimestamp = rowKeySplit[1];

			// read the value of table record i.e. sensor reading
			String speedLayerValue = Bytes.toString(result.getValue(
					Bytes.toBytes("d"), Bytes.toBytes("val")));

			// Create Reading object
			Reading sensorReading = new Reading(speedLayerSensorId,
					speedLayerTimestamp, speedLayerValue);

			// Add to set
			resultSet.add(sensorReading);

		}

		// Add in speed table 2
		// look up id then look up timestamp if not there add to map. otherwise
		// skip
		for (Result result = speedLayer2.next(); (result != null); result = speedLayer2
				.next()) {

			// gets rowkey
			String rowKey = Bytes.toString(result.getRow());
			// String sensorID = rowKey.substring(0, rowKey.indexOf("-"));

			String[] rowKeySplit = rowKey.split("-");

			try {
				if (rowKeySplit.length != 2)
					throw new Exception();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("did not split correctly!");

				return null;
			}

			String speedLayer2SensorId = rowKeySplit[0];
			String speedLayer2Timestamp = rowKeySplit[1];

			// read the value of table record i.e. sensor reading
			String speedLayer2Value = Bytes.toString(result.getValue(
					Bytes.toBytes("d"), Bytes.toBytes("val")));

			// Create Reading object
			Reading sensorReading = new Reading(speedLayer2SensorId,
					speedLayer2Timestamp, speedLayer2Value);

			// Add to set
			resultSet.add(sensorReading);

		}

		return resultSet;
	}

	/**
	 * Merges data from serving layer and both speed layers
	 * 
	 * @param servingLayer
	 *            results from serving layer
	 * @param speedLayer
	 *            results from speed layer
	 * @param speedLayer2
	 *            results from speed layer 2
	 * @return a Set of the results
	 * @throws IOException
	 */
	public static Set<Reading> mergeSpeedAndServingBuilding(
			ResultScanner servingLayer, ResultScanner speedLayer,
			ResultScanner speedLayer2) throws IOException {

		Set<Reading> resultSet = new HashSet<Reading>();

		for (Result result = servingLayer.next(); (result != null); result = servingLayer
				.next()) {

			// gets rowkey
			String rowKey = Bytes.toString(result.getRow());

			String[] rowKeySplit = rowKey.split("-");

			try {
				if (rowKeySplit.length != 2)
					throw new Exception();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("did not split correctly!");

				return null;
			}

			String servingLayerTimestamp = rowKeySplit[1];

			// String rowKeySplit = rowKey.
			// for column family d of row with rowkey , rowkey, get a map of
			// qualifier (id) to value (sensor Reading)
			NavigableMap<byte[], byte[]> readingsMap = result
					.getFamilyMap(Bytes.toBytes("d"));

			NavigableSet<byte[]> servingLayerRowSensorIDList = readingsMap
					.descendingKeySet();

			for (byte[] id : servingLayerRowSensorIDList) {
				// get id and value
				String servingLayerValue = Bytes.toString(readingsMap.get(id));
				String servingLayerSensorId = Bytes.toString(id);

				// Create Reading object
				Reading sensorReading = new Reading(servingLayerSensorId,
						servingLayerTimestamp, servingLayerValue);

				// Add to set
				resultSet.add(sensorReading);
			}
		}

		// look up id then look up timestamp if not there add to map. otherwise
		// skip
		for (Result result = speedLayer.next(); (result != null); result = speedLayer
				.next()) {

			// gets rowkey
			String rowKey = Bytes.toString(result.getRow());

			String[] rowKeySplit = rowKey.split("-");

			try {
				if (rowKeySplit.length != 2)
					throw new Exception();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("did not split correctly!");

				return null;
			}

			String speedLayerTimestamp = rowKeySplit[1];

			// String rowKeySplit = rowKey.
			// for column family d of row with rowkey , rowkey, get a map of
			// qualifier (id) to value (sensor Reading)
			NavigableMap<byte[], byte[]> readingsMap = result
					.getFamilyMap(Bytes.toBytes("d"));

			NavigableSet<byte[]> speedLayerRowSensorIDList = readingsMap
					.descendingKeySet();

			for (byte[] id : speedLayerRowSensorIDList) {
				// get id and value
				String speedLayerValue = Bytes.toString(readingsMap.get(id));
				String speedLayerSensorId = Bytes.toString(id);

				// Create Reading object
				Reading sensorReading = new Reading(speedLayerSensorId,
						speedLayerTimestamp, speedLayerValue);

				// Add to set
				resultSet.add(sensorReading);
			}

		}

		// Add in speed table 2
		// look up id then look up timestamp if not there add to map. otherwise
		// skip
		for (Result result = speedLayer2.next(); (result != null); result = speedLayer2
				.next()) {

			// gets rowkey
			String rowKey = Bytes.toString(result.getRow());
			// String sensorID = rowKey.substring(0, rowKey.indexOf("-"));

			String[] rowKeySplit = rowKey.split("-");

			try {
				if (rowKeySplit.length != 2)
					throw new Exception();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("did not split correctly!");

				return null;
			}

			String speedLayer2Timestamp = rowKeySplit[1];

			// String rowKeySplit = rowKey.
			// for column family d of row with rowkey , rowkey, get a map of
			// qualifier (id) to value (sensor Reading)
			NavigableMap<byte[], byte[]> readingsMap = result
					.getFamilyMap(Bytes.toBytes("d"));

			NavigableSet<byte[]> speedLayer2RowSensorIDList = readingsMap
					.descendingKeySet();

			for (byte[] id : speedLayer2RowSensorIDList) {
				// get id and value
				String speedLayer2Value = Bytes.toString(readingsMap.get(id));
				String speedLayer2SensorId = Bytes.toString(id);

				// Create Reading object
				Reading sensorReading = new Reading(speedLayer2SensorId,
						speedLayer2Timestamp, speedLayer2Value);

				// Add to set
				resultSet.add(sensorReading);
			}

		}

		return resultSet;
	}
}
