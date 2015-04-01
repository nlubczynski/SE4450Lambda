package com.se4450.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

public class Merge {

	/**
	 * Public method that server calls to get data for a specific sensor id, and
	 * start and end time
	 * 
	 * @param sensorId
	 *            sensor id to get data for
	 * @param timestampStart
	 *            epoch timestamp of when you want to start getting data from
	 * @param timestampEnd
	 *            epoch timestamp of when you want to end getting data from
	 * @return JSOnArray representing sensor readings of sensorId between
	 *         (inclusive) two timestamps
	 */
	public static JSONArray querySensorData(String sensorId,
			String timestampStart, String timestampEnd) {

		String startRowKeyString = formatStartRowKeyString(sensorId,
				timestampStart);
		String endRowKeyString = formatEndRowKeyString(sensorId, timestampEnd);

		Set<Reading> resultSet = getData(startRowKeyString, endRowKeyString);

		return toJSON(resultSet);
	}

	/**
	 * Public method server calls to get all data
	 * 
	 * @return JSONArray of all data in Serving and Speed layers
	 */
	public static JSONArray queryAllData() {
		Set<Reading> resultSet = getData(null, null);

		return toJSON(resultSet);
	}

	/**
	 * public method server calls to query building data
	 * 
	 * @param buildingID
	 *            id to get sensors for
	 * @param epoch
	 *            timestampStart time range start - inclusive
	 * @param epoch
	 *            timestampEnd time range end -inclusive
	 * @return a JSONArray of all the information
	 */
	public static JSONArray queryBuildingData(String buildingID,
			String timestampStart, String timestampEnd) {

		String startRowKeyString = formatStartRowKeyString(buildingID,
				timestampStart);
		String endRowKeyString = formatEndRowKeyString(buildingID, timestampEnd);

		Set<Reading> resultSet = getBuildingData(startRowKeyString,
				endRowKeyString);

		return toJSON(resultSet);
	}

	/**
	 * HBase takes in a string to start the scan at. This method formats that
	 * string. HBase will start scan at the row key which represents this
	 * string.
	 * 
	 * @param id
	 *            id to be used in HBase scan
	 * @param timestampEnd
	 *            the timestamp representing an end time for HBase scan
	 * @return a string value to use in Hbase scan as an end bound.
	 */
	private static String formatStartRowKeyString(String id,
			String timestampStart) {

		// timestamps are saved in HBase as 13 digits. Need to make timestamps
		// passed in from HTTP get request 13 digits long so HBase can use them
		// to filter scan. Otherwise 2 would be interpreted as 2000000000000
		timestampStart = formatTimestampStart(timestampStart);

		// create row key filter strings to pass to scan class.
		// Scanner will get data starting at this value

		String startRowKeyString = buildRowKeyFilterString(id, timestampStart);

		return startRowKeyString;
	}

	/**
	 * HBase takes in a string to end the scan at. This method formats that
	 * string. HBase will stop scan when the row key which represents this
	 * string is encountered.
	 * 
	 * @param id
	 *            id to be used in HBase scan
	 * @param timestampEnd
	 *            the timestamp representing an end time for HBase scan
	 * @return a string value to use in Hbase scan as an end bound.
	 */
	private static String formatEndRowKeyString(String id, String timestampEnd) {

		String endRowKeyString = null;

		if (timestampEnd != null) {
			// format to ensure data at current timestamp is retrieved
			timestampEnd = formatTimestampEnd(timestampEnd);
		} else {
			// format to ensure all data for the given timestamp is retrieved
			id = formatSensorId(id);
		}

		endRowKeyString = buildRowKeyFilterString(id, timestampEnd);

		return endRowKeyString;
	}

	/**
	 * Formats timestamp value to be 13 digits long
	 * 
	 * @param timestamp
	 *            string value representing a timestamp value
	 * @return timestamp value that is 13 digits long
	 */
	private static String formatTimestampStart(String timestamp) {

		if (timestamp != null)
			timestamp = String.format("%013d", Long.parseLong(timestamp));
		return timestamp;
	}

	/**
	 * Formats a string timestamp to be used as the end bound filter string in
	 * HBase scan. Adds 1 ms to it and makes it 13 digits
	 * 
	 * @param timestamp
	 *            is the timestamp to be formated
	 * @return a formated string to be used in HBase scan
	 */
	private static String formatTimestampEnd(String timestamp) {

		timestamp = String.format("%013d", Long.parseLong(timestamp) + 1);

		return timestamp;
	}

	/**
	 * Formats a sensorId to be used as the end bound filter string in HBase
	 * scan. Adds 1 to sensorId and a dash delimiter.
	 * 
	 * @param id
	 *            to be formated
	 * @return a formated string to be used in HBase scan
	 */
	private static String formatSensorId(String id) {

		id = String.valueOf((Long.parseLong(id) + 1) + "-");

		return id;
	}

	/**
	 * Gets all data for building in serving and speed layers based on two row
	 * key filters
	 * 
	 * @return JSONArray of data
	 */
	private static Set<Reading> getBuildingData(String startRowKeyString,
			String endRowKeyString) {

		Configuration conf = HBaseConfiguration.create();

		conf = Utilities.loadHBaseConfiguration(conf);

		// get table references
		HTable servingLayerTable = getTableReference("BuildingServingLayer",
				conf);
		HTable speedLayerTable = getTableReference("BuildingSpeedLayer", conf);

		HTable speedLayerTable2 = getTableReference("BuildingSpeedLayer2", conf);

		// default families
		ArrayList<String> families = new ArrayList<String>();
		families.add("d");

		// get table results
		ResultScanner servingLayerTableResults = scanHBaseTable(
				servingLayerTable, families, startRowKeyString, endRowKeyString);
		ResultScanner speedLayerTableResults = scanHBaseTable(speedLayerTable,
				families, startRowKeyString, endRowKeyString);
		ResultScanner speedLayer2TableResults = scanHBaseTable(
				speedLayerTable2, families, startRowKeyString, endRowKeyString);

		Set<Reading> results = null;

		try {
			results = mergeSpeedAndServingBuilding(servingLayerTableResults,
					speedLayerTableResults, speedLayer2TableResults);
		} catch (IOException e1) {
			e1.printStackTrace();
			System.out.println("Could not print out results");

			return null;
		}

		servingLayerTableResults.close();
		speedLayerTableResults.close();
		speedLayer2TableResults.close();

		try {
			servingLayerTable.close();
			speedLayerTable.close();
			speedLayerTable2.close();
		} catch (IOException e) {
			e.printStackTrace();

			System.out.println("Could not close hTable reference");
		}

		return results;
	}

	/**
	 * Gets all data in serving and speed layers based on two row key filters
	 * 
	 * @param startRowKeyString
	 *            the row key to start scan on
	 * @param endRowKeyString
	 *            the row key to end scan on. Data from this row key will not be
	 *            returned.
	 * @return array of HBase data
	 */
	private static Set<Reading> getData(String startRowKeyString,
			String endRowKeyString) {
		Configuration conf = HBaseConfiguration.create();

		conf = Utilities.loadHBaseConfiguration(conf);

		// get table references
		HTable servingLayerTable = getTableReference(
				"SensorValuesServingLayer", conf);
		HTable speedLayerTable = getTableReference("SensorValuesSpeedLayer",
				conf);

		HTable speedLayerTable2 = getTableReference("SensorValuesSpeedLayer2",
				conf);

		// default families
		ArrayList<String> families = new ArrayList<String>();
		families.add("d");

		// get table results
		ResultScanner servingLayerTableResults = scanHBaseTable(
				servingLayerTable, families, startRowKeyString, endRowKeyString);
		ResultScanner speedLayerTableResults = scanHBaseTable(speedLayerTable,
				families, startRowKeyString, endRowKeyString);
		ResultScanner speedLayer2TableResults = scanHBaseTable(
				speedLayerTable2, families, startRowKeyString, endRowKeyString);

		Set<Reading> results = null;

		try {
			results = mergeSpeedAndServing(servingLayerTableResults,
					speedLayerTableResults, speedLayer2TableResults);
		} catch (IOException e1) {
			e1.printStackTrace();
			System.out.println("Could not print out results");

			return null;
		}

		servingLayerTableResults.close();
		speedLayerTableResults.close();

		try {
			servingLayerTable.close();
			speedLayerTable.close();
		} catch (IOException e) {
			e.printStackTrace();

			System.out.println("Could not close hTable reference");
		}

		return results;
	}

	/**
	 * Builds the row key filter string based on an id and timestamp
	 * 
	 * @param id
	 *            to be used in filter string
	 * @param timestamp
	 *            to be used in filter string
	 * @return a formatted value to use HBase scan
	 */
	private static String buildRowKeyFilterString(String id, String timestamp) {

		StringBuilder sb = new StringBuilder();

		// if no id present then return null as all data should be retrieved
		if (id == null)
			return null;
		// id and timestamp
		else if (timestamp != null) {

			sb.append(id);
			sb.append("-");
			sb.append(timestamp);

		}
		// id no timestamp
		else {
			sb.append(id);
		}
		return sb.toString();
	}

	/**
	 * Gets the reference for an HTable
	 * 
	 * @param tableName
	 *            name of the table
	 * @param conf
	 *            configuration class for connecting to HTable
	 * @return an hTable reference to the HBase table
	 */
	private static HTable getTableReference(String tableName, Configuration conf) {
		HTable hTable = null;
		try {
			hTable = new HTable(conf, tableName);
		} catch (IOException e1) {
			e1.printStackTrace();
			System.out.println("Cannot create HTable reference " + tableName);
			return null;
		}

		return hTable;
	}

	/**
	 * This method scans an HBase table
	 * 
	 * @author Braden
	 * @param hTable
	 *            the instance of the the HBase table to be scan
	 * @param families
	 *            the column family that are to be included in scan
	 * @param rowKeyStart
	 *            the row key to which begin the scan on
	 * @param rowKeyEnd
	 *            the row key to which end the scan on
	 * @return a ResultsScanner object that holds results of the scan
	 */
	private static ResultScanner scanHBaseTable(HTable hTable,
			ArrayList<String> families, String rowKeyStart, String rowKeyEnd) {

		// Create Scan object for building scan
		Scan scan = new Scan();
		scan.setCaching(20);

		// Add families to scan object
		for (String family : families) {
			scan.addFamily(Bytes.toBytes(family));
		}

		// set
		if (rowKeyStart != null)
			scan.setStartRow(Bytes.toBytes(rowKeyStart));
		if (rowKeyEnd != null)
			scan.setStopRow(Bytes.toBytes(rowKeyEnd));

		ResultScanner scanner = null;
		try {
			scanner = hTable.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();

			System.out.println("*****ERROR*****");
			System.out.println(e.getMessage());

			return null;
		}

		return scanner;
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
	private static Set<Reading> mergeSpeedAndServing(
			ResultScanner servingLayer, ResultScanner speedLayer,
			ResultScanner speedLayer2) throws IOException {

		Set<Reading> resultSet = new HashSet<Reading>();

		// For Debug purposed only
		int servingLayerResultsCount = 0;
		for (Result result = servingLayer.next(); (result != null); result = servingLayer
				.next()) {

			servingLayerResultsCount++;
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
		int speedLayerResultsCount = 0;
		for (Result result = speedLayer.next(); (result != null); result = speedLayer
				.next()) {

			speedLayerResultsCount++;
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
		int speedLayer2ResultsCount = 0;
		for (Result result = speedLayer2.next(); (result != null); result = speedLayer2
				.next()) {

			speedLayer2ResultsCount++;
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

		System.out.println("****Summary****");
		System.out.println("Number in merged results "
				+ servingLayerResultsCount + speedLayerResultsCount
				+ speedLayer2ResultsCount);
		System.out.println("Number in serving results "
				+ servingLayerResultsCount);
		System.out.println("Number in speed results " + speedLayerResultsCount);
		System.out
				.println("Number in speed results " + speedLayer2ResultsCount);

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
	private static Set<Reading> mergeSpeedAndServingBuilding(
			ResultScanner servingLayer, ResultScanner speedLayer,
			ResultScanner speedLayer2) throws IOException {

		Set<Reading> resultSet = new HashSet<Reading>();

		// For Debug purposed only
		int servingLayerResultsCount = 0;
		for (Result result = servingLayer.next(); (result != null); result = servingLayer
				.next()) {

			servingLayerResultsCount++;
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
		int speedLayerResultsCount = 0;
		for (Result result = speedLayer.next(); (result != null); result = speedLayer
				.next()) {

			speedLayerResultsCount++;
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
		int speedLayer2ResultsCount = 0;
		for (Result result = speedLayer2.next(); (result != null); result = speedLayer2
				.next()) {

			speedLayer2ResultsCount++;
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

		System.out.println("****Summary****");
		System.out.println("Number in merged results "
				+ servingLayerResultsCount + speedLayerResultsCount
				+ speedLayer2ResultsCount);
		System.out.println("Number in serving results "
				+ servingLayerResultsCount);
		System.out.println("Number in speed results " + speedLayerResultsCount);
		System.out
				.println("Number in speed results " + speedLayer2ResultsCount);

		return resultSet;
	}

	/**
	 * Converts a Set to JSONarray
	 * 
	 * @param resultSet
	 *            Set of results
	 * @return a JSONArray of data
	 */
	private static JSONArray toJSON(Set<Reading> resultSet) {
		JSONArray resultsArray = new JSONArray();

		for (Reading reading : resultSet) {
			JSONObject obj = reading.serialize();
			resultsArray.put(obj);
		}

		return resultsArray;
	}
}
