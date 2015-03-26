package com.se4450.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.protobuf.ServiceException;

/**
 * Hello world!
 *
 */
public class Merge {

	public static void main(String[] args) {
		Reading r1 = new Reading("1", "123", "100");
		Reading r2 = new Reading("1", "123", "1004");
		Reading r3 = new Reading("2", "123", "100");
		Reading r4 = new Reading("1", "1234", "100");

		Set<Reading> set = new HashSet<Reading>();

		set.add(r1);
		set.add(r2);
		set.add(r3);
		set.add(r4);

		for (Reading reading : set) {
			System.out.println(reading.getId() + " : "
					+ reading.get_timestamp());

		}
	}

	/**
	 * Public method that server calls to get data for a specific data
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

		// create row key filter strings to pass to scan class.
		// Scanner will get data between these two values.

		// start is inclusive
		String startRowKeyString = buildRowKeyFilterString(sensorId,
				timestampStart);

		// End date is exclusive so must fix to be inclusive
		String endRowKeyString = null;

		// if timestamp is not null add 1 ms to increment.
		if (timestampEnd != null) {
			String timestampEndFixed = String.valueOf((Integer
					.parseInt(timestampEnd) + 1));

			// create row key filter based on this new parameter
			endRowKeyString = buildRowKeyFilterString(sensorId,
					timestampEndFixed);
		}
		// if timestamp is null increment sensorId to next value as we want to
		// get all the values for that sensor
		else {
			String sensorIdFixed = String
					.valueOf((Integer.parseInt(sensorId) + 1));

			// create row key filter based on this new paramaeters
			endRowKeyString = buildRowKeyFilterString(sensorIdFixed,
					timestampEnd);

		}

		return getData(startRowKeyString, endRowKeyString);

	}

	/**
	 * Public method server calls to get all data
	 * 
	 * @return JSONArray of all data in Serving and Speed layers
	 */

	public static JSONArray queryAllData() {
		return getData(null, null);
	}

	/**
	 * Gets all data in serving and speed layers based on two row key filters
	 * 
	 * @return JSONArray of data
	 */
	private static JSONArray getData(String startRowKeyString,
			String endRowKeyString) {
		Configuration conf = HBaseConfiguration.create();

		conf = Utilities.loadHBaseConfiguration(conf);

		// get table references
		HTable servingLayerTable = getTableReference(
				"SensorValuesServingLayer", conf);
		HTable speedLayerTable = getTableReference("SensorValuesSpeedLayer",
				conf);

		// default families
		ArrayList<String> families = new ArrayList<String>();
		families.add("d");

		// get table results
		ResultScanner servingLayerTableResults = scanHBaseTable(
				servingLayerTable, families, startRowKeyString, endRowKeyString);
		ResultScanner speedLayerTableResults = scanHBaseTable(speedLayerTable,
				families, startRowKeyString, endRowKeyString);

		Set<Reading> results = null;

		try {
			results = mergeSpeedAndServing(servingLayerTableResults,
					speedLayerTableResults);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("Could not print out results");

			return null;
		}

		JSONArray resultsJSON = toJSON(results);

		servingLayerTableResults.close();
		speedLayerTableResults.close();

		try {
			servingLayerTable.close();
			speedLayerTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

			System.out.println("Could not close hTable reference");
		}

		return resultsJSON;
	}

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

	private static HTable getTableReference(String tableName, Configuration conf) {
		try {
			HBaseAdmin.checkHBaseAvailable(conf);
		} catch (ServiceException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

		HTable hTable = null;
		try {
			hTable = new HTable(conf, tableName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
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
			// TODO Auto-generated catch block
			e.printStackTrace();

			System.out.println("*****ERROR*****");
			System.out.println(e.getMessage());

			return null;
		}

		return scanner;
	}

	private static Set<Reading> mergeSpeedAndServing(
			ResultScanner servingLayer, ResultScanner speedLayer)
			throws IOException {

		Set<Reading> resultSet = new HashSet<Reading>();

		// For Debug purposed only
		int servingLayerResultsCount = 0;
		for (Result result = servingLayer.next(); (result != null); result = servingLayer
				.next()) {

			// gets rowkey
			String rowKey = Bytes.toString(result.getRow());
			// String sensorID = rowKey.substring(0, rowKey.indexOf("-"));

			String[] rowKeySplit = rowKey.split("-");

			try {
				if (rowKeySplit.length != 2)
					throw new Exception();
			} catch (Exception e) {
				// TODO Auto-generated catch block
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
			// gets rowkey
			String rowKey = Bytes.toString(result.getRow());
			// String sensorID = rowKey.substring(0, rowKey.indexOf("-"));

			String[] rowKeySplit = rowKey.split("-");

			try {
				if (rowKeySplit.length != 2)
					throw new Exception();
			} catch (Exception e) {
				// TODO Auto-generated catch block
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

		System.out.println("****Summary****");
		System.out.println("Number in merged results "
				+ servingLayerResultsCount + speedLayerResultsCount);
		System.out.println("Number in serving results "
				+ servingLayerResultsCount);
		System.out.println("Number in speed results " + speedLayerResultsCount);

		return resultSet;
	}

	private static JSONArray toJSON(Set<Reading> resultSet) {
		JSONArray resultsArray = new JSONArray();

		for (Reading reading : resultSet) {
			JSONObject obj = reading.serialize();
			resultsArray.put(obj);
		}

		return resultsArray;
	}
}
