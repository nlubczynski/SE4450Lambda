package com.se4450.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.json.JSONException;
import org.json.JSONObject;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

/**
 * Hello world!
 *
 */
public class Merge {
	/**
	 * Main method for debug purposes only
	 * @param args not used in current implementation
	 */
	public static void main(String[] args) {

		getAllDataQuery();

		System.exit(0);
	}

	/**
	 * A query that is called by tomcat server to get all data in serving and
	 * speed layers
	 * 
	 * @return JSONArray of data
	 */
	public static JSONArray getAllDataQuery() {
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
				servingLayerTable, families);
		ResultScanner speedLayerTableResults = scanHBaseTable(speedLayerTable,
				families);

		HashMap<String, HashMap<String, String>> results = null;

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
	 * Scans Hbase wen no start or end row key values are required. Will scan
	 * whole table.
	 * 
	 * @param hTable
	 *            the instance of the the HBase table to be scan
	 * @param families
	 *            the column family that are to be included in scan
	 * @return a ResultScanner for whole table
	 */

	private static ResultScanner scanHBaseTable(HTable hTable,
			ArrayList<String> families) {
		return scanHBaseTable(hTable, families, null, null);
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

	private static HashMap<String, HashMap<String, String>> mergeSpeedAndServing(
			ResultScanner servingLayer, ResultScanner speedLayer)
			throws IOException {

		// HashMap sensorID=>Map{timestamp => timestamp val, value => value val
		HashMap<String, HashMap<String, String>> mergedResults = new HashMap<String, HashMap<String, String>>();

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

			// map used for getting values
			HashMap<String, String> rowValues = new HashMap<String, String>();

			// String rowKeySplit = rowKey.
			String value = Bytes.toString(result.getValue(Bytes.toBytes("d"),
					Bytes.toBytes("val")));

			// check if there is an entry that has servingLayerSensorId.
			if (!(mergedResults.containsKey(servingLayerSensorId))) {
				// create the entry for servingLayerSensorId
				mergedResults.put(servingLayerSensorId,
						new HashMap<String, String>());
			}

			// get the map and add in value
			rowValues = mergedResults.get(servingLayerSensorId);
			rowValues.put(servingLayerTimestamp, value);
			servingLayerResultsCount++;

			// create new entry in merge

			// update the results map
			mergedResults.put(servingLayerSensorId, rowValues);
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

			// check if serving layer has record for that sensor
			if (mergedResults.containsKey(speedLayerSensorId)) {
				// check if the sensor has record with that timestamp
				if (mergedResults.get(speedLayerSensorId).containsKey(
						speedLayerTimestamp)) {
					continue;
				}
				// doesnt have record with timestamp
				else {
					// get the sensors map and add the timstamp an value
					HashMap<String, String> sensorMap = mergedResults
							.get(speedLayerSensorId);
					sensorMap.put(speedLayerTimestamp, speedLayerValue);
					
					speedLayerResultsCount++;
				}
			}
			// serving layer had no record of that timestamp. creat it all from
			// scratch
			else {
				HashMap<String, String> newSensorMap = new HashMap<String, String>();
				newSensorMap.put(speedLayerTimestamp, speedLayerValue);
				mergedResults.put(speedLayerSensorId, newSensorMap);
				
				speedLayerResultsCount++;;
			}
		}

		System.out.println("****Summary****");
		System.out.println("Number in merged results " + servingLayerResultsCount + speedLayerResultsCount);
		System.out.println("Number in serving results " + servingLayerResultsCount );
		System.out.println("Number in speed results " + speedLayerResultsCount);

		return mergedResults;
	}

	private static JSONArray toJSON(
			HashMap<String, HashMap<String, String>> results) {
		JSONArray resultsArray = new JSONArray();

		Set<String> sensorIds = results.keySet();

		for (String sensorId : sensorIds) {
			HashMap<String, String> sensorReadings = results.get(sensorId);

			Set<String> timestamps = sensorReadings.keySet();

			JSONObject obj = new JSONObject();

			for (String timestamp : timestamps) {
				String value = sensorReadings.get(timestamp);

				try {
					obj.put("sensorID", sensorId);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					obj.put("timestamp", timestamp);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					obj.put("value", value);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				resultsArray.put(obj);
			}
		}

		return resultsArray;
	}
}
