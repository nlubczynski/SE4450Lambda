package com.se4450.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;

public class Query {
	private String m_rowKeyStart;
	private String m_rowKeyEnd;

	public Query() {

	}

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
	public JSONArray querySensorData(String sensorId, String timestampStart,
			String timestampEnd) {

		m_rowKeyStart = HBaseScannerUtilities.formatStartRowKeyString(sensorId,
				timestampStart);
		m_rowKeyEnd = HBaseScannerUtilities.formatEndRowKeyString(sensorId,
				timestampEnd);

		Set<Reading> resultSet = getData(m_rowKeyStart, m_rowKeyEnd);

		return Reading.toJSON(resultSet);
	}

	/**
	 * Public method server calls to get all data
	 * 
	 * @return JSONArray of all data in Serving and Speed layers
	 */
	public JSONArray queryAllData() {

		Set<Reading> resultSet = getData(null, null);

		return Reading.toJSON(resultSet);
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
	public JSONArray queryBuildingData(String buildingID,
			String timestampStart, String timestampEnd) {

		m_rowKeyStart = HBaseScannerUtilities.formatStartRowKeyString(
				buildingID, timestampStart);
		m_rowKeyEnd = HBaseScannerUtilities.formatEndRowKeyString(buildingID,
				timestampEnd);

		Set<Reading> resultSet = getBuildingData(m_rowKeyStart, m_rowKeyEnd);

		return Reading.toJSON(resultSet);
	}

	/**
	 * Gets all data for building in serving and speed layers based on two row
	 * key filters
	 * 
	 * @return JSONArray of data
	 */
	private Set<Reading> getBuildingData(String startRowKeyString,
			String endRowKeyString) {

		Configuration conf = HBaseConfiguration.create();

		conf = Utilities.loadHBaseConfiguration(conf);

		// get table references
		HTable servingLayerTable = HBaseScannerUtilities.getTableReference(
				"BuildingServingLayer", conf);
		HTable speedLayerTable = HBaseScannerUtilities.getTableReference(
				"BuildingSpeedLayer", conf);

		HTable speedLayerTable2 = HBaseScannerUtilities.getTableReference(
				"BuildingSpeedLayer2", conf);

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
			results = Merge.mergeSpeedAndServingBuilding(
					servingLayerTableResults, speedLayerTableResults,
					speedLayer2TableResults);
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
	private Set<Reading> getData(String startRowKeyString,
			String endRowKeyString) {
		Configuration conf = HBaseConfiguration.create();

		conf = Utilities.loadHBaseConfiguration(conf);

		// get table references
		HTable servingLayerTable = HBaseScannerUtilities.getTableReference(
				"SensorValuesServingLayer", conf);
		HTable speedLayerTable = HBaseScannerUtilities.getTableReference(
				"SensorValuesSpeedLayer", conf);

		HTable speedLayerTable2 = HBaseScannerUtilities.getTableReference(
				"SensorValuesSpeedLayer2", conf);

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
			results = Merge.mergeSpeedAndServing(servingLayerTableResults,
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
	private ResultScanner scanHBaseTable(HTable hTable,
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

}
