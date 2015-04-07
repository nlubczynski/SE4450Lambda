package com.se4450.merge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

public final class HBaseScannerUtilities {
	private HBaseScannerUtilities() {
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
	public static String formatStartRowKeyString(String id,
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
	public static String formatEndRowKeyString(String id, String timestampEnd) {

		String endRowKeyString = null;

		if (timestampEnd != null && timestampEnd != "") {
			// format to ensure data at current timestamp is retrieved
			timestampEnd = formatTimestampEnd(timestampEnd);
		} else {
			// format to ensure all data for the given timestamp isretrieved
			id = formatId(id);
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
	public static String formatTimestampStart(String timestamp) {

		if (timestamp != null && timestamp != "")
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
	public static String formatTimestampEnd(String timestamp) {

		if (timestamp == null || timestamp == "")
			return timestamp;

		timestamp = String.format("%013d", Long.parseLong(timestamp) + 1);

		return timestamp;
	}

	/**
	 * Formats an id to be used as the end bound filter string in HBase scan.
	 * Adds 1 to sensorId and a dash delimiter.
	 * 
	 * @param id
	 *            to be formated
	 * @return a formated string to be used in HBase scan
	 */
	public static String formatId(String id) {
		if (id != null && id != "") {
			id = String.valueOf((Long.parseLong(id) + 1) + "-");
		}
		return id;
	}

	/**
	 * Builds the row key filter string based on an id and timestamp. Does not
	 * manipulate parameters.
	 * 
	 * @param id
	 *            to be used in filter string
	 * @param timestamp
	 *            to be used in filter string. Already formatted
	 * @return a formatted value to use HBase scan
	 */
	public static String buildRowKeyFilterString(String id, String timestamp) {

		StringBuilder sb = new StringBuilder();

		// if no id present then return null as all data should be retrieved
		if (id == null || id == "")
			return id;
		// id and timestamp
		else if (timestamp != null && timestamp != "") {

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
	public static HTable getTableReference(String tableName, Configuration conf) {
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

}
