package com.se4450.merge.test;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.se4450.merge.HBaseScannerUtilities;

public class HBaseUtilitiesTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testFormatStartRowKeyString() {
		String id = "1";
		String timestampStart = "1234567890123";

		String actual = HBaseScannerUtilities.formatStartRowKeyString(id,
				timestampStart);
		String expected = "1-1234567890123";

		assertEquals(expected, actual);

	}

	@Test
	public void testFormatEndRowKeyString() {
		String id = "1";
		String timestampEnd = "1234567890123";

		String actual = HBaseScannerUtilities.formatEndRowKeyString(id,
				timestampEnd);
		String expected = "1-1234567890124";

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatEndRowKeyString_emptyTimestamp() {
		String id = "1";
		String timestampEnd = "";

		String actual = HBaseScannerUtilities.formatEndRowKeyString(id,
				timestampEnd);
		String expected = "2-";

		assertEquals(expected, actual);
	}
	
	@Test
	public void testFormatEndRowKeyString_nullTimestamp() {
		String id = "1";
		String timestampEnd = null;

		String actual = HBaseScannerUtilities.formatEndRowKeyString(id,
				timestampEnd);
		String expected = "2-";

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatTimestampStart() {
		String timestampStart = "1234567890123";

		String actual = HBaseScannerUtilities
				.formatTimestampStart(timestampStart);
		String expected = "1234567890123";

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatTimestampStart_LessThan13Digits() {
		String timestampStart = "1";

		String actual = HBaseScannerUtilities
				.formatTimestampStart(timestampStart);
		String expected = "0000000000001";

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatTimestampStart_nullTimestamp() {
		String timestampStart = null;

		String actual = HBaseScannerUtilities
				.formatTimestampStart(timestampStart);
		String expected = null;

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatTimestampStart_emptyTimestamp() {
		String timestampStart = "";

		String actual = HBaseScannerUtilities
				.formatTimestampStart(timestampStart);
		String expected = "";

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatTimestampEnd() {
		String timestampEnd = "1234567890123";

		String actual = HBaseScannerUtilities
				.formatTimestampStart(timestampEnd);
		String expected = "1234567890123";

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatTimestampEnd_not13Digits() {
		String timestampEnd = "1";

		String actual = HBaseScannerUtilities
				.formatTimestampStart(timestampEnd);
		String expected = "0000000000001";

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatTimestampEnd_null() {
		String timestampEnd = null;

		String actual = HBaseScannerUtilities
				.formatTimestampStart(timestampEnd);
		String expected = null;

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatTimestampEnd_emptyString() {
		String timestampEnd = "";

		String actual = HBaseScannerUtilities
				.formatTimestampStart(timestampEnd);
		String expected = "";

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatId() {
		String id = "1";

		String actual = HBaseScannerUtilities.formatId(id);
		String expected = "2-";

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatId_null() {
		String id = null;

		String actual = HBaseScannerUtilities.formatId(id);
		String expected = null;

		assertEquals(expected, actual);
	}

	@Test
	public void testFormatId_emptyString() {
		String id = "";

		String actual = HBaseScannerUtilities.formatId(id);
		String expected = "";

		assertEquals(expected, actual);
	}

	@Test
	public void testBuildRowKeyFilterString() {
		String id = "1";
		String timestamp = "1234567890123";

		String actual = HBaseScannerUtilities.buildRowKeyFilterString(id,
				timestamp);
		String expected = "1-1234567890123";

		assertEquals(expected, actual);
	}

	@Test
	public void testBuildRowKeyFilterString_nullID() {
		String id = null;
		String timestamp = "1234567890123";

		String actual = HBaseScannerUtilities.buildRowKeyFilterString(id,
				timestamp);
		String expected = null;

		assertEquals(expected, actual);
	}

	@Test
	public void testBuildRowKeyFilterString_emptyID() {
		String id = "";
		String timestamp = "1234567890123";

		String actual = HBaseScannerUtilities.buildRowKeyFilterString(id,
				timestamp);
		String expected = "";

		assertEquals(expected, actual);
	}

	@Test
	public void testBuildRowKeyFilterString_nullTimestamp() {
		String id = "1";
		String timestamp = null;

		String actual = HBaseScannerUtilities.buildRowKeyFilterString(id,
				timestamp);
		String expected = "1";

		assertEquals(expected, actual);
	}

	@Test
	public void testBuildRowKeyFilterString_emptyTimestamp() {
		String id = "1";
		String timestamp = "";

		String actual = HBaseScannerUtilities.buildRowKeyFilterString(id,
				timestamp);
		String expected = "1";

		assertEquals(expected, actual);
	}
}
