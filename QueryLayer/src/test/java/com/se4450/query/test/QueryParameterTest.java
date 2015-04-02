package com.se4450.query.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.se4450.query.QueryParameter;

public class QueryParameterTest {

	@Test
	public void testIsEmpty_emptySensorIDList() {
		QueryParameter query = new QueryParameter();

		List<String> sensorIDList = new ArrayList<String>();
		String buildingID = "1";

		query.setSensorIDList(sensorIDList);
		query.setBuildingID(buildingID);

		boolean actual = query.isEmpty();
		boolean expected = false;

		assertEquals(expected, actual);
	}

	@Test
	public void testIsEmpty_emptyBuildingID() {
		QueryParameter query = new QueryParameter();

		List<String> sensorIDList = new ArrayList<String>();
		sensorIDList.add("1");

		String buildingID = "";

		query.setSensorIDList(sensorIDList);
		query.setBuildingID(buildingID);

		boolean actual = query.isEmpty();
		boolean expected = false;

		assertEquals(expected, actual);
	}

	@Test
	public void testIsEmpty_nullBuildingID() {
		QueryParameter query = new QueryParameter();

		List<String> sensorIDList = new ArrayList<String>();
		sensorIDList.add("1");

		String buildingID = null;

		query.setSensorIDList(sensorIDList);
		query.setBuildingID(buildingID);

		boolean actual = query.isEmpty();
		boolean expected = false;

		assertEquals(expected, actual);
	}

	@Test
	public void testIsEmpty_bothEmpty() {
		QueryParameter query = new QueryParameter();

		List<String> sensorIDList = new ArrayList<String>();

		String buildingID = "";

		query.setSensorIDList(sensorIDList);
		query.setBuildingID(buildingID);

		boolean actual = query.isEmpty();
		boolean expected = true;

		assertEquals(expected, actual);
	}

	@Test
	public void testIsEmpty_listEmptyBuildingIDNull() {
		QueryParameter query = new QueryParameter();

		List<String> sensorIDList = new ArrayList<String>();

		String buildingID = null;

		query.setSensorIDList(sensorIDList);
		query.setBuildingID(buildingID);

		boolean actual = query.isEmpty();
		boolean expected = true;

		assertEquals(expected, actual);
	}

	@Test
	public void testParseQueryString_onlySensorID() {
		String queryString = "sensorID=1";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = null;
		String startExpected = null;
		String endExpected = null;
		List<String> sensorIDListExpected = new ArrayList<String>();
		sensorIDListExpected.add("1");
		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
	
	@Test
	public void testParseQueryString_multipleSensorIDs() {
		String queryString = "sensorID=1,2,3";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = null;
		String startExpected = null;
		String endExpected = null;
		List<String> sensorIDListExpected = new ArrayList<String>();
		sensorIDListExpected.add("1");
		sensorIDListExpected.add("2");
		sensorIDListExpected.add("3");
		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
	
	@Test
	public void testParseQueryString_buildingID() {
		String queryString = "buildingID=1";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = "1";
		String startExpected = null;
		String endExpected = null;
		List<String> sensorIDListExpected = new ArrayList<String>();

		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
	
	@Test
	public void testParseQueryString_end() {
		String queryString = "end=123";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = null;
		String startExpected = null;
		String endExpected = "123";
		List<String> sensorIDListExpected = new ArrayList<String>();

		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
	
	@Test
	public void testParseQueryString_start() {
		String queryString = "start=123";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = null;
		String startExpected = "123";
		String endExpected = null;
		List<String> sensorIDListExpected = new ArrayList<String>();
		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
	
	@Test
	public void testParseQueryString_buildingStartEnd() {
		String queryString = "buildingID=1&start=123&end=123";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = "1";
		String startExpected = "123";
		String endExpected = "123";
		List<String> sensorIDListExpected = new ArrayList<String>();
		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
	
	@Test
	public void testParseQueryString_sensorStartEnd() {
		String queryString = "sensorID=1&start=123&end=123";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = null;
		String startExpected = "123";
		String endExpected = "123";
		List<String> sensorIDListExpected = new ArrayList<String>();
		sensorIDListExpected.add("1");
		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
	
	@Test
	public void testParseQueryString_buildingStart() {
		String queryString = "buildingID=1&start=123";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = "1";
		String startExpected = "123";
		String endExpected = null;
		List<String> sensorIDListExpected = new ArrayList<String>();
		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
	
	@Test
	public void testParseQueryString_sensorStart() {
		String queryString = "sensorID=1&start=123";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = null;
		String startExpected = "123";
		String endExpected = null;
		List<String> sensorIDListExpected = new ArrayList<String>();
		sensorIDListExpected.add("1");
		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
	
	@Test
	public void testParseQueryString_buildingEnd() {
		String queryString = "buildingID=1&end=123";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = "1";
		String startExpected = null;
		String endExpected = "123";
		List<String> sensorIDListExpected = new ArrayList<String>();
		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
	@Test
	public void testParseQueryString_sensorEnd() {
		String queryString = "sensorID=1&end=123";
		
		QueryParameter query = new QueryParameter();

		query.parseQueryString(queryString);
		
		String buildingExpected = null;
		String startExpected = null;
		String endExpected = "123";
		List<String> sensorIDListExpected = new ArrayList<String>();
		sensorIDListExpected.add("1");
		
		assertEquals(buildingExpected, query.getBuildingID());
		assertEquals(startExpected, query.getStart());
		assertEquals(endExpected, query.getEnd());
		assertEquals(sensorIDListExpected, query.getSensorIDList());
	}
}
