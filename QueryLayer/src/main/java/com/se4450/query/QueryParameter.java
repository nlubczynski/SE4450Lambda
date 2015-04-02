package com.se4450.query;

import java.util.ArrayList;
import java.util.List;

public class QueryParameter {

	private List<String> m_sensorIDList;
	private String m_buildingID;
	private String m_start;
	private String m_end;

	public QueryParameter() {

		m_sensorIDList = new ArrayList<String>();
		m_buildingID = null;
		m_start = null;
		m_end = null;
	}

	public boolean isEmpty() {
		if (m_sensorIDList.size() == 0 && (m_buildingID == null || m_buildingID == ""))
			return true;

		return false;
	}

	public void parseQueryString(String queryString) {
		String[] queryParameters = null;

		queryParameters = queryString.split("&");

		for (String parameter : queryParameters) {
			String[] parameterSplit = parameter.split("=");

			String parameterId = parameterSplit[0];
			String parameterValue = parameterSplit[1];

			if (parameterId.equals("sensorID")) {
				String[] idList = parameterValue.split(",");
				for (String id : idList) {

					m_sensorIDList.add(id);
				}

			} else if (parameterId.equals("start")) {
				m_start = parameterValue;
			} else if (parameterId.equals("end")) {
				m_end = parameterValue;
			} else if (parameterId.equals("buildingID")) {
				m_buildingID = parameterValue;
			}
		}
	}

	public List<String> getSensorIDList() {
		return m_sensorIDList;
	}

	public void setSensorIDList(List<String> m_sensorIDList) {
		this.m_sensorIDList = m_sensorIDList;
	}

	public String getBuildingID() {
		return m_buildingID;
	}

	public void setBuildingID(String m_buildingID) {
		this.m_buildingID = m_buildingID;
	}

	public String getStart() {
		return m_start;
	}

	public void setStart(String m_start) {
		this.m_start = m_start;
	}

	public String getEnd() {
		return m_end;
	}

	public void setEnd(String m_end) {
		this.m_end = m_end;
	}
}
