package com.se4450.merge;

import org.json.JSONException;
import org.json.JSONObject;

public class Reading {
	private String m_id;
	private String m_timestamp;
	private String m_value;

	public Reading(String id, String timestamp, String value) {
		seId(id);
		set_timestamp(timestamp);
		set_value(value);

	}

	public String getId() {
		return m_id;
	}

	public void seId(String m_id) {
		this.m_id = m_id;
	}

	public String get_timestamp() {
		return m_timestamp;
	}

	public void set_timestamp(String m_timestamp) {
		this.m_timestamp = m_timestamp;
	}

	public String get_value() {
		return m_value;
	}

	public void set_value(String m_value) {
		this.m_value = m_value;
	}

	@Override
	public boolean equals(Object other) {
		if (other == null)
			return false;

		if (!(other instanceof Reading))
			return false;
		Reading otherMyClass = (Reading) other;

		// if both id and timestamp are equal return true
		if (this.m_id.equals(otherMyClass.getId())
				&& this.m_timestamp.equals(otherMyClass.get_timestamp()))
			return true;

		return false;

	}

	@Override
	public int hashCode() {
		int hashCode = this.m_id.hashCode() + this.m_timestamp.hashCode();
		return hashCode;
	}

	public JSONObject serialize() {
		JSONObject obj = new JSONObject();

		try {
			obj.put("sensorID", this.m_id);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		try {
			obj.put("timestamp", this.m_timestamp);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		try {
			obj.put("value", this.m_value);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

		return obj;
	}
}
