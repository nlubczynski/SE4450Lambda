package com.se4450.query;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;

import com.se4450.merge.Query;

/**
 * Servlet implementation class MergeLayerREST
 */
@WebServlet(name = "QueryLayerMerge", urlPatterns = { "/QueryLayerMerge" })
public class QueryLayerREST extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public QueryLayerREST() {
		super();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {

		Query query = new Query();
		
		JSONArray requestResponse = null;

		String queryString = request.getQueryString();
		String[] queryParameters = null;

		// if there was no query parameter get all data
		if (queryString == null || queryString.isEmpty()) {
			requestResponse = query.queryAllData();
		}

		// if there was a query parameter parse and build query
		else {
			queryParameters = queryString.split("&");
			ArrayList<String> sensorIdRequested = new ArrayList<String>();
			String startRowKeyRequested = null;
			String endRowKeyRequeted = null;
			String buildingID = null;

			for (String parameter : queryParameters) {
				String[] parameterSplit = parameter.split("=");

				String parameterId = parameterSplit[0];
				String parameterValue = parameterSplit[1];

				if (parameterId.equals("sensorID")) {
					String[] idList = parameterValue.split(",");
					for (String id : idList) {

						sensorIdRequested.add(id);
					}

				} else if (parameterId.equals("start")) {
					startRowKeyRequested = parameterValue;
				} else if (parameterId.equals("end")) {
					endRowKeyRequeted = parameterValue;
				} else if (parameterId.equals("buildingID")) {
					buildingID = parameterValue;
				}

			}
			requestResponse = new JSONArray();

			// if sensor id was requested loop through and get data for each
			// sensor
			if (sensorIdRequested.size() > 0) {
				for (int i = 0; i < sensorIdRequested.size(); i++) {
					JSONArray newRequestResponse = query.querySensorData(sensorIdRequested.get(i),
									startRowKeyRequested, endRowKeyRequeted);

					for (int j = 0; j < newRequestResponse.length(); j++) {
						try {
							requestResponse.put(newRequestResponse
									.getJSONObject(j));
						} catch (JSONException e) {
							e.printStackTrace();
						}
					}
				}
			}
			// no sensor means a building id was requested so get all data for
			// building
			else {
				requestResponse = query.queryBuildingData(
						buildingID, startRowKeyRequested, endRowKeyRequeted);
			}
		}

		response.setContentType("application/json");

		PrintWriter out = response.getWriter();

		out.println(requestResponse);

		out.flush();
	}
}
