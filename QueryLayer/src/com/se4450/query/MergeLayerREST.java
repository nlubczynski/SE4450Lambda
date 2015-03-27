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

/**
 * Servlet implementation class MergeLayerREST
 */
@WebServlet(name = "QueryLayerMerge", urlPatterns = { "/QueryLayerMerge" })
public class MergeLayerREST extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public MergeLayerREST() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub

		JSONArray requestResponse = null;

		String queryString = request.getQueryString();
		String[] queryParameters = null;

		// if there was no query parameter get all data
		if (queryString == null || queryString.isEmpty()) {
			requestResponse = com.se4450.merge.Merge.queryAllData();
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
			// get first value
			requestResponse = new JSONArray();

			if (sensorIdRequested.size() > 0) {
				for (int i = 0; i < sensorIdRequested.size(); i++) {
					JSONArray newRequestResponse = com.se4450.merge.Merge
							.querySensorData(sensorIdRequested.get(i),
									startRowKeyRequested, endRowKeyRequeted);

					for (int j = 0; j < newRequestResponse.length(); j++) {
						try {
							requestResponse.put(newRequestResponse
									.getJSONObject(j));
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else {
				requestResponse = com.se4450.merge.Merge.queryBuildingData(
						buildingID, startRowKeyRequested, endRowKeyRequeted);
			}
		}

		response.setContentType("application/json");

		PrintWriter out = response.getWriter();

		out.println(requestResponse);

		out.flush();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
