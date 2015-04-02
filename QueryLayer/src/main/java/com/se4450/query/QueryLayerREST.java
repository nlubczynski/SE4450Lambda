package com.se4450.query;

import java.io.IOException;
import java.io.PrintWriter;

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

		QueryParameter queryParameter = new QueryParameter();
		queryParameter.parseQueryString(queryString);

		// if there was no query parameter get all data
		if (queryParameter.isEmpty()) {
			requestResponse = query.queryAllData();
		}

		// if there was a query parameter parse and build query
		else {
			requestResponse = new JSONArray();

			// if sensor id was requested loop through and get data for each
			// sensor
			if (queryParameter.getSensorIDList().size() > 0) {
				for (int i = 0; i < queryParameter.getSensorIDList().size(); i++) {
					JSONArray newRequestResponse = query.querySensorData(
							queryParameter.getSensorIDList().get(i),
							queryParameter.getStart(), queryParameter.getEnd());

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
						queryParameter.getBuildingID(),
						queryParameter.getStart(), queryParameter.getEnd());
			}
		}

		response.setContentType("application/json");

		PrintWriter out = response.getWriter();

		out.println(requestResponse);

		out.flush();
	}
}
