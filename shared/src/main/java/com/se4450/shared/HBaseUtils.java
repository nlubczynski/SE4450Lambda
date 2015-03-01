package com.se4450.shared;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class HBaseUtils {
	/**
	 * Parses the file passed in and places the hbase configuration keys in the
	 * hbConf object
	 * 
	 * @param fileName
	 *            The location/name of the hbase-site.xml file containing the
	 *            hbase keys
	 * @param hbConf
	 *            A Hash map to place the key/value pairs from the xml document
	 *            in
	 * @return
	 */
	public static boolean LoadHBaseSiteData(String fileName,
			Map<String, Object> hbConf) {

		File hbaseSiteXml = new File(fileName);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		Document doc;

		try {
			dBuilder = dbFactory.newDocumentBuilder();
			doc = dBuilder.parse(hbaseSiteXml);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
			return false;
		} catch (SAXException | IOException e) {
			e.printStackTrace();
			return false;
		}

		// Get and loop all over all the properties
		NodeList properties = doc.getElementsByTagName("property");
		for (int index = 0; index < properties.getLength(); ++index) {
			Element property = (Element) properties.item(index);
			hbConf.put(
					// put <name, value> in the hash map
					property.getElementsByTagName("name").item(0)
							.getTextContent(),
					property.getElementsByTagName("value").item(0)
							.getTextContent());
		}

		return true;
	}
}
