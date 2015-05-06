package org.epics.archiverappliance.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;

import javax.servlet.ServletContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Parses the appliances.xml file and loads the appliances
 * @author mshankar
 *
 */
public class AppliancesList {
	private static Logger logger = Logger.getLogger(AppliancesList.class.getName());
	
	/**
	 * Parses the appliances.xml file and loads the appliances into the specified appliancesList
	 * @param appliancesList
	 */
	public static HashMap<String, ApplianceInfo> loadAppliancesXML(ServletContext servletContext) throws IOException, ConfigException {
		HashMap<String, ApplianceInfo> appliancesList = new HashMap<String, ApplianceInfo>();
		try(InputStream appliancesXMLInputStream = determineApplianceXMLFileAndReturnStream(servletContext)) {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse (appliancesXMLInputStream);

            NodeList applianceList = doc.getElementsByTagName("appliance");
            int totalAppliances = applianceList.getLength();
            logger.debug("Found " + totalAppliances + " appliances in appliances.xml");
            
            HashSet<String> allInetPorts = new HashSet<String>();
            
            for(int i = 0; i < totalAppliances; i++) {
            	Node applianceNode = applianceList.item(i);
            	String identity = getChildNodeTextContent(applianceNode, "identity", i);
            	String cluster_inetport = getChildNodeTextContent(applianceNode, "cluster_inetport", i);
            	String mgmt_url = getChildNodeTextContent(applianceNode, "mgmt_url", i);
            	String engine_url = getChildNodeTextContent(applianceNode, "engine_url", i);
            	String etl_url = getChildNodeTextContent(applianceNode, "etl_url", i);
            	String retrieval_url = getChildNodeTextContent(applianceNode, "retrieval_url", i);
            	String data_retrieval_url = getChildNodeTextContent(applianceNode, "data_retrieval_url", i);
            	ApplianceInfo applianceInfo = new ApplianceInfo(identity, mgmt_url, engine_url, retrieval_url, etl_url, cluster_inetport, data_retrieval_url);
            	if(appliancesList.containsKey(identity)) { 
        			String msg = "We have more than one appliance with identity " + identity + ". This is probably a cut and paste typo; please fix this.";
					logger.fatal(msg);
					throw new ConfigException(msg);
            	}
            	appliancesList.put(identity, applianceInfo);
            	
            	if(!cluster_inetport.startsWith("localhost")) { 
            		if(allInetPorts.contains(cluster_inetport)) { 
            			String msg = "When adding appliance with identity " + identity + ", we already have another appliance with the same cluster_inetport " + cluster_inetport + ". This is probably a cut and paste typo.";
						logger.fatal(msg);
						throw new ConfigException(msg);
            		} else { 
            			allInetPorts.add(cluster_inetport);
            		}
            	}
            }
		} catch(Exception ex) {
			throw new IOException("Exception parsing appliance.xml", ex);
		}
		return appliancesList;
	}

	private static InputStream determineApplianceXMLFileAndReturnStream(ServletContext servletContext) throws IOException, FileNotFoundException {
		String applianceFileFromEnvVar = System.getenv(ConfigService.ARCHAPPL_APPLIANCES);
		if(applianceFileFromEnvVar == null || applianceFileFromEnvVar.equals("")) {
			applianceFileFromEnvVar = System.getProperty(ConfigService.ARCHAPPL_APPLIANCES);
		}
		if(applianceFileFromEnvVar != null) {
			logger.info("appliances.xml file specified in the environment as " + applianceFileFromEnvVar);
			File appliancesXMLFile = new File(applianceFileFromEnvVar);
			if(!appliancesXMLFile.exists()) {
				String msg = "Specified appliances.xml file " + applianceFileFromEnvVar + " does not seem to exist. This is a fatal error; cannot continue";
				logger.fatal(msg);
				throw new IOException(msg);
			}
			return new FileInputStream(appliancesXMLFile);
		} else {
			logger.info("Environment variable " + ConfigService.ARCHAPPL_APPLIANCES + " not specified. Using appliances.xml as found in classpath");
			InputStream appliancesXMLInputStream = servletContext.getResourceAsStream("/WEB-INF/classes/appliances.xml");
			if(appliancesXMLInputStream == null) {
				String msg = "Unable to find appliances.xml file " + servletContext.getRealPath("/WEB-INF/classes/appliances.xml") + " in the servlet classpath. Please copy into WEB-INF/classes/appliances.xml. This is a fatal error; cannot continue";
				logger.fatal(msg);
				throw new IOException(msg);
			}
			return appliancesXMLInputStream;
		}
	}
	
	/**
	 * Gets the test content of the specified element.
	 * Not super efficient, but serves the purpose for this usecase.
	 * @param node
	 * @param elementName
	 * @return
	 */
	private static String getChildNodeTextContent(Node node, String elementName, int applianceNum) throws IOException {
		NodeList childNodes = node.getChildNodes();
		for(int i = 0; i < childNodes.getLength(); i++) {
			Node childNode = childNodes.item(i);
			if(childNode.getNodeName().equals(elementName)) {
				return childNode.getTextContent();
			}
		}
		throw new IOException("Cannot determine " + elementName + " for appliance " + applianceNum + " in appliances.xml ");
	}
}
