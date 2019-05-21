/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.bpl.cahdlers.ArchivesHandler;
import org.epics.archiverappliance.mgmt.bpl.cahdlers.InfoHandler;
import org.epics.archiverappliance.retrieval.channelarchiver.XMLRPCClient;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Add a external Archiver Data Server into the system.
 * We expect two arguments
 * <ol>
 * <li><code>externalarchiverserverurl</code> - For Channel Archivers, this is the URL to the XML-RPC server. For other EPICS Archiver Appliance clusters, this is the <code>data_retrieval_url</code> of the cluster as defined in the <code>appliances.xml</code>.</li>
 * <li><code>externalServerType</code> - For Channel Archivers, this is the string <code>CA_XMLRPC</code>. For other EPICS Archiver Appliance clusters, this is the string <code>ARCHAPPL_PBRAW</code>.</li>
 * </ol>
 * @author mshankar
 *
 */
public class AddExternalArchiverServer implements BPLAction {
	private static Logger logger = Logger.getLogger(AddExternalArchiverServer.class.getName());
	
	enum ExternalServerType { 
		CA_XMLRPC,
		ARCHAPPL_PBRAW
	}

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,ConfigService configService) throws IOException {
		String serverUrl = req.getParameter("externalarchiverserverurl");
		if(serverUrl == null || serverUrl.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		String externalServerTypeStr = req.getParameter("externalServerType");
		if(externalServerTypeStr == null || externalServerTypeStr.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		ExternalServerType externalServerType = ExternalServerType.valueOf(externalServerTypeStr);
		
		logger.info("Adding External Archiver Server " + serverUrl);
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		
		// Check to see if we already have this server...
		for(String existingServerURL : configService.getExternalArchiverDataServers().keySet()) {
			if(existingServerURL.equals(serverUrl)) {
				logger.info("We already have the External Archiver server " + serverUrl);
				resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
				infoValues.put("validation", "We already have the server " + serverUrl + " in the system.");
				try(PrintWriter out = resp.getWriter()) {
					out.println(JSONValue.toJSONString(infoValues));
				}
				return;
			}
		}
		
		if(externalServerType == ExternalServerType.CA_XMLRPC) {
			logger.debug("Getting the available indexes from " + serverUrl);
			InfoHandler handler = new InfoHandler();
			try {
				XMLRPCClient.archiverInfo(serverUrl, handler);
				ArchivesHandler archivesHandler = new ArchivesHandler();
				XMLRPCClient.archiverArchives(serverUrl, archivesHandler);
				infoValues.put("archives", archivesHandler.getArchives());
			} catch(Exception ex) {
				logger.error("Exception adding Channel Archiver server " + serverUrl, ex);
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				return;
			}

			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			infoValues.put("Connected", "ok");
			infoValues.put("desc", handler.getDesc());
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}			
		} else { 
			logger.debug("Testing connectivity for external EPICS Archiver Appliance at " + serverUrl);
			try { 
				URL url = new URL(serverUrl + "/ping");
				try(InputStream is = url.openStream()) { 
					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					byte[] buf = new byte[1024];
					int bytesRead = is.read(buf);
					while(bytesRead > 0) {
						bos.write(buf, 0, bytesRead);
						bytesRead = is.read(buf);
					}
					String pingresponse = bos.toString();
					logger.info("Response from external EPICS Archiver Appliance at " + serverUrl + " => " + pingresponse);
					if(pingresponse.contains("Pong")) { 
						resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
						infoValues.put("Connected", "ok");
						infoValues.put("desc", "EPICS Archiver Appliance at " + serverUrl);
						infoValues.put("archives", "pbraw");
						try(PrintWriter out = resp.getWriter()) {
							out.println(JSONValue.toJSONString(infoValues));
						}			
					} else { 
						resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
						infoValues.put("validation", "Unexpected response " + pingresponse + " from the EPICS Archiver Appliance at " + serverUrl);
						try(PrintWriter out = resp.getWriter()) {
							out.println(JSONValue.toJSONString(infoValues));
						}			
					}
				}
			} catch(IOException ex) { 
				logger.error("Exception pinging the external EPICS Archiver Appliance", ex);
				resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
				infoValues.put("validation", "Exception pinging the external EPICS Archiver Appliance at " + serverUrl + " " + ex.getMessage());
				try(PrintWriter out = resp.getWriter()) {
					out.println(JSONValue.toJSONString(infoValues));
				}
				return;
			}
		}
	}
}
