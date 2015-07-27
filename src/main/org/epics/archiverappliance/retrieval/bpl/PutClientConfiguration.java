/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.bpl;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Puts a client configuration JSON file given the file name.
 * The new configuration is part of te POST body.
 * @author mshankar
 *
 */
public class PutClientConfiguration implements BPLAction {
	private static Logger logger = Logger.getLogger(PutClientConfiguration.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		if(!configService.getInstallationProperties().containsKey("org.epics.archiverappliance.retrieval.bpl.GetClientConfiguration.DocumentRoot"))  {
			logger.error("This installation has not been configured to serve archiver config files.");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

		byte[] newConfigBytes = null;
		try(ByteArrayOutputStream bos = new ByteArrayOutputStream()) { 
			try(InputStream is = new BufferedInputStream(req.getInputStream())) { 
				byte[] buf = new byte[10*1024];
				int bytesRead = is.read(buf);
				while(bytesRead > 0) { 
					bos.write(buf, 0, bytesRead);
					bytesRead = is.read(buf);
				}
			}
			newConfigBytes = bos.toByteArray();
		}
		logger.debug("New configuraiton is of size " + newConfigBytes.length);
		
		String configFileName = req.getParameter("configFile");
		if(configFileName == null 
				|| configFileName.equals("") 
				|| configFileName.contains("..") 
				|| configFileName.startsWith("/") 
				|| !configFileName.endsWith(".json")) { 
			logger.error("The config file has not been specified (correctly) " + configFileName);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		Path documentRoot = Paths.get((String) configService.getInstallationProperties().get("org.epics.archiverappliance.retrieval.bpl.GetClientConfiguration.DocumentRoot"));
		if(!Files.exists(documentRoot)) { 
			logger.error("The document root does not exist " + documentRoot.toString());
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

		Path configFilePath = documentRoot.resolve(configFileName);
		if(!configFilePath.startsWith(documentRoot)) { 
			logger.error("The final path to the config file " + configFilePath + " does not seem to be part of the document root. Denying access to the file as a security precaution.");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		

		if(req.getParameter("download") != null) {
			logger.info("Sending back the incoming data as a content dispositon");
			// Allow applications served from other URL's to access the JSON data from this server.
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			
			resp.setContentType("application/force-download");
			resp.setContentLength(newConfigBytes.length);
			resp.setHeader("Content-Transfer-Encoding", "binary");
			resp.setHeader("Content-Disposition","attachment; filename=\"" + "xxx\"");//fileName);
			
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			resp.setHeader("Content-Disposition", "attachment; filename=" + configFileName);

			try (OutputStream out = resp.getOutputStream()) {
				out.write(newConfigBytes);
			}
			return;
		} else { 
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			try(FileOutputStream fos = new FileOutputStream(configFilePath.toFile(), false)) { 
				fos.write(newConfigBytes);
			}
			HashMap<String, Object> infoValues = new HashMap<String, Object>();
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("status", "ok");
				out.println(JSONValue.toJSONString(infoValues));
			}
		}
		
	}

}
