/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.bpl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * Get a client configuration JSON file given the file name.
 * @author mshankar
 *
 */
public class GetClientConfiguration implements BPLAction {
	private static Logger logger = LogManager.getLogger(GetClientConfiguration.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		if(!configService.getInstallationProperties().containsKey("org.epics.archiverappliance.retrieval.bpl.GetClientConfiguration.DocumentRoot"))  {
			logger.error("This installation has not been configured to serve archiver config files.");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		String configFileName = req.getParameter("configFile");
		if(configFileName == null || configFileName.equals("") || configFileName.contains("..")) { 
			logger.error("The config file has not been specified (correctly).");
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
		
		if(!Files.exists(configFilePath)) { 
			logger.error("The archive viewer config file does not exist on the file system " + configFilePath.toString());
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		try (OutputStream out = resp.getOutputStream()) {
			try(FileInputStream fis = new FileInputStream(configFilePath.toFile())) { 
				byte[] buf = new byte[10*1024];
				int bytesRead = fis.read(buf);
				while(bytesRead > 0) {
					out.write(buf, 0, bytesRead);
					bytesRead = fis.read(buf);
				}
			}
		}
	}

}
