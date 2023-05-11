/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigService.WAR_FILE;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;

/**
 * Glue code that is in all the BPL servlets.
 * @author mshankar
 *
 */
public class BasicDispatcher {
	private static final Logger logger = LogManager.getLogger(BasicDispatcher.class);

	public static void dispatch(HttpServletRequest req, HttpServletResponse resp, ConfigService configService, HashMap<String, Class<? extends BPLAction>> actions) throws IOException {
		String requestPath = req.getPathInfo();
		if(requestPath == null || requestPath.equals("")) { 
			logger.warn("Request path is empty.");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		logger.info("Servicing " + requestPath);
		if(requestPath.equals("/ping")) {
			resp.setContentType("text/plain");
			try(PrintWriter out = resp.getWriter()) {
				out.println("pong");
				return;
			}
		} else if(requestPath.equals("/postStartup")) {
			if(configService.isStartupComplete()) {
				logger.warn("poststartup being called after startup complete");
			} else {
				try {
					configService.postStartup();
				} catch(ConfigException ex) {
					logger.fatal("Exception running postStartup", ex);
					throw new IOException(ex);
				}
			}
			resp.setContentType("text/plain");
			try(PrintWriter out = resp.getWriter()) {
				out.println("Done");
				return;
			}
		} else if(requestPath.equals("/startupState")) {
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			try(PrintWriter out = resp.getWriter()) {
				HashMap<String, String> ret = new HashMap<String, String>();
				ret.put("status", configService.getStartupState().toString());
				out.println(JSONObject.toJSONString(ret));
				return;
			}
		}
		
		if(!configService.isStartupComplete()) {
			logger.warn("We do not let the other actions complete until the config service startup is complete...");
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}
		
		
		if(configService.getWarFile() == WAR_FILE.MGMT && !configService.getMgmtRuntimeState().haveChildComponentsStartedUp()) {
			String header = req.getHeader(GetUrlContent.ARCHAPPL_COMPONENT);
			if(header == null || !header.equals("true")) {
				logger.error("We do not let the actions complete until all the components have started up");
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				return;
			}
		}
		
		Class<? extends BPLAction> actionClass = actions.get(requestPath);
		if(actionClass == null) {
			logger.error("Do not have a appropriate BPL action for " + requestPath + ". Please register the appropriate business method in getActions.");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		BPLAction action;
		try {
			action = actionClass.getConstructor().newInstance();
			action.execute(req, resp, configService);
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
			throw new IOException(e);
		}
	}
}
