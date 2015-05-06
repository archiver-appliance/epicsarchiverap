/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.ui;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;

/**
 * A servlet that uses velocity as a template engine.
 * @author mshankar
 *
 */
@SuppressWarnings("serial")
public class TemplatedContentServlet extends HttpServlet {
	private static Logger logger = Logger.getLogger(TemplatedContentServlet.class.getName());
	private VelocityEngine ve = null;
	private ConfigService configService = null;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String path = req.getPathInfo();
		logger.info("Path info for request is " + path);
		
		if(path.startsWith("/js/") || 
				path.startsWith("/css/")
		) {
			File f = new File(this.getServletContext().getRealPath("WEB-INF/classes/templates/" + path));
			if(f.exists()) {
				if(path.startsWith("/js/")) {
					resp.setContentType("application/javascript");
				} else if(path.startsWith("/css/")) {
					resp.setContentType("text/css");
				}
				resp.setContentLength((int) f.length());
				resp.addHeader("Cache-Control", "max-age=86400"); // 86400 = 1 day
				copyFileToResponse(f, resp);
				return;
			} else {
				logger.error("Invalid request " + f.getAbsolutePath());
				resp.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
		}
		
		String pvName = req.getParameter("pv");
		String startTimeStr = req.getParameter("from"); 
		String endTimeStr = req.getParameter("to");
		
		HashMap<String, String[]> otherParams = new HashMap<String, String[]>(req.getParameterMap());
		otherParams.remove("pv");
		otherParams.remove("from");
		otherParams.remove("to");
		
		if(pvName == null) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		
		ApplianceInfo applianceForPV = configService.getApplianceForPV(pvName);
		if(applianceForPV == null) { 
			String realName = configService.getRealNameForAlias(pvName);
			if(realName != null) { 
				applianceForPV = configService.getApplianceForPV(realName);
			}
		}
		if(!applianceForPV.equals(configService.getMyApplianceInfo())) {
			try {
				logger.debug("Data for appliance is elsewhere. Redirecting to appliance " + applianceForPV.getIdentity());
				URI redirectURI = new URI(applianceForPV.getRetrievalURL() + "/../content/" + req.getPathInfo());
				String redirectURIStr = redirectURI.normalize().toString() +  "?" + req.getQueryString();
				logger.debug("URI for redirect is " + redirectURIStr);
				resp.sendRedirect(redirectURIStr);
				return;
			} catch(URISyntaxException ex) {
				throw new IOException(ex);
			}
		}

		
		Timestamp end = TimeUtils.plusHours(TimeUtils.now(), 1);
		if(endTimeStr != null) {
			end = TimeUtils.convertFromISO8601String(endTimeStr);
		}
		
		// We get one shift + 1 hr by default
		// This should not be too slow to render using javascript in a browser.
		// One days worth of data is 86400 samples for a 1Hz DBR_DOUBLE and takes a while on the client side.
		Timestamp start = TimeUtils.minusHours(end, 9);
		if(startTimeStr != null) {
			start = TimeUtils.convertFromISO8601String(startTimeStr);
		}
		
		logger.info("Getting data for PV " + pvName + " from " + TimeUtils.convertToISO8601String(start) + " to " + TimeUtils.convertToISO8601String(end));
		
		// We set up the previous and next times as well
		long startMillis = start.getTime();
		long endMillis = end.getTime();
		long duration = endMillis - startMillis;
		long prevStartMillis = startMillis - duration;
		long nextEndMillis = endMillis + duration;
		
		VelocityContext context = new VelocityContext();
		context.put("pv", pvName);
		context.put("start", TimeUtils.convertToISO8601String(new Timestamp(startMillis)));
		context.put("end", TimeUtils.convertToISO8601String(new Timestamp(endMillis)));
		context.put("prevStart", TimeUtils.convertToISO8601String(new Timestamp(prevStartMillis)));
		context.put("nextEnd", TimeUtils.convertToISO8601String(new Timestamp(nextEndMillis)));
		context.put("otherparams", otherParams);
		context.put("humanreadablestart", TimeUtils.convertToHumanReadableString(new Timestamp(startMillis)));
		context.put("humanreadableend", TimeUtils.convertToHumanReadableString(new Timestamp(endMillis)));

		logger.info("Processing request for templated content");
		resp.setContentType("text/html");
		Writer out = resp.getWriter();
		Template template = ve.getTemplate("templates/testjson.html");
		template.merge( context, out);
		out.close();
	}

	@Override
	public void init() throws ServletException {
		super.init();
		ve = new VelocityEngine();
		ve.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.log.SimpleLog4JLogSystem" );
		ve.setProperty("runtime.log.logsystem.log4j.category", "velocity");
		ve.setProperty("file.resource.loader.path", this.getServletContext().getRealPath("/WEB-INF/classes"));
		ve.init();
		this.configService = (ConfigService) this.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
	}
	
	
	private static void copyFileToResponse(File f, HttpServletResponse resp) throws IOException {
		InputStream is = null;
		OutputStream os = null;
		try {
			is = new FileInputStream(f);
			os = resp.getOutputStream();
			IOUtils.copy(is, os);
		} finally {
			if(os != null) try { os.close(); os = null; } catch (Throwable t) {} 
			if(is != null) try { is.close(); is = null; } catch (Throwable t) {} 
		}
	}
}
