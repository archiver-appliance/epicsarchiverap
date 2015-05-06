package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.json.simple.JSONValue;

public class ChangeArchivalParamsAction implements BPLAction {
	private static Logger logger = Logger.getLogger(ChangeArchivalParamsAction.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String samplingPeriodStr = req.getParameter("samplingperiod");
		if(samplingPeriodStr == null || samplingPeriodStr.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		float samplingPeriod = PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
		try {
			 samplingPeriod = Float.parseFloat(samplingPeriodStr);
			if(samplingPeriod < 0) {
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
				return;
			}
		} catch(Exception ex) {
			logger.error("Exception parsing sampling period " + samplingPeriodStr, ex);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		String samplingMethodStr = req.getParameter("samplingmethod");
		if(samplingMethodStr == null || samplingMethodStr.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		SamplingMethod samplingMethod = SamplingMethod.valueOf(samplingMethodStr);

		String firstDestURL = req.getParameter("dest");
		if(firstDestURL == null || firstDestURL.equals("")) {
			logger.error("No first destination specified when asking engine to change parameters");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		StoragePlugin firstDest = StoragePluginURLParser.parseStoragePlugin(firstDestURL, configService);


		try {
			logger.debug("Changing the archival parameters for pv " + pvName + " to a sampling period of " + samplingPeriod + " and a sampling method of " + samplingMethod.toString() + " with a first dest of " + firstDest.getDescription());
			ArchiveEngine.changeArchivalParameters(pvName, samplingPeriod, samplingMethod, configService, firstDest);
			HashMap<String, Object> infoValues = new HashMap<String, Object>();
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("status", "ok");
				infoValues.put("desc", "Successfully changed the archival parameters for " + pvName);
				out.println(JSONValue.toJSONString(infoValues));
			}
		} catch(Exception ex) {
			logger.error("Exception changing params for PV" + pvName, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}
		
	}
}
