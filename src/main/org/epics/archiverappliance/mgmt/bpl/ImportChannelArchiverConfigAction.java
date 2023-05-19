/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ChannelArchiver.EngineConfigParser;
import org.epics.archiverappliance.config.ChannelArchiver.PVConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * Use this to import a Channel Archiver configuration file using HTTP POST.
 * @author mshankar
 *
 */
public class ImportChannelArchiverConfigAction implements BPLAction {
	private static final Logger logger = LogManager.getLogger(ImportChannelArchiverConfigAction.class);

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		if(!configService.hasClusterFinishedInitialization()) {
			// If you have defined spare appliances in the appliances.xml that will never come up; you should remove them
			// This seems to be one of the few ways we can prevent split brain clusters from messing up the pv <-> appliance mapping.
			throw new IOException("Waiting for all the appliances listed in appliances.xml to finish loading up their PVs into the cluster");
		}
				InputStream is = null;
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		List<String> fieldsAsPartOfStream = ArchivePVAction.getFieldsAsPartOfStream(configService);
		try(PrintWriter out = resp.getWriter())  {
			boolean isFirst = true;
			out.println("[");
			is = new BufferedInputStream(req.getInputStream());
			is.mark(1024);
			logger.info((new LineNumberReader(new InputStreamReader(is))).readLine());
			is.reset();
			LinkedList<PVConfig> pvConfigs = EngineConfigParser.importEngineConfig(is);
			for(PVConfig pvConfig : pvConfigs) {
				if(isFirst) { isFirst = false; } else { out.println(","); }
				boolean scan = !pvConfig.isMonitor();
				float samplingPeriod = pvConfig.getPeriod();
				if(logger.isDebugEnabled()) logger.debug("Adding " + pvConfig.getPVName() + " using " + (scan ? SamplingMethod.SCAN : SamplingMethod.MONITOR) + " and a period of " + samplingPeriod);
				ArchivePVAction.archivePV(out, pvConfig.getPVName(), true, scan ? SamplingMethod.SCAN : SamplingMethod.MONITOR, samplingPeriod, null, null, null, false, configService, fieldsAsPartOfStream);
			}
			out.println("]");
		} catch(Exception ex) {
			logger.error("Error importing configuration", ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		} finally {
			if(is != null) { try { is.close(); is = null; } catch (Exception ex) {}} 
		}
	}

}
