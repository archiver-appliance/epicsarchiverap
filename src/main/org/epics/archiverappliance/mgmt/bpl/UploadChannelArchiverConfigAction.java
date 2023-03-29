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

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ChannelArchiver.EngineConfigParser;
import org.epics.archiverappliance.config.ChannelArchiver.PVConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;

/**
 * Use this to upload a Channel Archiver configuration file.
 * @author mshankar
 *
 */
public class UploadChannelArchiverConfigAction implements BPLAction {
	private static final Logger logger = Logger.getLogger(UploadChannelArchiverConfigAction.class);

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		if(!configService.hasClusterFinishedInitialization()) {
			// If you have defined spare appliances in the appliances.xml that will never come up; you should remove them
			// This seems to be one of the few ways we can prevent split brain clusters from messing up the pv <-> appliance mapping.
			throw new IOException("Waiting for all the appliances listed in appliances.xml to finish loading up their PVs into the cluster");
		}
				
		// Check that we have a file upload request
		boolean isMultipart = ServletFileUpload.isMultipartContent(req);
		if(!isMultipart) {
			throw new IOException("HTTP request is not sending multipart content; therefore we cannnot process");
		}

		// Create a new file upload handler
		ServletFileUpload upload = new ServletFileUpload();
		List<String> fieldsAsPartOfStream = ArchivePVAction.getFieldsAsPartOfStream(configService);
		try (PrintWriter out = new PrintWriter(new NullOutputStream())) {
			FileItemIterator iter = upload.getItemIterator(req);
			while (iter.hasNext()) {
				FileItemStream item = iter.next();
				String name = item.getFieldName();
				if (item.isFormField()) {
					logger.debug("Form field " + name + " detected.");
				} else {
					logger.debug("File field " + name + " with file name " + item.getName() + " detected.");
					try(InputStream is = new BufferedInputStream(item.openStream())) {
						is.mark(1024);
						logger.info((new LineNumberReader(new InputStreamReader(is))).readLine());
						is.reset();
						LinkedList<PVConfig> pvConfigs = EngineConfigParser.importEngineConfig(is);
						for(PVConfig pvConfig : pvConfigs) {
							boolean scan = !pvConfig.isMonitor();
							float samplingPeriod = pvConfig.getPeriod();
							if(logger.isDebugEnabled()) logger.debug("Adding " + pvConfig.getPVName() + " using " + (scan ? SamplingMethod.SCAN : SamplingMethod.MONITOR) + " and a period of " + samplingPeriod);
							ArchivePVAction.archivePV(out, pvConfig.getPVName(), true, scan ? SamplingMethod.SCAN : SamplingMethod.MONITOR, samplingPeriod, null, null, null, false, configService, fieldsAsPartOfStream);
						}
					} catch(Exception ex) {
						logger.error("Error importing configuration", ex);
						resp.sendRedirect("../ui/integration.html?message=Error importing config file " + item.getName() + " " + ex.getMessage());
						return;
					}
				}
			}
			resp.sendRedirect("../ui/integration.html?message=Successfully imported configuration files");
		} catch(FileUploadException ex) {
			throw new IOException(ex);
		}
		
		
	}

}
