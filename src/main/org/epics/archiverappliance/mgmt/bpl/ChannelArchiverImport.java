package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ChannelArchiver.PVConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;

public class ChannelArchiverImport {
	private static final Logger logger = Logger.getLogger(ChannelArchiverImport.class);
	
	public static void importPV(PVConfig pvConfig, PrintWriter out, List<String> fieldsAsPartOfStream, boolean skipCapacityPlanning, ConfigService configService) throws IOException {
		boolean scan = !pvConfig.isMonitor();
		float samplingPeriod = pvConfig.getPeriod();
		if (logger.isDebugEnabled()) {
			String msg = "Adding " + pvConfig.getPVName() + " using " + (scan ? SamplingMethod.SCAN : SamplingMethod.MONITOR) + " and a period of " + samplingPeriod;
			if (pvConfig.getPolicy() != null) {
				msg = msg + " and policy " + pvConfig.getPolicy();
			}
			logger.debug(msg);
		}
		ArchivePVAction.archivePV(out, pvConfig.getPVName(), true, scan ? SamplingMethod.SCAN : SamplingMethod.MONITOR, samplingPeriod, null, pvConfig.getPolicy(), null, skipCapacityPlanning, configService, fieldsAsPartOfStream);
	}
}
