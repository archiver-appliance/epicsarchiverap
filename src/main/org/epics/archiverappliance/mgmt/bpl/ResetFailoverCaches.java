package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;

/**
 * 
 * @epics.BPLAction - Reset the failover caches for all the retrieval components in this cluster.  
 * @epics.BPLActionEnd
 *
 * Each retrieval component in a cluster caches the PV's from remote failover appliances.
 * These caches contain one entry for each PV in this appliance indicating if the PV is being archived in the remote appliance.
 * This information is cached using a TTL to minimize the impact on the remote failover appliance. 
 * This method manually unloads this cache on all the retrieval components.
 * 
 * @author mshankar
 *
 */
public class ResetFailoverCaches implements BPLAction {
	private static Logger logger = LogManager.getLogger(ResetFailoverCaches.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		LinkedList<String> retrievalURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			retrievalURLs.add(info.getRetrievalURL() + "/resetFailoverCachesForThisAppliance");
		}
		logger.info("Reseting the failover caches.");
		try(PrintWriter out = resp.getWriter()) {
			GetUrlContent.combineJSONArraysAndPrintln(retrievalURLs, out);
		} 
	}
}
