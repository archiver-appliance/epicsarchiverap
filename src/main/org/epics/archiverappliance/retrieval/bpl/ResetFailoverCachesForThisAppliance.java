package org.epics.archiverappliance.retrieval.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.json.simple.JSONValue;

/**
 * 
 * Each retrieval component in a cluster caches the PV's from remote failover appliances.
 * These caches contain one entry for each PV in this appliance indicating if the PV is being archived in the remote appliance.
 * This information is cached using a TTL to minimize the impact on the remote failover appliance. 
 * This method manually unloads this cache on this appliance.
 * 
 * @author mshankar
 *
 */
public class ResetFailoverCachesForThisAppliance implements BPLAction {
	private static Logger logger = LogManager.getLogger(ResetFailoverCachesForThisAppliance.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Reseting the failover caches for this appliance");
		try(PrintWriter out = resp.getWriter()) {
			configService.resetFailoverCaches();
			LinkedList<HashMap<String, String>> ret = new LinkedList<HashMap<String, String>>();
			HashMap<String, String> status = new HashMap<String, String>();
			status.put("appliance", configService.getMyApplianceInfo().getIdentity());
			status.put("status", "ok");
			ret.add(status);
			out.println(JSONValue.toJSONString(ret));
		} 
	}
}
