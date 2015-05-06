package org.epics.archiverappliance.mgmt.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class StorageReport implements BPLAction {
	private static Logger logger = Logger.getLogger(StorageReport.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Generating storage report");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			LinkedList<Map<String, String>> result = new LinkedList<Map<String, String>>();
			for(ApplianceInfo info : configService.getAppliancesInCluster()) {
				HashMap<String, String> applianceInfo = new HashMap<String, String>();
				result.add(applianceInfo);
				applianceInfo.put("instance", info.getIdentity());
				int pvCount = 0;
				for(@SuppressWarnings("unused") String pvName : configService.getPVsForThisAppliance()) {
					pvCount++;
				}
				applianceInfo.put("pvCount", Integer.toString(pvCount));

				// The getApplianceMetrics here is not a typo. We redisplay some of the appliance metrics in this page.
				JSONObject engineMetrics = GetUrlContent.getURLContentAsJSONObject(info.getEngineURL() + "/getApplianceMetrics"); 
				JSONObject etlMetrics = GetUrlContent.getURLContentAsJSONObject(info.getEtlURL() + "/getApplianceMetrics");
				JSONObject retrievalMetrics = GetUrlContent.getURLContentAsJSONObject(info.getRetrievalURL() + "/getApplianceMetrics");
				if(engineMetrics != null && etlMetrics != null && retrievalMetrics != null) {
					logger.debug("All of the components are working for " + info.getIdentity());
					applianceInfo.put("status", "Working");
				} else {
					logger.debug("At least one of the components is not working for " + info.getIdentity());
					StringWriter buf = new StringWriter();
					buf.append("Stopped - ");
					if(engineMetrics == null) buf.append("engine ");
					if(etlMetrics == null) buf.append("ETL ");
					if(retrievalMetrics == null) buf.append("retrieval ");
					applianceInfo.put("status", buf.toString());
				}
				
				GetUrlContent.combineJSONObjects(applianceInfo, engineMetrics);
				GetUrlContent.combineJSONObjects(applianceInfo, etlMetrics);
				GetUrlContent.combineJSONObjects(applianceInfo, retrievalMetrics);
				
				applianceInfo.put("capacityUtilized", "N/A");
			}
			out.println(JSONValue.toJSONString(result));
		}
	}
}
