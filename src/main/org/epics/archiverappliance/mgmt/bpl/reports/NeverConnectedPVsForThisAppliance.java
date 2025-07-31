package org.epics.archiverappliance.mgmt.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.MgmtRuntimeState;
import org.epics.archiverappliance.mgmt.MgmtRuntimeState.NeverConnectedRequestState;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

public class NeverConnectedPVsForThisAppliance implements BPLAction {
	private static Logger logger = LogManager.getLogger(NeverConnectedPVsForThisAppliance.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Getting the status of pvs that never connected since the start of this appliance");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		String myIdentity = configService.getMyApplianceInfo().getIdentity();
		MgmtRuntimeState mgmtRunTimeState = configService.getMgmtRuntimeState();
		LinkedList<HashMap<String, String>> result = new LinkedList<HashMap<String, String>>(); 
		try (PrintWriter out = resp.getWriter()) {
			List<NeverConnectedRequestState> NeverConnectedRequestState = mgmtRunTimeState.getNeverConnectedRequests();
			for(NeverConnectedRequestState neverConnectedPV : NeverConnectedRequestState) {
				HashMap<String, String> pvStatus = new HashMap<String, String>();
				result.add(pvStatus);
				pvStatus.put("pvName", neverConnectedPV.getPvName());
				pvStatus.put("requestTime", (neverConnectedPV.getMetInfoRequestSubmitted() != null) ? TimeUtils.convertToHumanReadableString(neverConnectedPV.getMetInfoRequestSubmitted()) : "N/A");
				pvStatus.put("startOfWorkflow", TimeUtils.convertToHumanReadableString(neverConnectedPV.getStartOfWorkflow()));
				pvStatus.put("currentState", neverConnectedPV.getCurrentState().toString());
				pvStatus.put("appliance", myIdentity);
			}
			out.println(JSONValue.toJSONString(result));
		}
	}
}
