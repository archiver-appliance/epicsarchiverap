package org.epics.archiverappliance.retrieval.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.common.ArchivedPVsInList;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.bpl.PVsMatchingParameter;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * 
 * Given a list of PV's in the POST, filter these to those that are being archived in this cluster.
 * @author mshankar
 *
 */
public class FilterArchivedPVs implements BPLAction {

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		LinkedList<String> pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req);
		List<String> archivedPVs = ArchivedPVsInList.getArchivedPVs(pvNames, configService);

		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            JSONValue.writeJSONString(archivedPVs, out);
        }	
	}
}
