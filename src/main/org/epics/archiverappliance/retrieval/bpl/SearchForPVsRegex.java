package org.epics.archiverappliance.retrieval.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;

public class SearchForPVsRegex implements BPLAction {
	private static Logger logger = LogManager.getLogger(SearchForPVsRegex.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String nameToMatch = req.getParameter("regex");
		if(nameToMatch == null || nameToMatch.equals("")) {
			logger.error("This search needs to be called with a regex argument.");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		logger.debug("Regex for searching for pvnames is " + nameToMatch);
		
		resp.setContentType("text/plain");
		try(PrintWriter out = resp.getWriter()) {
			List<String> matchingPVNames = (List<String>) GetMatchingPVs.getMatchingPVsInCluster(configService, -1, nameToMatch, GetMatchingPVs.includeExternalServers(req));
			for(String pvName : matchingPVNames) {
				out.println(pvName);
			}
		}
	}

}
