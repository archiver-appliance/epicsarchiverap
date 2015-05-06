package org.epics.archiverappliance.retrieval.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;

public class SearchForPVsRegex implements BPLAction {
	private static Logger logger = Logger.getLogger(SearchForPVsRegex.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String regex = req.getParameter("regex");
		if(regex == null || regex.equals("")) {
			logger.error("This search needs to be called with a regex argument.");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		logger.debug("Regex for searching for pvnames is " + regex);
		
		Pattern pattern = Pattern.compile(regex);
		LinkedList<String> matchingPVNames = new LinkedList<String>();
		for(String pvName : configService.getAllPVs()) {
			Matcher matcher = pattern.matcher(pvName);
			if(matcher.matches()) {
				matchingPVNames.add(pvName);
			}
		}
		resp.setContentType("text/plain");
		try(PrintWriter out = resp.getWriter()) {
			for(String pvName : matchingPVNames) {
				out.println(pvName);
			}
		}
	}

}
