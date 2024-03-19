package org.epics.archiverappliance.etl.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;

public class InstanceReportDetails implements BPLAction {

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		try (PrintWriter out = resp.getWriter()) {
			out.println(ApplianceMetricsDetails.getETLMetricsDetails(configService));
		}
	}
}
