package org.epics.archiverappliance.engine.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.epics.EngineMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

public class InstanceReportDetails implements BPLAction {

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {

		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			out.println(EngineMetrics.computeEngineMetrics(configService.getEngineContext(), configService).getDetails(configService.getEngineContext()));
		}
	}
}
