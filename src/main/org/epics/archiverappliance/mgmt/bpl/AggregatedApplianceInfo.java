package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * Gets the optimized aggregate typeInfo information for this appliance.
 * @author mshankar
 *
 */
public class AggregatedApplianceInfo implements BPLAction {
	private static Logger logger = LogManager.getLogger(AggregatedApplianceInfo.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.debug("Getting the aggregated appliance information for the appliance" + configService.getMyApplianceInfo().getIdentity());
		
		ApplianceAggregateInfo aggregateInfo = configService.getAggregatedApplianceInfo(configService.getMyApplianceInfo());
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			JSONEncoder<ApplianceAggregateInfo> jsonEncoder = JSONEncoder.getEncoder(ApplianceAggregateInfo.class);
			out.println(jsonEncoder.encode(aggregateInfo));
		} catch(Exception ex) {
			logger.error("ExceptionGetting the aggregated appliance information for the appliance" + configService.getMyApplianceInfo().getIdentity(), ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
}
