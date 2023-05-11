package org.epics.archiverappliance.mgmt;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigService.WAR_FILE;
import org.epics.archiverappliance.utils.ui.GetUrlContent;

/**
 * The other web apps tell the mgmt webapp when they have started using this call.
 * If the mgmt web app has started, we simply call poststartup on them
 * @author mshankar
 *
 */
public class WebappReady implements BPLAction {
	private static Logger configlogger = LogManager.getLogger("config." + WebappReady.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String webApp = req.getParameter("webapp");
		if(webApp == null || webApp.equals("")) { 
			resp.sendError(HttpServletResponse.SC_NOT_FOUND); 
			configlogger.error("Received a webAppReady without a webapp parameter to identify the webapp.");
			return;
		}
		configlogger.info("Received webAppReady from " + webApp);
		WAR_FILE warFile = WAR_FILE.valueOf(webApp);
		if(warFile == null) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND); 
			configlogger.error("Received a webAppReady with an invalid webapp parameter to identify the webapp - " + webApp);
			return;
		}
		if(configService.isStartupComplete()) {
			ApplianceInfo myApplianceInfo = configService.getMyApplianceInfo();
			switch(warFile) {
			case RETRIEVAL: {
				String url = myApplianceInfo.getRetrievalURL() + "/postStartup";
				GetUrlContent.checkURL(url);
				configService.getMgmtRuntimeState().componentStartedUp(WAR_FILE.RETRIEVAL);
				break;
			}
			case ETL: {
				String url = myApplianceInfo.getEtlURL() + "/postStartup";
				GetUrlContent.checkURL(url);
				configService.getMgmtRuntimeState().componentStartedUp(WAR_FILE.ETL);
				break;
			}
			case ENGINE: {
				String url = myApplianceInfo.getEngineURL() + "/postStartup";
				GetUrlContent.checkURL(url);				
				configService.getMgmtRuntimeState().componentStartedUp(WAR_FILE.ENGINE);
				break;
			}
			case MGMT: {
				configlogger.error("The MGMT webapp should not send itself a startupcomplete call. Did you wire your appliances incorrectly?");
				break;
			}
			default: {
				configlogger.error("Received a webappready for war file that we did not expect. " + warFile);
				resp.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
			}
		} else {
			configlogger.info("Mgmt webapp is not ready yet....");
			resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE); 
			return;
		}
	}
}
