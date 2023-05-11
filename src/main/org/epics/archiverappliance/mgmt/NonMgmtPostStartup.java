package org.epics.archiverappliance.mgmt;

import java.util.concurrent.ScheduledFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;

/**
 * This pings the mgmt web app to let it know that we have started up.
 * @author mshankar
 *
 */
public class NonMgmtPostStartup implements Runnable {
	private static Logger logger = LogManager.getLogger(NonMgmtPostStartup.class.getName());
	private ScheduledFuture<?> cancellingFuture;
	private ConfigService configService;
	private String warFile;
	
	public NonMgmtPostStartup(ConfigService configService, String warFile) {
		this.configService = configService;
		this.warFile = warFile;
		logger.debug("NonMgmtPostStartup for " + warFile);
	}
	
	@Override
	public void run() {
		ApplianceInfo myApplianceInfo = configService.getMyApplianceInfo();
		logger.info("Starting NonMgmtPostStartup for " + this.warFile + " on appliance " + myApplianceInfo.getIdentity());
		if(configService.isStartupComplete()) {
			cancellingFuture.cancel(false);
			logger.info("Startup complete for webappp " + this.warFile);
		} else {
			String mgmtPingURL = myApplianceInfo.getMgmtURL() + "/webAppReady?webapp=" + warFile;
			logger.info("Pinging the management webapp using " + mgmtPingURL);
			GetUrlContent.checkURL(mgmtPingURL);
		}
	}
	
	public void setCancellingFuture(ScheduledFuture<?> cancellingFuture) {
		this.cancellingFuture = cancellingFuture;
	}
}
