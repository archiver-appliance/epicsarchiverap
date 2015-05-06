package org.epics.archiverappliance.mgmt;

import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigService.WAR_FILE;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.utils.ui.GetUrlContent;

/**
 * Make sure all the other web apps have started and if so, send them their post startup messages...
 * @author mshankar
 *
 */
public class MgmtPostStartup implements Runnable {
	private static Logger logger = Logger.getLogger(MgmtPostStartup.class.getName());
	private static Logger configlogger = Logger.getLogger("config." + MgmtPostStartup.class.getName());
	private ScheduledFuture<?> cancellingFuture;
	private ConfigService configService;
	
	public MgmtPostStartup(ConfigService configService) {
		this.configService = configService;
	}
	
	@Override
	public void run() {
		logger.info("About to run MgmtPostStartup");
		if(this.configService.isStartupComplete()) {
			logger.info("Startup is complete for MgmtPostStartup");
			if(this.configService.getMgmtRuntimeState().haveChildComponentsStartedUp()) { 
				cancellingFuture.cancel(false);
			} else { 
				this.checkIfAllComponentsHaveStartedUp();
				if(this.configService.getMgmtRuntimeState().haveChildComponentsStartedUp()) { 
					cancellingFuture.cancel(false);
				}
			}
		} else {
			try { 
				logger.debug("Before post startup in MgmtPostStartup");
				configService.postStartup();
				configlogger.info("Finished post startup for the mgmt webapp");
			} catch(ConfigException ex) {
				logger.error("Exception running post startup on the management app", ex);
			}
		}
	}
	
	public void setCancellingFuture(ScheduledFuture<?> cancellingFuture) {
		this.cancellingFuture = cancellingFuture;
	}
	
	private void checkIfAllComponentsHaveStartedUp() { 
		// Check to see the other apps to see if the mgmt webapp is starting up after a JVM crash.
		// In normal circumstances, the other apps should start up and go thru the webappReady/postStartup exchange
		// However, in case the mgmt webapp crashes and is restarted by jsvc, we need to mimic the startup sequence...
		try { 
			ApplianceInfo myApplianceInfo = configService.getMyApplianceInfo();
			{
				logger.debug("Asking for startup status from the retrieval web app");
				String url = myApplianceInfo.getRetrievalURL() + "/startupState";
				@SuppressWarnings("unchecked")
				HashMap<String, String> retrievalStatus = (HashMap<String, String>)GetUrlContent.getURLContentAsJSONObject(url);
				ConfigService.STARTUP_SEQUENCE retrievalStartupState = ConfigService.STARTUP_SEQUENCE.valueOf(retrievalStatus.get("status"));
				if(retrievalStartupState == ConfigService.STARTUP_SEQUENCE.STARTUP_COMPLETE) { 
					configService.getMgmtRuntimeState().componentStartedUp(WAR_FILE.RETRIEVAL);
				}
			}
			
			{
				logger.debug("Asking for startup status from the ETL web app");
				String url = myApplianceInfo.getEtlURL() + "/startupState";
				@SuppressWarnings("unchecked")
				HashMap<String, String> etlStatus = (HashMap<String, String>)GetUrlContent.getURLContentAsJSONObject(url);
				ConfigService.STARTUP_SEQUENCE etlStartupState = ConfigService.STARTUP_SEQUENCE.valueOf(etlStatus.get("status"));
				if(etlStartupState == ConfigService.STARTUP_SEQUENCE.STARTUP_COMPLETE) { 
					configService.getMgmtRuntimeState().componentStartedUp(WAR_FILE.ETL);
				}
			}
			
			{
				logger.debug("Asking for startup status from the engine web app");
				String url = myApplianceInfo.getEngineURL() + "/startupState";
				@SuppressWarnings("unchecked")
				HashMap<String, String> engineStatus = (HashMap<String, String>)GetUrlContent.getURLContentAsJSONObject(url);
				ConfigService.STARTUP_SEQUENCE engineStartupState = ConfigService.STARTUP_SEQUENCE.valueOf(engineStatus.get("status"));
				if(engineStartupState == ConfigService.STARTUP_SEQUENCE.STARTUP_COMPLETE) { 
					configService.getMgmtRuntimeState().componentStartedUp(WAR_FILE.ENGINE);
				}
			}
			
		} catch(Exception ex) { 
			logger.warn("Exception checking startup state", ex);
		}
	}
}
