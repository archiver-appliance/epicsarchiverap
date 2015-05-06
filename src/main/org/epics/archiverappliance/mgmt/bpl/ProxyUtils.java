package org.epics.archiverappliance.mgmt.bpl;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;

/**
 * Small utility class to proxy mgmt BPL to appliance other than this appliance. 
 * @author mshankar
 *
 */
public class ProxyUtils {
	private static Logger logger = Logger.getLogger(ProxyUtils.class.getName());

	/**
	 * Route pathAndQuery to all appliances other than this appliance
	 * @param configService
	 * @param pathAndQuery
	 */
	public static void routeURLToOtherAppliances(ConfigService configService, String pathAndQuery) { 
		ArrayList<String> otherURLs = new ArrayList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) { 
			if(!info.equals(configService.getMyApplianceInfo())) { 
				otherURLs.add(info.getMgmtURL() + pathAndQuery);
			}
		}
		
		for(String otherURL : otherURLs) { 
			try { 
				GetUrlContent.getURLContentAsJSONObject(otherURL);
			} catch(Throwable t) { 
				logger.error("Exception getting content of URL " + otherURL, t);
			}
		}
	}
}
