package org.epics.archiverappliance.mgmt.bpl;

import java.io.File;
import java.util.LinkedList;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;

/**
 * Small utility class for listing PVs that match a parameter
 * @author mshankar
 *
 */
public class PVsMatchingParameter {
	public static LinkedList<String> getMatchingPVs(HttpServletRequest req, ConfigService configService) {
		return getMatchingPVs(req, configService, false);
	}
	/**
	 * Given a BPL request, get all the matching PVs
	 * @param req
	 * @param configService
	 * @param includePVSThatDontExist - Some BPL requires us to include PVs that don't exist so that they can give explicit status
	 * @return
	 */
	public static LinkedList<String> getMatchingPVs(HttpServletRequest req, ConfigService configService, boolean includePVSThatDontExist) {
		LinkedList<String> pvNames = new LinkedList<String>();
		if(req.getParameter("pv") != null) { 
			String[] pvs = req.getParameter("pv").split(",");
			for(String pv : pvs) { 
				if(pv.contains("*") || pv.contains("?")) {
					WildcardFileFilter matcher = new WildcardFileFilter(pv); 
					for(String pvName : configService.getAllPVs()) {
						if(matcher.accept((new File(pvName)))) {
							pvNames.add(pvName);
						}
					}
				} else {
					ApplianceInfo info = configService.getApplianceForPV(pv);
					if(info != null) { 
						pvNames.add(pv);
					} else { 
						if(includePVSThatDontExist) { 
							pvNames.add(pv);							
						}
					}
				}
			}
		} else { 
			if(req.getParameter("regex") != null) { 
				String regex = req.getParameter("regex");
				Pattern pattern = Pattern.compile(regex);
				for(String pvName : configService.getAllPVs()) {
					if(pattern.matcher(pvName).matches()) { 
						pvNames.add(pvName);
					}
				}
			} else { 
				for(String pvName : configService.getAllPVs()) {
					pvNames.add(pvName);
				}
			}
		}
		return pvNames;
	}
}
