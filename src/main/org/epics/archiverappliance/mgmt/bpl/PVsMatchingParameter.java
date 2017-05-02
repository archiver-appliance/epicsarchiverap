package org.epics.archiverappliance.mgmt.bpl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.LinkedList;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Small utility class for listing PVs that match a parameter
 * @author mshankar
 *
 */
public class PVsMatchingParameter {
	private static Logger logger = Logger.getLogger(PVsMatchingParameter.class.getName());
	public static LinkedList<String> getMatchingPVs(HttpServletRequest req, ConfigService configService, int defaultLimit) {
		return getMatchingPVs(req, configService, false, defaultLimit);
	}
	/**
	 * Given a BPL request, get all the matching PVs
	 * @param req HttpServletRequest 
	 * @param configService  ConfigService 
	 * @param includePVSThatDontExist Some BPL requires us to include PVs that don't exist so that they can give explicit status
	 * @param defaultLimit The default value for the limit if the limit is not specified in the request.
	 * @return LinkedList Matching PVs
	 */
	public static LinkedList<String> getMatchingPVs(HttpServletRequest req, ConfigService configService, boolean includePVSThatDontExist, int defaultLimit) {
		LinkedList<String> pvNames = new LinkedList<String>();
		int limit = defaultLimit;
		String limitParam = req.getParameter("limit");
		if(limitParam != null) { 
			limit = Integer.parseInt(limitParam);
		}
		
		if(req.getParameter("pv") != null) { 
			String[] pvs = req.getParameter("pv").split(",");
			for(String pv : pvs) { 
				if(pv.contains("*") || pv.contains("?")) {
					WildcardFileFilter matcher = new WildcardFileFilter(pv); 
					for(String pvName : configService.getAllPVs()) {
						if(matcher.accept((new File(pvName)))) {
							pvNames.add(pvName);
							if(limit != -1 && pvNames.size() >= limit) { 
								return pvNames;
							}
						}
					}
					for(String pvName : configService.getAllAliases()) {
						if(matcher.accept((new File(pvName)))) {
							pvNames.add(pvName);
							if(limit != -1 && pvNames.size() >= limit) { 
								return pvNames;
							}
						}
					}
				} else {
					ApplianceInfo info = configService.getApplianceForPV(pv);
					if(info != null) { 
						pvNames.add(pv);
						if(limit != -1 && pvNames.size() >= limit) { 
							return pvNames;
						}
					} else { 
						if(includePVSThatDontExist) { 
							pvNames.add(pv);							
							if(limit != -1 && pvNames.size() >= limit) { 
								return pvNames;
							}
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
						if(limit != -1 && pvNames.size() >= limit) { 
							return pvNames;
						}
					}
				}
				for(String pvName : configService.getAllAliases()) {
					if(pattern.matcher(pvName).matches()) { 
						pvNames.add(pvName);
						if(limit != -1 && pvNames.size() >= limit) { 
							return pvNames;
						}
					}
				}
			} else { 
				for(String pvName : configService.getAllPVs()) {
					pvNames.add(pvName);
					if(limit != -1 && pvNames.size() >= limit) { 
						return pvNames;
					}
				}
				for(String pvName : configService.getAllAliases()) {
					pvNames.add(pvName);
					if(limit != -1 && pvNames.size() >= limit) { 
						return pvNames;
					}
				}
			}
		}
		return pvNames;
	}
	
	
	public static LinkedList<String> getPVNamesFromPostBody(HttpServletRequest req, ConfigService configService) throws IOException {
		LinkedList<String> pvNames = new LinkedList<String>();
		String contentType = req.getContentType();
		if(contentType != null) { 
			switch(contentType) { 
			case MimeTypeConstants.APPLICATION_JSON: 
				try (LineNumberReader lineReader = new LineNumberReader(new InputStreamReader(new BufferedInputStream(req.getInputStream())))) {
					JSONParser parser=new JSONParser();
					for(Object pvName : (JSONArray) parser.parse(lineReader)) {
						pvNames.add((String) pvName);
					}
				} catch(ParseException ex) { 
					throw new IOException(ex);
				}
				return pvNames;
			case MimeTypeConstants.APPLICATION_FORM_URLENCODED: 
				String[] pvs = req.getParameter("pv").split(",");
				for(String pv : pvs) {
					pvNames.add(pv);
				}
				return pvNames;
			case MimeTypeConstants.TEXT_PLAIN:				
			default:
				// For the default we assume text/plain which is a list of PV's separated by unix newlines
				try (LineNumberReader lineReader = new LineNumberReader(new InputStreamReader(new BufferedInputStream(req.getInputStream())))) {
					String pv = lineReader.readLine();
					logger.debug("Parsed pv " + pv);
					while(pv != null) { 
						pvNames.add(pv);
						pv = lineReader.readLine();
					}
				}
				return pvNames;
			}
		}

		return pvNames;
	}
}
