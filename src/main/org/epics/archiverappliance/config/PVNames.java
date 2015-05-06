package org.epics.archiverappliance.config;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * Utility class for dealing with various aspects of EPICS PV names
 * @author mshankar
 *
 */
public class PVNames {
	private static Logger logger = Logger.getLogger(PVNames.class.getName());
	/**
	 * Remove the .VAL, .HIHI etc portion of a pvName and return the plain pvName
	 * @param pvName
	 * @return
	 */
	public static String stripFieldNameFromPVName(String pvName) {
		if(pvName == null || pvName.equals("")) { 
			return pvName;
		}

		return pvName.split("\\.")[0];
	}
	
	
	/**
	 * Remove the .VAL, .HIHI etc portion of an array of pvNames and return an array of plain pvNames
	 * @param pvNames
	 * @return
	 */
	public static String[] stripFieldNameFromPVNames(String[] pvNames) {
		if(pvNames == null || pvNames.length == 0) return pvNames;
		String[] ret = new String[pvNames.length];
		for(int i = 0; i < pvNames.length; i++) {
			ret[i] = stripFieldNameFromPVName(pvNames[i]);
		}
		return ret;
	}
	
	public static String getFieldName(String pvName) {
		if(pvName == null || pvName.equals("") || !pvName.contains(".")) { 
			return "";
		}

		String[] parts = pvName.split("\\.");
		if(parts.length != 2) { 
			logger.error("Invalid PV name " + pvName);
			return "";
		}
		return parts[1];
	}
	
	/**
	 * Remove .VAL from pv names if present.
	 * Returned value is something that can be used to lookup for PVTypeInfo
	 * @param pvName
	 * @return
	 */
	public static String normalizePVName(String pvName) {
		if(pvName == null || pvName.equals("")) { 
			return pvName;
		}

		String[] parts = pvName.split("\\.");
		if(parts.length == 1) { 
			return pvName;
		} else if (parts.length == 2) {
			String fieldName = parts[1];
			if(fieldName != null && !fieldName.equals("") && fieldName.equals("VAL")) {
				return parts[0];
			} else {
				return pvName;
			}
		} else {
			logger.error("Invalid PV name " + pvName);
			return "";
		}
	}

	
	/**
	 * Gives you something you can use with caget to get the field associated with a PV even if you have a field already.
	 * normalizePVNameWithField("ABC", "NAME") gives "ABC.NAME"
	 * normalizePVNameWithField("ABC.HIHI", "NAME") gives "ABC.NAME"
	 * @param pvName
	 * @param fieldName
	 * @return
	 */
	public static String normalizePVNameWithField(String pvName, String fieldName) {
		if(pvName == null || pvName.equals("")) { 
			return pvName;
		}

		String[] parts = pvName.split("\\.");
		if(parts.length == 1) { 
			return pvName + "." + fieldName;
		} else if (parts.length == 2) {
				return parts[0] + "." + fieldName;
		} else {
			logger.error("Invalid PV name " + pvName);
			return "";
		}
	}

	
	/**
	 * Is this a field?
	 * @param pvName
	 * @return
	 */
	public static boolean isField(String pvName) {
		if(pvName == null || pvName.equals("")) { 
			return false;
		}
		
		String[] parts = pvName.split("\\.");
		if(parts.length == 1) { 
			return false;
		} else if (parts.length == 2) {
			String fieldName = parts[1];
			if(fieldName == null || fieldName.equals("")) {
				return false;
			} else {
				if(fieldName.equals("VAL")) {
					return false;
				} else {
					return true;
				}
			}
		} else {
			logger.error("Invalid PV name " + pvName);
			return false;
		}
	}
	
	
	/**
	 * A standard process for dealing with aliases, standard fields and the like.
	 * @param pvName
	 * @param configService
	 * @return
	 */
	public static PVTypeInfo determineAppropriatePVTypeInfo(String pvName, ConfigService configService) {
		// First check for the pvName as is
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo != null) {
			return typeInfo;
		}
		String pvNameAlone = PVNames.stripFieldNameFromPVName(pvName);
		String fieldName= PVNames.getFieldName(pvName);
		// Check for aliases.
		String realName = configService.getRealNameForAlias(pvNameAlone);
		if(realName != null) {
			pvName = realName + (fieldName == null || fieldName.equals("") ? "" : ("." + fieldName));
			pvNameAlone = realName;

			typeInfo = configService.getTypeInfoForPV(pvName);
			if(typeInfo != null) {
				return typeInfo;
			}
		}

		// Check for fields archived as part of PV.
		if(fieldName != null && !fieldName.equals("")) {
			typeInfo = configService.getTypeInfoForPV(pvNameAlone);
			if(typeInfo != null && typeInfo.checkIfFieldAlreadySepcified(fieldName)) {
				return typeInfo;
			}
		}
		
		return null;
	}
	
	
	private static Pattern validPVName = Pattern.compile("[a-zA-Z0-9\\_\\-\\+\\:\\[\\]\\<\\>\\;\\.\\/\\,\\#\\{\\}]+");
	/**
	 * Check to see if the pvName has valid characters.
	 * For certain characters, EPICS will not throw exceptions but generate spurious traffic which is hard to detect.
	 * From the App dev Guide at http://www.aps.anl.gov/epics/base/R3-14/12-docs/AppDevGuide/node7.html#SECTION007140000000000000000
	 * Valid characters are a-z A-Z 0-9 _ - + : [ ] < > ;
	 * We add the '.' character for suporting fieldnames as well.
	 * And we add the '/' character because some folks at FACET use this.
	 * And we add the ',' character because some folks at LBL use this.
	 * And we add the '#' character because some folks at FRIB use this.
	 * And we add the '{' and the '}' character because some folks at BNL use this.
	 * @param pvName
	 * @return
	 */
	public static boolean isValidPVName(String pvName) {
		if(pvName == null || pvName.isEmpty()) return false;
		return validPVName.matcher(pvName).matches();
	}
	
}
