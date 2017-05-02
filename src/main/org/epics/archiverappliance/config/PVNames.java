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
	 * When you intend to connect to the PV's using PVAccess, use this string as a prefix in the UI/archivePV BPL. For example, pva://double01
	 * This syntax should be consistent with CSS. 
	 */
	public static final String V4_PREFIX = "pva://";
	
	/**
	 * Remove the .VAL, .HIHI etc portion of a pvName and return the plain pvName
	 * @param pvName The name of PV.
	 * @return String The plain pvName
	 */
	public static String stripFieldNameFromPVName(String pvName) {
		if(pvName == null || pvName.equals("")) { 
			return pvName;
		}

		return pvName.split("\\.")[0];
	}
	
	
	/**
	 * Remove the .VAL, .HIHI etc portion of an array of pvNames and return an array of plain pvNames
	 * @param pvNames The name of PVs.
	 * @return String An array of plain pvNames
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
	 * @param pvName The name of PVs.
	 * @return String  normalizePVName
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
	 * @param pvName The name of PV.
	 * @param fieldName &emsp; 
	 * @return String normalizePVNameWithField
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
	 * @param pvName  The name of PV.
	 * @return boolean True or False
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
	 * Transfer any fields from the source name to the dest name
	 * Transferring ABC:123 onto DEF:456 should give DEF:456
	 * Transferring ABC:123.DESC onto DEF:456 should give DEF:456.DESC
	 * @param srcName The source name
	 * @param destName The destination name
	 * @return String transferField
	 */
	public static String transferField(String srcName, String destName) { 
		if(isField(srcName)) { 
			return normalizePVNameWithField(destName, getFieldName(srcName));
		} else { 
			return destName;
		}
	}
	
	/**
	 * A standard process for dealing with aliases, standard fields and the like.
	 * @param pvName The name of PV.
	 * @param configService ConfigService
	 * @return PVTypeInfo  &emsp;
	 */
	public static PVTypeInfo determineAppropriatePVTypeInfo(String pvName, ConfigService configService) {
		// First check for the pvName as is
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo != null) {
			logger.debug("Found typeinfo for pvName " + pvName);
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
				logger.debug("Found typeinfo for " + pvName + " as alias " + realName);
				return typeInfo;
			}
		}

		// Check for fields archived as part of PV.
		if(fieldName != null && !fieldName.equals("")) {
			typeInfo = configService.getTypeInfoForPV(pvNameAlone);
			if(typeInfo != null && typeInfo.checkIfFieldAlreadySepcified(fieldName)) {
				logger.debug("Found typeinfo for " + pvName + " for field " + fieldName);
				return typeInfo;
			}
		}
		
		logger.debug("Did not find typeinfo for pvName " + pvName);
		return null;
	}
	
	
	/**
	 * A standard process for dealing with aliases, standard fields and the like; should be similar to determineAppropriatePVTypeInfo
	 * @param pvName The name of PV.
	 * @param configService ConfigService
	 * @return ApplianceInfo  &emsp;
	 */
	public static ApplianceInfo determineAppropriateApplianceInfo(String pvName, ConfigService configService) {
		// First check for the pvName as is
		ApplianceInfo info = configService.getApplianceForPV(pvName);
		if(info != null) {
			logger.debug("Found appliance info for pvName " + pvName);
			return info;
		}
		String pvNameAlone = PVNames.stripFieldNameFromPVName(pvName);
		String fieldName= PVNames.getFieldName(pvName);
		// Check for aliases.
		String realName = configService.getRealNameForAlias(pvNameAlone);
		if(realName != null) {
			pvName = realName + (fieldName == null || fieldName.equals("") ? "" : ("." + fieldName));
			pvNameAlone = realName;

			info = configService.getApplianceForPV(pvName);
			if(info != null) {
				logger.debug("Found appliance info for " + pvName + " as alias " + realName);
				return info;
			}
		}

		// Check for fields archived as part of PV.
		if(fieldName != null && !fieldName.equals("")) {
			info = configService.getApplianceForPV(pvNameAlone);
			if(info != null) {
				logger.debug("Found appliance info for " + pvName + " for field " + fieldName);
				return info;
			}
		}
		
		logger.debug("Did not find appliance info for pvName " + pvName);
		return null;
	}

	
	
	
	private static Pattern validPVName = Pattern.compile("[a-zA-Z0-9\\_\\-\\+\\:\\[\\]\\<\\>\\;\\.\\/\\,\\#\\{\\}]+");
	/**
	 * Check to see if the pvName has valid characters.
	 * For certain characters, EPICS will not throw exceptions but generate spurious traffic which is hard to detect.
	 * From the App dev Guide at http://www.aps.anl.gov/epics/base/R3-14/12-docs/AppDevGuide/node7.html#SECTION007140000000000000000
	 * Valid characters are a-z A-Z 0-9 _ - + : [ ] &lt; &gt; ;
	 * We add the '.' character for suporting fieldnames as well.
	 * And we add the '/' character because some folks at FACET use this.
	 * And we add the ',' character because some folks at LBL use this.
	 * And we add the '#' character because some folks at FRIB use this.
	 * And we add the '{' and the '}' character because some folks at BNL use this.
	 * @param pvName The name of PV.
	 * @return boolean True or False
	 */
	public static boolean isValidPVName(String pvName) {
		if(pvName == null || pvName.isEmpty()) return false;
		return validPVName.matcher(pvName).matches();
	}


	/**
	 * Does this pvName imply a connection using PVAccess?
	 * @param pvName  The name of PV.
	 * @return boolean True or False
	 */
	public static boolean isEPICSV4PVName(String pvName) { 
		if(pvName == null || pvName.isEmpty()) return false;
		return pvName.startsWith(V4_PREFIX);
	}
	
	
	/**
	 * Remove the pva:// prefix from the PV name if present.
	 * @param pvName The name of PV.
	 * @return String  &emsp;
	 */
	public static String stripPrefixFromName(String pvName) { 
		if(pvName == null || pvName.isEmpty()) return pvName;
		if(pvName.startsWith(V4_PREFIX)) { 
			return pvName.replace(V4_PREFIX, "");
		}
		
		return pvName;
	}


}
