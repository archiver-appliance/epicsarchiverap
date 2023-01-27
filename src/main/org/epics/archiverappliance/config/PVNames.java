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
	 * When you intend to connect to the PV's using Channel Access, use this string as a prefix in the UI/archivePV BPL. For example, ca://double01
	 * This syntax should be consistent with CSS.
	 */
	public static final String V3_PREFIX = "ca://";
	
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
	 * A standard process for dealing with aliases, standard fields and the like and getting to the PVTypeInfo.
	 * @param pvName The name of PV.
	 * @param configService ConfigService
	 * @return PVTypeInfo  &emsp;
	 * 
	 * Places where we look for the typeinfo.
	 * <ul>
	 * <li> If the PV is not a field PV
	 * <ul>
	 * <li>Typeinfo for full PV name</li>
	 * <li>Alias for full PV name + Typeinfo for full PV name</li>
	 * </ul>
	 * </li><li>If the PV is a field PV
	 * <ul>
	 * <li>Typeinfo for fieldless PVName + archiveFields</li>
	 * <li>Typeinfo for full PV name</li>
	 * <li>Alias for fieldless PVName + Typeinfo for fieldless PVName + archiveFields</li>
	 * <li>Alias for full PV name + Typeinfo for full PV name</li>
	 * </ul>
	 * </ul>
	 * 
	 */
	public static PVTypeInfo determineAppropriatePVTypeInfo(String pvName, ConfigService configService) {
		boolean pvDoesNotHaveField = !PVNames.isField(pvName);
		
		if(pvDoesNotHaveField) {
			logger.debug("Looking for typeinfo for fieldless PV name " + pvName);
			// Typeinfo for full PV name
			{
				PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
				if(typeInfo != null) {
					logger.debug("Found typeinfo for pvName " + pvName);
					return typeInfo;
				}
			}
			
			// Alias for full PV name + Typeinfo for full PV name
			{
				String realName = configService.getRealNameForAlias(pvName);
				if(realName != null) { 
					PVTypeInfo typeInfo = configService.getTypeInfoForPV(realName);
					if(typeInfo != null) {
						logger.debug("Found typeinfo for real pvName " + realName + " which is an alias of " + pvName);
						return typeInfo;
					}
				}
			}
		} else { 
			logger.debug("Looking for typeinfo for PV name with a field " + pvName);
			String pvNameAlone = PVNames.stripFieldNameFromPVName(pvName);
			String fieldName = PVNames.getFieldName(pvName);
			 // Typeinfo for fieldless PVName + archiveFields
			{
				PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvNameAlone);
				if(typeInfo != null && typeInfo.checkIfFieldAlreadySepcified(fieldName)) {
					logger.debug("Found typeinfo for fieldless pvName " + pvNameAlone + " for archiveField " + fieldName);
					return typeInfo;
				}
			}
			
			// Typeinfo for full PV name
			{
				PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
				if(typeInfo != null) {
					logger.debug("Found typeinfo for full pvName with field " + pvName);
					return typeInfo;
				}
			}
			 
			// Alias for fieldless PVName + Typeinfo for fieldless PVName + archiveFields
			{
				String realName = configService.getRealNameForAlias(pvNameAlone);
				if(realName != null) { 
					PVTypeInfo typeInfo = configService.getTypeInfoForPV(PVNames.stripFieldNameFromPVName(realName));
					if(typeInfo != null && typeInfo.checkIfFieldAlreadySepcified(fieldName)) {
						logger.debug("Found typeinfo for aliased fieldless pvName " + realName + " for archiveField " + fieldName);
						return typeInfo;
					}
				}
			}
			
			// Alias for full PV name + Typeinfo for full PV name
			{
				String realName = configService.getRealNameForAlias(pvName);
				if(realName != null) { 
					PVTypeInfo typeInfo = configService.getTypeInfoForPV(realName);
					if(typeInfo != null) {
						logger.debug("Found typeinfo for real pvName " + realName + " which is an alias of " + pvName);
						return typeInfo;
					}
				}				
			}
		} // Ends Looking for typeinfo for PV name with a field " + pvName
		
		logger.debug("Did not find typeinfo for pvName " + pvName);
		return null;
	}

	/**
	 * A standard process for dealing with aliases, standard fields and the like and checking to see if the PV is in the archive workflow.
	 * @param pvName
	 * @param configService
	 * @return True if the PV or its avatars are in the archive workflow.
	 * It is not possible to state this accurately for all fields.
	 * For example, for fields that are archived as part of the stream, if the main PV is in the archive workflow, then the field is as well.
	 * But it is impossible to state this before the PVTypeInfo has been computed.
	 * So we resort to being pessimistic.
	 * 
	 * Places where we look for the typeinfo.
	 * <ul>
	 * <li> If the PV is not a field PV
	 * <ul>
	 * <li>ArchivePVRequests for full PV name</li>
	 * <li>Alias for full PV name + ArchivePVRequests for full PV name</li>
	 * </ul>
	 * </li><li>If the PV is a field PV
	 * <ul>
	 * <li>ArchivePVRequests for full PVName</li>
	 * <li>Alias for full PV name + ArchivePVRequests for full PVName</li>
	 * </ul>
	 * Note that this translates to the fact that regardless of whether the PV is a field or not, we look in the same places.
	 * </ul>
	 */
	public static boolean determineIfPVInWorkflow(String pvName, ConfigService configService) {
		logger.debug("Looking for archiverequests for PV " + pvName);
		
		// ArchivePVRequests for full PV name
		{
			if(configService.doesPVHaveArchiveRequestInWorkflow(pvName)) {
				logger.debug("Found PV in archive request workflow " + pvName);
				return true;
			}
		}
		
		// Alias for full PV name + ArchivePVRequests for full PV name
		{
			String realName = configService.getRealNameForAlias(pvName);
			if(realName != null) { 
				if(configService.doesPVHaveArchiveRequestInWorkflow(realName)) {
					logger.debug("Found aliased PV in archive request workflow " + realName);
					return true;
				}
			}
		}
		
		return false;
	}

	
	
	
	private static Pattern validPVName = Pattern.compile("[a-zA-Z0-9\\_\\-\\+\\:\\[\\]\\<\\>\\;\\.\\/\\,\\#\\{\\}\\^]+");
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
	 * And we add the '^' character because some folks at LNL use this.
	 * @param pvName The name of PV.
	 * @return boolean True or False
	 */
	public static boolean isValidPVName(String pvName) {
		if(pvName == null || pvName.isEmpty()) return false;
		return validPVName.matcher(pvName).matches();
	}


	public enum EPICSVersion {
		V3,
		V4,
		DEFAULT
	}

	/**
	 * What type of name is the pv.
	 * return EPICSVersion.V3 if starts with ca://
	 * return EPICSVersion.V4 if starts with pva://
	 * returns EPICSVersion.DEFAULT otherwise.
	 *
	 * @param pvName  The name of PV.
	 * @return EPICSVersion
	 */
	public static EPICSVersion pvNameVersion(String pvName) {
		if(pvName == null || pvName.isEmpty()) return EPICSVersion.DEFAULT;
		if (pvName.startsWith(V4_PREFIX))
			return EPICSVersion.V4;
		if (pvName.startsWith(V3_PREFIX))
			return EPICSVersion.V3;
		return EPICSVersion.DEFAULT;
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
		if(pvName.startsWith(V3_PREFIX)) {
			return pvName.replace(V3_PREFIX, "");
		}
		
		return pvName;
	}


}
