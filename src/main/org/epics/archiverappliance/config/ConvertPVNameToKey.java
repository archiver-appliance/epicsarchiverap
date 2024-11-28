package org.epics.archiverappliance.config;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.exception.ConfigException;

/**
 * Most labs use a standard character (typically the ":" or the "-" character) in their naming conventions to separate out the components of a name.
 * We want to replace these characters with the File.separator so that the key/file structure reflects the naming convention reasonably closely
 * This give us 2 things
 * <ol>
 * <li>All the files/keys for the entire archiver system do not land in the same folder. This improves performance significantly on ext2/3/4 like filesystems which can handle upto around 10000 files in a folder reasonably well but are very inefficient if this limit is crossed.
 * Note, systems like ZFS/BTRFS do not have this limitation.
 * </li>
 * <li>We have a deterministic way to go from PV name to key/file name</li>
 * </ol>
 * <p>
 * One other issue we need to solve is that some PV's (<code>X:CTRD_V</code>) are substrings of other PV's (<code>X:CTRD_V_BOOK</code>).
 * So, files for <code>X:CTRD_V</code> are matches for <code>X:CTRD_V_BOOK</code> as well.
 * We can either replace the underscores with /'s as well.
 * Or the character that separates the final name component of the PV name from the rest of the file name (time partition info, extension etc) should also be something that would not show up in a PV name at the end.
 * For now, we choose the latter and use another property to identify the separator string (for example ":") as the terminator.
 * For example, <code>X:CTRD_V_BOOK</code> is mapped to <code>X/CTRD_V_BOOK:</code>.   
 * </p>
 * <p>
 * This class uses two properties obtained from the <code>archappl.properties</code> file to map PV names to keys.
 * <ol>
 * <li>org.epics.archiverappliance.config.ConvertPVNameToKey.siteNameSpaceSeparators - This is a list of characters that separate the components of the PV name. The syntax for this must satisfy Java's {@link String#replaceAll(String, String) replaceAll} regex requirements. So to specify a list containing the ":" and the "-" characters, we have to use <code>[\\:\\-]</code></li>
 * <li>org.epics.archiverappliance.config.ConvertPVNameToKey.siteNameSpaceTerminator - This is the character used as the terminator of the PV name portion of the key.</li>
 * </ol>
 * <p>
 * @author mshankar
 */
public class ConvertPVNameToKey implements PVNameToKeyMapping {
	private static Logger configlogger = LogManager.getLogger("config." + ConvertPVNameToKey.class.getName());
	private static final String SITE_NAME_SPACE_SEPARATORS = "org.epics.archiverappliance.config.ConvertPVNameToKey.siteNameSpaceSeparators";
	private static final String SITE_NAME_SPACE_TERMINATOR = "org.epics.archiverappliance.config.ConvertPVNameToKey.siteNameSpaceTerminator";
	private String siteNameSpaceSeparators;
	private char terminatorChar = ':';
	private String fileSeparator;
	
	private ConfigService configService;
	private ConcurrentHashMap<String, String> chunkKeys = new ConcurrentHashMap<String, String>();
	
	
	/* (non-Javadoc)
	 * @see org.epics.archiverappliance.config.PVNameToKeyMapping#convertPVNameToKey(java.lang.String)
	 * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html">java.util.concurrent.ConcurrentHashMap</a>
	 */
	@Override
	public String convertPVNameToKey(String pvName) {
		// First check the local cache for the mapping.
		String chunkKey = chunkKeys.get(pvName);
		if(chunkKey != null) return chunkKey;
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) { 
			return generateChunkKey(pvName);
		}

		// Then check the typeinfo.
		chunkKey = typeInfo.getChunkKey();
		if(chunkKey != null) { 
			chunkKeys.put(pvName, chunkKey);
			return chunkKey;
		}

		// If it ain't set there either, generate it and set in all places.
		chunkKey =  generateChunkKey(pvName);
		typeInfo.setChunkKey(chunkKey);
		chunkKeys.put(pvName, chunkKey);
		return chunkKey;
	}


	@Override
	public void initialize(ConfigService configService) throws ConfigException {
		this.configService = configService;
		this.siteNameSpaceSeparators = configService.getInstallationProperties().getProperty(SITE_NAME_SPACE_SEPARATORS);
		if(this.siteNameSpaceSeparators == null || this.siteNameSpaceSeparators.equals("") || this.siteNameSpaceSeparators.length() < 1) {
			throw new ConfigException("The appliance archiver cannot function without knowning the characters that separate the components of a PV name ");
		}
		String terminatorStr = configService.getInstallationProperties().getProperty(SITE_NAME_SPACE_TERMINATOR);
		if(terminatorStr == null || terminatorStr.equals("") || terminatorStr.length() < 1) {
			throw new ConfigException("The appliance archiver cannot function without knowning the character that terminates the translated path name ");
		}
		this.terminatorChar = terminatorStr.charAt(0);
		this.fileSeparator = File.separator.equals("/") ? "/" : "\\\\";
		
		configlogger.info("The pv name components in this installation are separated by these characters " + this.siteNameSpaceSeparators + 
				" and the key names are terminated by " + this.terminatorChar);
	}
	
	
	/**
	 * One option is to simply override the final element of chunk key generation. 
	 * To do that, subclass and override this method. (Of course, you still have to register the subclass in archappl.properties).
	 * @param pvName The name of PV.
	 * @return pvName  &emsp;
	 */
	protected String generateChunkKey(String pvName) { 
		return pvName.replaceAll(siteNameSpaceSeparators, fileSeparator) + terminatorChar;
	}


	/* (non-Javadoc)
	 * @see org.epics.archiverappliance.config.PVNameToKeyMapping#containsSiteSeparators(java.lang.String)
	 */
	@Override
	public boolean containsSiteSeparators(String pvName) {
		return pvName.matches(siteNameSpaceSeparators);
	}


	@Override
	public String[] breakIntoParts(String pvName) {
		return pvName.split(siteNameSpaceSeparators);
	}
}
