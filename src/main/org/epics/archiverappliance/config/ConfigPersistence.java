package org.epics.archiverappliance.config;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


/**
 * Interface for persisting configuration
 * These are the pieces of configuration - all of these are key/value; keys are strings, values are typically JSON strings or plain strings.
 * <ol>
 * <li>Map&lt;String, PVTypeInfo&gt; typeInfos</li>
 * <li>Map&lt;String, UserSpecifiedSamplingParams&gt; archivePVRequests</li>
 * <li>Map&lt;String, String&gt; externalDataServer</li>
 * <li>Map&lt;String, String&gt; aliasNamesToRealNames</li>
 * </ol>
 * 
 * The APIs typically have one method to get all the keys, one to get a value given a key and a third to change a value given a key.
 * Others may be added later to improve performance and such.
 * @author mshankar
 *
 */
public interface ConfigPersistence {
	
	default void initialize(ConfigService configService) {
		
	}
	
	public List<String> getTypeInfoKeys() throws IOException;
	public PVTypeInfo getTypeInfo(String pvName) throws IOException;
	public List<PVTypeInfo> getAllTypeInfosForAppliance(String applianceIdentity) throws IOException;
	public void putTypeInfo(String pvName, PVTypeInfo typeInfo) throws IOException;
	public void deleteTypeInfo(String pvName) throws IOException;
	
	
	public List<String> getArchivePVRequestsKeys() throws IOException;
	public UserSpecifiedSamplingParams getArchivePVRequest(String pvName) throws IOException;
	public void putArchivePVRequest(String pvName, UserSpecifiedSamplingParams userParams) throws IOException;
	public void removeArchivePVRequest(String pvName) throws IOException;
	
	public List<String> getExternalDataServersKeys() throws IOException;
	public String getExternalDataServer(String serverId) throws IOException;
	public void putExternalDataServer(String serverId, String serverInfo) throws IOException;
	public void removeExternalDataServer(String serverId, String serverInfo) throws IOException;
	

	public List<String> getAliasNamesToRealNamesKeys() throws IOException;
	public String getAliasNamesToRealName(String pvName) throws IOException;
	default public List<String> getAliasNamesForRealName(String realName) throws IOException {
		LinkedList<String> ret = new LinkedList<String>();
		for(String aliasName : getAliasNamesToRealNamesKeys()) {
			if(getAliasNamesToRealName(aliasName).equals(realName)) {
				ret.add(aliasName);
			}
		}
		return ret;
	}
	public void putAliasNamesToRealName(String pvName, String realName) throws IOException;	
	public void removeAliasName(String pvName, String realName) throws IOException;	
}
