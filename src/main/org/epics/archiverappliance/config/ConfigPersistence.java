package org.epics.archiverappliance.config;

import java.io.IOException;
import java.util.List;


/**
 * Interface for persisting configuration
 * These are the pieces of configuration - all of these are key/value; keys are strings, values are typically JSON strings or plain strings.
 * <ol>
 * <li>Map<String, PVTypeInfo> typeInfos</li>
 * <li>Map<String, UserSpecifiedSamplingParams> archivePVRequests</li>
 * <li>Map<String, String> externalDataServer</li>
 * <li>Map<String, String> aliasNamesToRealNames</li>
 * </ol>
 * 
 * The APIs typically have one method to get all the keys, one to get a value given a key and a third to change a value given a key.
 * Others may be added later to improve performance and such.
 * @author mshankar
 *
 */
public interface ConfigPersistence {
	public List<String> getTypeInfoKeys() throws IOException;
	public PVTypeInfo getTypeInfo(String pvName) throws IOException;
	public void putTypeInfo(String pvName, PVTypeInfo typeInfo) throws IOException;
	public void deleteTypeInfo(String pvName) throws IOException;
	
	
	public List<String> getArchivePVRequestsKeys() throws IOException;
	public UserSpecifiedSamplingParams getArchivePVRequest(String pvName) throws IOException;
	public void putArchivePVRequest(String pvName, UserSpecifiedSamplingParams userParams) throws IOException;
	public void removeArchivePVRequest(String pvName) throws IOException;
	
	public List<String> getExternalDataServersKeys() throws IOException;
	public String getExternalDataServer(String serverId) throws IOException;
	public void putExternalDataServer(String serverId, String serverInfo) throws IOException;
	

	public List<String> getAliasNamesToRealNamesKeys() throws IOException;
	public String getAliasNamesToRealName(String pvName) throws IOException;
	public void putAliasNamesToRealName(String pvName, String realName) throws IOException;	
	public void removeAliasName(String pvName, String realName) throws IOException;	
}
