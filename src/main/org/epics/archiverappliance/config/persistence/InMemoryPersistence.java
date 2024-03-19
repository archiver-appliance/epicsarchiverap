package org.epics.archiverappliance.config.persistence;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.epics.archiverappliance.config.ConfigPersistence;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;

/**
 * Dummy in memory persistence layer for unit tests
 * @author mshankar
 *
 */
public class InMemoryPersistence implements ConfigPersistence {
	private ConcurrentHashMap<String, PVTypeInfo> typeInfos = new ConcurrentHashMap<String, PVTypeInfo>();
	private ConcurrentHashMap<String, UserSpecifiedSamplingParams> archivePVRequests = new ConcurrentHashMap<String, UserSpecifiedSamplingParams>();
	private ConcurrentHashMap<String, String> externalDataServersKeys = new ConcurrentHashMap<String, String>();
	private ConcurrentHashMap<String, String> aliasNamesToRealNames = new ConcurrentHashMap<String, String>();
	
	
	@Override
	public List<String> getTypeInfoKeys() throws IOException {
		return new LinkedList<String>(typeInfos.keySet());
	}
	
	@Override
	public List<PVTypeInfo> getAllTypeInfosForAppliance(String applianceIdentity) throws IOException {
		return new LinkedList<PVTypeInfo>(typeInfos.values());
	}

	@Override
	public PVTypeInfo getTypeInfo(String pvName) throws IOException {
		return typeInfos.get(pvName);
	}

	@Override
	public void putTypeInfo(String pvName, PVTypeInfo typeInfo) throws IOException {
		typeInfos.put(pvName, typeInfo);
	}
	
	@Override
	public void deleteTypeInfo(String pvName) throws IOException {
		typeInfos.remove(pvName);
	}


	@Override
	public List<String> getArchivePVRequestsKeys() throws IOException {
		return new LinkedList<String>(archivePVRequests.keySet());
	}

	@Override
	public UserSpecifiedSamplingParams getArchivePVRequest(String pvName) throws IOException {
		return archivePVRequests.get(pvName);
	}

	@Override
	public void putArchivePVRequest(String pvName, UserSpecifiedSamplingParams userParams) throws IOException {
		archivePVRequests.put(pvName, userParams);
	}
	
	@Override
	public void removeArchivePVRequest(String pvName) throws IOException {
		archivePVRequests.remove(pvName);
	}

	@Override
	public List<String> getExternalDataServersKeys() throws IOException {
		return new LinkedList<String>(externalDataServersKeys.keySet());
	}

	@Override
	public String getExternalDataServer(String serverId) throws IOException {
		return externalDataServersKeys.get(serverId);
	}

	@Override
	public void putExternalDataServer(String serverId, String serverInfo) throws IOException {
		externalDataServersKeys.put(serverId, serverInfo);
	}
	
	@Override
	public void removeExternalDataServer(String serverId, String serverInfo) throws IOException {
		externalDataServersKeys.remove(serverId);
	}


	@Override
	public List<String> getAliasNamesToRealNamesKeys() throws IOException {
		return new LinkedList<String>(aliasNamesToRealNames.keySet());
	}

	@Override
	public String getAliasNamesToRealName(String pvName) throws IOException {
		return aliasNamesToRealNames.get(pvName);
	}

	@Override
	public void putAliasNamesToRealName(String pvName, String realName) throws IOException {
		aliasNamesToRealNames.put(pvName, realName);
	}
	
	@Override
	public void removeAliasName(String pvName, String realName) throws IOException { 
		aliasNamesToRealNames.remove(pvName);
	}
}
