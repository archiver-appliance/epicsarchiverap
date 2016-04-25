package org.epics.archiverappliance.config.persistence;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigPersistence;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Uses JDBM2 as a persistence layer; probably not for production installations as JDBM2 does not support concurrent access and so on.
 * To set the path to the JDBM2 file, use the environment variable ARCHAPPL_PERSISTENCE_LAYER_JDBM2FILENAME.
 * This defaults to <code>./archapplconfig.jdbm2</code>
 * To use this persistence layer, use
 * <pre>
 * export ARCHAPPL_PERSISTENCE_LAYER="org.epics.archiverappliance.config.persistence.JDBM2Persistence"
 * export ARCHAPPL_PERSISTENCE_LAYER_JDBM2FILENAME="/scratch/Archiver/persistence.jdbm2"
 * </pre>
 * @author mshankar
 *
 */
public class JDBM2Persistence implements ConfigPersistence {
	private static Logger logger = Logger.getLogger(JDBM2Persistence.class.getName());
	public static final String ARCHAPPL_JDBM2_FILENAME = ConfigService.ARCHAPPL_PERSISTENCE_LAYER + "_JDBM2FILENAME";
	private String pathToConfigData = "./archapplconfig.jdbm2";
	private ConcurrentHashMap<String, PVTypeInfo> cachedTypeInfos = new ConcurrentHashMap<String, PVTypeInfo>();
	
	public JDBM2Persistence() throws ConfigException { 
		String pathFromEnv = System.getProperty(ARCHAPPL_JDBM2_FILENAME);
		if(pathFromEnv == null) { 
			pathFromEnv = System.getenv(ARCHAPPL_JDBM2_FILENAME);
		}
		if(pathFromEnv != null) { 
			pathToConfigData = pathFromEnv;
		}
		logger.info("Loading JDBM2 data from " + pathToConfigData);
		try {
			preLoadTypeInfos();
		} catch(IOException ex) { 
			throw new ConfigException("Exception preloading pvTypeInfos", ex);
		}
		logger.info("Done caching " + cachedTypeInfos.size() + " pvTypeInfos from " + pathToConfigData);
	}
	
	@Override
	public List<String> getTypeInfoKeys() throws IOException {
		return getKeys("TypeInfo");
	}

	@Override
	public PVTypeInfo getTypeInfo(String pvName) throws IOException {
		if(cachedTypeInfos.containsKey(pvName)) return cachedTypeInfos.get(pvName);
		logger.debug("Getting typeinfo for pv " + pvName + " from db instead of cache");
		return getValueForKey("TypeInfo", pvName, new PVTypeInfo(), PVTypeInfo.class);
	}

	@Override
	public void putTypeInfo(String pvName, PVTypeInfo typeInfo) throws IOException {
		cachedTypeInfos.put(pvName, typeInfo);
		putValueForKey("TypeInfo", pvName, typeInfo, PVTypeInfo.class);
	}

	@Override
	public void deleteTypeInfo(String pvName) throws IOException {
		logger.debug("Removing typeinfo for pv " + pvName + " from db and cache");
		if(cachedTypeInfos.containsKey(pvName)) cachedTypeInfos.remove(pvName);
		removeKey("TypeInfo", pvName);
	}

	@Override
	public List<String> getArchivePVRequestsKeys() throws IOException {
		return getKeys("ArchivePVRequests");
	}

	@Override
	public UserSpecifiedSamplingParams getArchivePVRequest(String pvName) throws IOException {
		return getValueForKey("ArchivePVRequests", pvName, new UserSpecifiedSamplingParams(), UserSpecifiedSamplingParams.class);
	}

	@Override
	public void putArchivePVRequest(String pvName, UserSpecifiedSamplingParams userParams) throws IOException {
		putValueForKey("ArchivePVRequests", pvName, userParams, UserSpecifiedSamplingParams.class);
	}
	
	@Override
	public void removeArchivePVRequest(String pvName) throws IOException {
		removeKey("ArchivePVRequests", pvName);
	}

	@Override
	public List<String> getExternalDataServersKeys() throws IOException {
		return getKeys("ExternalDataServers");
	}

	@Override
	public String getExternalDataServer(String serverId) throws IOException {
		return getStringValueForKey("ExternalDataServers", serverId);
	}

	@Override
	public void putExternalDataServer(String serverId, String serverInfo) throws IOException {
		putStringValueForKey("ExternalDataServers", serverId, serverInfo);	
	}
	
	@Override
	public void removeExternalDataServer(String serverId, String serverInfo) throws IOException {
		removeKey("ExternalDataServers", serverId);
	}



	@Override
	public List<String> getAliasNamesToRealNamesKeys() throws IOException {
		return getKeys("AliasNamesToRealNames");
	}

	@Override
	public String getAliasNamesToRealName(String pvName) throws IOException {
		return getStringValueForKey("AliasNamesToRealNames", pvName);
	}

	@Override
	public void putAliasNamesToRealName(String pvName, String realName) throws IOException {
		putStringValueForKey("AliasNamesToRealNames", pvName, realName);
	}
	
	@Override
	public void removeAliasName(String pvName, String realName) throws IOException {
		removeKey("AliasNamesToRealNames", pvName);
	}
	
	
	private synchronized List<String> getKeys(String recordName) throws IOException {
		RecordManager recMan = null;
		try { 
			recMan = RecordManagerFactory.createRecordManager(pathToConfigData);
			PrimaryTreeMap<String,String> typeInfos = recMan.treeMap(recordName);
			List<String> typeInfoKeys = new LinkedList<String>(typeInfos.keySet());
			logger.debug(recordName + " returns " + typeInfoKeys.size() + " keys");
			return typeInfoKeys;
		} finally { 
			if(recMan != null) { try { recMan.close(); recMan = null; } catch(Exception ex) {} } 
		}
	}
	
	
	
	private synchronized <T> T getValueForKey(String recordName, String key, T obj, Class<T> clazz) throws IOException {
		RecordManager recMan = null;
		try { 
			recMan = RecordManagerFactory.createRecordManager(pathToConfigData);
			PrimaryTreeMap<String,String> map = recMan.treeMap(recordName);
			String jsonStr = map.get(key);
			if(jsonStr != null) { 
				JSONObject jsonObj = (JSONObject) JSONValue.parse(jsonStr);
				JSONDecoder<T> decoder = JSONDecoder.getDecoder(clazz);
				decoder.decode(jsonObj, obj);
				return obj;
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		} finally { 
			if(recMan != null) { try { recMan.close(); recMan = null; } catch(Exception ex) {} } 
		}

		return null;
	}
	
	private synchronized String getStringValueForKey(String recordName, String key) throws IOException {
		RecordManager recMan = null;
		try { 
			recMan = RecordManagerFactory.createRecordManager(pathToConfigData);
			PrimaryTreeMap<String,String> map = recMan.treeMap(recordName);
			return map.get(key);
		} catch(Exception ex) {
			throw new IOException(ex);
		} finally { 
			if(recMan != null) { try { recMan.close(); recMan = null; } catch(Exception ex) {} } 
		}
	}

	
	
	private synchronized <T> void putValueForKey(String recordName, String key, T obj, Class<T> clazz) throws IOException {
		if(key == null || key.equals("")) throw new IOException("key cannot be null when persisting " + recordName);
		if(obj == null || obj.equals("")) throw new IOException("value cannot be null when persisting " + recordName);

		RecordManager recMan = null;
		try { 
			recMan = RecordManagerFactory.createRecordManager(pathToConfigData);
			PrimaryTreeMap<String,String> map = recMan.treeMap(recordName);
			JSONEncoder<T> encoder = JSONEncoder.getEncoder(clazz);
			JSONObject jsonObj = encoder.encode(obj);
			String jsonStr = jsonObj.toJSONString();
			map.put(key, jsonStr);
		} catch(Exception ex) {
			throw new IOException(ex);
		} finally { 
			if(recMan != null) { try { recMan.close(); recMan = null; } catch(Exception ex) {} } 
		}
	}
	
	
	private synchronized void putStringValueForKey(String recordName, String key, String value) throws IOException {
		if(key == null || key.equals("")) throw new IOException("key cannot be null when persisting " + recordName);
		if(value == null || value.equals("")) throw new IOException("value cannot be null when persisting " + recordName);
		
		RecordManager recMan = null;
		try { 
			recMan = RecordManagerFactory.createRecordManager(pathToConfigData);
			PrimaryTreeMap<String,String> map = recMan.treeMap(recordName);
			map.put(key, value);
		} catch(Exception ex) {
			throw new IOException(ex);
		} finally { 
			if(recMan != null) { try { recMan.close(); recMan = null; } catch(Exception ex) {} } 
		}
	}
	
	
	private synchronized void removeKey(String recordName, String key) throws IOException {
		RecordManager recMan = null;
		try { 
			recMan = RecordManagerFactory.createRecordManager(pathToConfigData);
			PrimaryTreeMap<String,String> map = recMan.treeMap(recordName);
			map.remove(key);
		} catch(Exception ex) {
			throw new IOException(ex);
		} finally { 
			if(recMan != null) { try { recMan.close(); recMan = null; } catch(Exception ex) {} } 
		}
	}
	
	/**
	 * Optimization to make the performance test start faster.
	 * @return
	 * @throws IOException
	 */
	private synchronized <T> T preLoadTypeInfos() throws IOException {
		RecordManager recMan = null;
		try { 
			recMan = RecordManagerFactory.createRecordManager(pathToConfigData);
			PrimaryTreeMap<String,String> map = recMan.treeMap("TypeInfo");
			List<String> pvNames = new LinkedList<String>(map.keySet());
			JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
			for(String pvName : pvNames) { 
				String jsonStr = map.get(pvName);
				if(jsonStr != null) { 
					JSONObject jsonObj = (JSONObject) JSONValue.parse(jsonStr);
					PVTypeInfo obj = new PVTypeInfo();
					decoder.decode(jsonObj, obj);
					cachedTypeInfos.put(pvName, obj);
					if(logger.isDebugEnabled()) logger.debug("Caching typeInfo for PV " + pvName);
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		} finally { 
			if(recMan != null) { try { recMan.close(); recMan = null; } catch(Exception ex) {} } 
		}

		return null;
	}
}
