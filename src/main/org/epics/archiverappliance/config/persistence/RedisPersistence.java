package org.epics.archiverappliance.config.persistence;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Uses redis as a persistence layer.  
 * To set the path to the redis instance, use the environment variable ARCHAPPL_PERSISTENCE_LAYER_REDISURL.
 * This defaults to <code>localhost</code>
 * To use this persistence layer, use
 * <pre>
 * export ARCHAPPL_PERSISTENCE_LAYER="org.epics.archiverappliance.config.persistence.RedisPersistence"
 * export ARCHAPPL_PERSISTENCE_LAYER_REDISURL="localhost"
 * </pre>
 * @author mshankar
 *
 */
public class RedisPersistence implements ConfigPersistence {
	private static Logger logger = Logger.getLogger(RedisPersistence.class.getName());
	public static final String ARCHAPPL_PERSISTENCE_LAYER_REDISURL = ConfigService.ARCHAPPL_PERSISTENCE_LAYER + "_REDISURL";
	private static String redisURL = "localhost";
	private JedisPool jedisPool = null;

	
	public RedisPersistence() throws ConfigException { 
		String pathFromEnv = System.getProperty(ARCHAPPL_PERSISTENCE_LAYER_REDISURL);
		if(pathFromEnv == null) { 
			pathFromEnv = System.getenv(ARCHAPPL_PERSISTENCE_LAYER_REDISURL);
		}
		if(pathFromEnv != null) { 
			redisURL = pathFromEnv;
			jedisPool = new JedisPool(redisURL);
		}
		logger.info("PV information is persisted in the redis instance at " + redisURL);
	}
	
	@Override
	public List<String> getTypeInfoKeys() throws IOException {
		return getKeys("TypeInfo");
	}
	
	@Override
	public List<PVTypeInfo> getAllTypeInfosForAppliance(String applianceIdentity) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public PVTypeInfo getTypeInfo(String pvName) throws IOException {
		logger.debug("Getting typeinfo for pv " + pvName + " from db instead of cache");
		return getValueForKey("TypeInfo", pvName, new PVTypeInfo(), PVTypeInfo.class);
	}

	@Override
	public void putTypeInfo(String pvName, PVTypeInfo typeInfo) throws IOException {
		putValueForKey("TypeInfo", pvName, typeInfo, PVTypeInfo.class);
	}

	@Override
	public void deleteTypeInfo(String pvName) throws IOException {
		logger.debug("Removing typeinfo for pv " + pvName + " from db and cache");
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
	
	private List<String> getKeys(String recordName) throws IOException {
		try(Jedis jedis = jedisPool.getResource()) {
			Set<String> keysFromRedis = jedis.keys(recordName + "/*");
			LinkedList<String> retVal = new LinkedList<String>();
			for(String keyFromRedis : keysFromRedis) { 
				retVal.add(keyFromRedis.split("/")[1]);
			}
			return retVal;
		}	
	}
	
	private <T> T getValueForKey(String recordName, String key, T obj, Class<T> clazz) throws IOException {
		try(Jedis jedis = jedisPool.getResource()) { 
			String jsonStr = jedis.get(recordName + "/" + key);
			if(jsonStr != null) { 
				JSONObject jsonObj = (JSONObject) JSONValue.parse(jsonStr);
				JSONDecoder<T> decoder = JSONDecoder.getDecoder(clazz);
				decoder.decode(jsonObj, obj);
				return obj;
			}
		} catch(Exception ex) { 
			throw new IOException(ex);
		}
		logger.debug("Cannot find data for key " + recordName + "/" + key + " in redis");
		return null;
	}
	
	private String getStringValueForKey(String recordName, String key) throws IOException {
		try(Jedis jedis = jedisPool.getResource()) { 
			String jsonStr = jedis.get(recordName + "/" + key);
			return jsonStr;
		} catch(Exception ex) { 
			throw new IOException(ex);
		}
	}
	
	private <T> void putValueForKey(String recordName, String key, T obj, Class<T> clazz) throws IOException {
		if(key == null || key.equals("")) throw new IOException("key cannot be null when persisting " + recordName);
		if(obj == null || obj.equals("")) throw new IOException("value cannot be null when persisting " + recordName);

		try(Jedis jedis = jedisPool.getResource()) { 
			JSONEncoder<T> encoder = JSONEncoder.getEncoder(clazz);
			JSONObject jsonObj = encoder.encode(obj);
			String jsonStr = jsonObj.toJSONString();
			jedis.set(recordName + "/" + key, jsonStr);
		} catch(Exception ex) { 
			throw new IOException(ex);
		}
	}
	
	private void putStringValueForKey(String recordName, String key, String value) throws IOException {
		if(key == null || key.equals("")) throw new IOException("key cannot be null when persisting " + recordName);
		if(value == null || value.equals("")) throw new IOException("value cannot be null when persisting " + recordName);

		try(Jedis jedis = jedisPool.getResource()) { 
			jedis.set(recordName + "/" + key, value);
		} catch(Exception ex) { 
			throw new IOException(ex);
		}
	}
	
	private void removeKey(String recordName, String key) throws IOException {
		try(Jedis jedis = jedisPool.getResource()) { 
			jedis.del(recordName + "/" + key);
		} catch(Exception ex) { 
			throw new IOException(ex);
		}
	}
}
