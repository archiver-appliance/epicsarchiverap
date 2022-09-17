package org.epics.archiverappliance.config.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
 * Uses SQLite as a persistence layer; use in small, non-clustered, reasonably static deployments with minimal concurrent access.
 * SQLite stores its data in a file, to set the path to the SQLite file, use the environment variable ARCHAPPL_PERSISTENCE_LAYER_SQLITEFILENAME.
 * This defaults to <code>./archapplconfig.db</code>
 * To use this persistence layer, use
 * <pre>
 * export ARCHAPPL_PERSISTENCE_LAYER="org.epics.archiverappliance.config.persistence.SQLitePersistence"
 * export ARCHAPPL_PERSISTENCE_LAYER_SQLITEFILENAME="/scratch/Archiver/archappl.db"
 * </pre>
 * This is extremely similar the MySQL implementation with the exception that the tables are created if we are creating the file for the first time. 
 * Some of the SQL Upsert syntax is also different.
 * 
 * This implementation tries to set WAL to improve write performance but continues on if setting pragma fails.
 * Using WAL usually implies the presence of a .shm and .wal file in addition to the .db file. 
 * 
 * @author mshankar
 *
 */
public class SQLitePersistence implements ConfigPersistence {
	private static Logger configlogger = Logger.getLogger("config." + SQLitePersistence.class.getName());
	private static Logger logger = Logger.getLogger(SQLitePersistence.class.getName());
	public static final String ARCHAPPL_SQLITE_FILENAME = ConfigService.ARCHAPPL_PERSISTENCE_LAYER + "_SQLITEFILENAME";
	private String pathToConfigData = "./archapplconfig.db";
	private String sqliteJDBCURL = null;
//	private ConcurrentHashMap<String, PVTypeInfo> cachedTypeInfos = new ConcurrentHashMap<String, PVTypeInfo>();
	private Connection theConnection;
	
	public SQLitePersistence() throws ConfigException {
		try { 
			Class.forName("org.sqlite.JDBC");
		} catch (Exception ex) { 
			throw new ConfigException("Cannot find the SQLite database driver in the classpath. Please download the library from https://github.com/xerial/sqlite-jdbc and copy to the Tomcat lib folder", ex);
		}

		String pathFromEnv = System.getProperty(ARCHAPPL_SQLITE_FILENAME);
		if(pathFromEnv == null) { 
			pathFromEnv = System.getenv(ARCHAPPL_SQLITE_FILENAME);
		}
		if(pathFromEnv != null) { 
			pathToConfigData = pathFromEnv;
		}
		sqliteJDBCURL = "jdbc:sqlite:" + pathToConfigData;
		
		if(!Files.exists(Paths.get(pathToConfigData))) {
			try {
				configlogger.info("The SQLite data file " + sqliteJDBCURL + " does not exist. Creating the file, tables and indexes");
				createDataFileWithTablesAndIndices();
			} catch(SQLException ex) {
				throw new ConfigException("Cannot create tables and indices in SQLite data file " + pathToConfigData, ex);
			}
		}
		
		try {
			theConnection = DriverManager.getConnection(sqliteJDBCURL);
			theConnection.setAutoCommit(true);
			try(Statement stmt = theConnection.createStatement()) {
				stmt.executeUpdate("pragma journal_mode=wal");
			} catch(SQLException ex) {
				configlogger.error("Exception setting WAL pragma - https://www.sqlite.org/wal.html", ex);
			}
		} catch(SQLException ex) {
			throw new ConfigException("Cannot initialize the JDBC Connection to " + pathToConfigData, ex);
		}

		configlogger.info("Loading SQLite data from " + pathToConfigData);
		
//		try {
//			preLoadTypeInfos();
//		} catch(IOException ex) { 
//			throw new ConfigException("Exception preloading pvTypeInfos", ex);
//		}
//		logger.info("Done caching " + cachedTypeInfos.size() + " pvTypeInfos from " + pathToConfigData);
	}
	
	@Override
	public void initialize(ConfigService configService) {
		configService.addShutdownHook(() -> {
			try {
				theConnection.close();
			} catch(Exception ex) {
				configlogger.error("Exception closing connection to SQLite " + pathToConfigData, ex);
			}
		});
	}

	private void createDataFileWithTablesAndIndices() throws SQLException {
		try(Connection conn = DriverManager.getConnection(sqliteJDBCURL)) {
			try(Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("CREATE TABLE PVTypeInfo ( pvName TEXT NOT NULL PRIMARY KEY, typeInfoJSON TEXT NOT NULL)");
				stmt.executeUpdate("CREATE TABLE PVAliases ( pvName TEXT NOT NULL PRIMARY KEY, realName TEXT NOT NULL)");
				stmt.executeUpdate("CREATE TABLE ArchivePVRequests ( pvName TEXT NOT NULL PRIMARY KEY, userParams TEXT NOT NULL)");
				stmt.executeUpdate("CREATE TABLE ExternalDataServers ( serverid TEXT NOT NULL PRIMARY KEY, serverinfo TEXT NOT NULL)");
			}
		}
	}
	
	@Override
	public List<String> getTypeInfoKeys() throws IOException {
		return getKeys("SELECT pvName AS pvName FROM PVTypeInfo ORDER BY pvName;", "getTypeInfoKeys");
	}
	
	private static boolean regexmatch(String typeInfoStr, Pattern instanceIDMatcher) {
		return instanceIDMatcher.matcher(typeInfoStr).matches();
	}

	private static boolean attrmatch(PVTypeInfo typeInfo, String instanceIdentity) {
		return typeInfo.getApplianceIdentity().equals(instanceIdentity);
	}
	
	private static PVTypeInfo parseTypeInfo(String typeInfoStr, JSONDecoder<PVTypeInfo> decoder) {
		try {
			JSONObject jsonObj = (JSONObject) JSONValue.parse(typeInfoStr);
			PVTypeInfo typeInfo = new PVTypeInfo();
			decoder.decode(jsonObj, typeInfo);
			return typeInfo;
		} catch(Exception ex) {
			return new PVTypeInfo();
		}
	}
	
	@Override
	public List<PVTypeInfo> getAllTypeInfosForAppliance(String applianceIdentity) throws IOException {
		try {
			List<String> typeInfoStrs = new LinkedList<String>();
			synchronized(this) {
				try(PreparedStatement stmt = theConnection.prepareStatement("SELECT typeInfoJSON AS typeInfoJSON FROM PVTypeInfo WHERE typeInfoJSON LIKE ?;")) {
					stmt.setString(1, "%\""+ applianceIdentity + "\"%");
					try(ResultSet rs = stmt.executeQuery()) {
						while (rs.next()) {
							typeInfoStrs.add(rs.getString("typeInfoJSON"));
						}
					}
				}
			}

			Pattern inst = Pattern.compile(".*\\\"" + applianceIdentity + "\\\".*");
			JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);
			ForkJoinPool customThreadPool = new ForkJoinPool(Math.max(Runtime.getRuntime().availableProcessors()/2, 1));
			List<PVTypeInfo> typeInfos = customThreadPool.submit(
			    () -> typeInfoStrs.parallelStream()
			    	.filter(x -> regexmatch(x, inst))
			    	.map(x -> parseTypeInfo(x, decoder))
			    	.filter(x -> attrmatch(x, applianceIdentity))
			    	.collect(Collectors.toList())).get();
			customThreadPool.shutdown();
			return typeInfos;
		} catch(Exception ex) {
			throw new IOException("Exception getting all typeinfos", ex);
		}
	}

	@Override
	public PVTypeInfo getTypeInfo(String pvName) throws IOException {
		return getValueForKey("SELECT typeInfoJSON AS typeInfoJSON FROM PVTypeInfo WHERE pvName = ?;", pvName, new PVTypeInfo(), PVTypeInfo.class, "getTypeInfo");
	}

	@Override
	public void putTypeInfo(String pvName, PVTypeInfo typeInfo) throws IOException {
		putValueForKey("INSERT INTO PVTypeInfo (pvName, typeInfoJSON) VALUES (?, ?) ON CONFLICT(pvName) DO UPDATE SET typeInfoJSON = ?;", pvName, typeInfo, PVTypeInfo.class, "putTypeInfo");
	}

	@Override
	public void deleteTypeInfo(String pvName) throws IOException {
		removeKey("DELETE FROM PVTypeInfo WHERE pvName = ?;", pvName, "deleteTypeInfo");
	}

	@Override
	public List<String> getArchivePVRequestsKeys() throws IOException {
		return getKeys("SELECT pvName AS pvName FROM ArchivePVRequests ORDER BY pvName;", "getArchivePVRequestsKeys");
	}

	@Override
	public UserSpecifiedSamplingParams getArchivePVRequest(String pvName) throws IOException {
		return getValueForKey("SELECT userParams AS userParams FROM ArchivePVRequests WHERE pvName = ?;", pvName, new UserSpecifiedSamplingParams(), UserSpecifiedSamplingParams.class, "getArchivePVRequest");
	}

	@Override
	public void putArchivePVRequest(String pvName, UserSpecifiedSamplingParams userParams) throws IOException {
		putValueForKey("INSERT INTO ArchivePVRequests (pvName, userParams) VALUES (?, ?) ON CONFLICT(pvName) DO UPDATE SET userParams = ?;", pvName, userParams, UserSpecifiedSamplingParams.class, "putArchivePVRequest");
	}
	
	@Override
	public void removeArchivePVRequest(String pvName) throws IOException {
		removeKey("DELETE FROM ArchivePVRequests WHERE pvName = ?;", pvName, "removeArchivePVRequest");
	}

	@Override
	public List<String> getExternalDataServersKeys() throws IOException {
		return getKeys("SELECT serverid AS serverid FROM ExternalDataServers ORDER BY serverid;", "getExternalDataServersKeys");
	}

	@Override
	public String getExternalDataServer(String serverId) throws IOException {
		return getStringValueForKey("SELECT serverinfo AS serverinfo FROM ExternalDataServers WHERE serverid = ?;", serverId, "getExternalDataServer");
	}

	@Override
	public void putExternalDataServer(String serverId, String serverInfo) throws IOException {
		putStringValueForKey("INSERT INTO ExternalDataServers (serverid, serverinfo) VALUES (?, ?) ON CONFLICT(serverid) DO UPDATE SET serverinfo = ?;", serverId, serverInfo, "putExternalDataServer");	
	}
	
	@Override
	public void removeExternalDataServer(String serverId, String serverInfo) throws IOException {
		removeKey("DELETE FROM ExternalDataServers WHERE serverid = ?;", serverId, "removeExternalDataServer");
	}



	@Override
	public List<String> getAliasNamesToRealNamesKeys() throws IOException {
		return getKeys("SELECT pvName AS pvName FROM PVAliases ORDER BY pvName;", "getAliasNamesToRealNamesKeys");
	}

	@Override
	public String getAliasNamesToRealName(String pvName) throws IOException {
		return getStringValueForKey("SELECT realName AS realName FROM PVAliases WHERE pvName = ?;", pvName, "getAliasNamesToRealName");
	}

	@Override
	public void putAliasNamesToRealName(String pvName, String realName) throws IOException {
		putStringValueForKey("INSERT INTO PVAliases (pvName, realName) VALUES (?, ?) ON CONFLICT(pvName) DO UPDATE SET realName = ?;", pvName, realName, "putAliasNamesToRealName");
	}
	
	@Override
	public void removeAliasName(String pvName, String realName) throws IOException { 
		removeKey("DELETE FROM PVAliases WHERE pvName = ?;", pvName, "removeAliasName");
	}

	
	
	private List<String> getKeys(String sql, String msg) throws IOException {
		LinkedList<String> ret = new LinkedList<String>();
		synchronized(this) {
			try(PreparedStatement stmt = theConnection.prepareStatement(sql)) {
				try(ResultSet rs = stmt.executeQuery()) {
					while(rs.next()) {
						String pvName = rs.getString(1);
						ret.add(pvName);
					}
				}
			} catch(SQLException ex) {
				throw new IOException(ex);
			}
		}
		logger.debug(msg + " returns " + ret.size() + " keys");
		return ret;
	}
	
	
	
	private <T> T getValueForKey(String sql, String key, T obj, Class<T> clazz, String msg) throws IOException {
		if(key == null || key.equals("")) return null;
		
		synchronized(this) {
			try(PreparedStatement stmt = theConnection.prepareStatement(sql)) {
				stmt.setString(1, key);
				try(ResultSet rs = stmt.executeQuery()) {
					while(rs.next()) {
						String jsonStr = rs.getString(1);
						JSONObject jsonObj = (JSONObject) JSONValue.parse(jsonStr);
						JSONDecoder<T> decoder = JSONDecoder.getDecoder(clazz);
						decoder.decode(jsonObj, obj);
						return obj;
					}
				}
			} catch(Exception ex) {
				throw new IOException(ex);
			}
		}
		
		return null;
	}
	
	private String getStringValueForKey(String sql, String key, String msg) throws IOException {
		if(key == null || key.equals("")) return null;
		
		synchronized(this) {
			try(PreparedStatement stmt = theConnection.prepareStatement(sql)) {
				stmt.setString(1, key);
				try(ResultSet rs = stmt.executeQuery()) {
					while(rs.next()) {
						return rs.getString(1);
					}
				}
			} catch(Exception ex) {
				throw new IOException(ex);
			}
		}
		
		return null;
	}

	
	
	private <T> void putValueForKey(String sql, String key, T obj, Class<T> clazz, String msg) throws IOException {
		if(key == null || key.equals("")) throw new IOException("key cannot be null when persisting " + msg);
		if(obj == null || obj.equals("")) throw new IOException("value cannot be null when persisting " + msg);
		
		synchronized(this) {			
			try(PreparedStatement stmt = theConnection.prepareStatement(sql)) {
				JSONEncoder<T> encoder = JSONEncoder.getEncoder(clazz);
				JSONObject jsonObj = encoder.encode(obj);
				String jsonStr = jsonObj.toJSONString();

				stmt.setString(1, key);
				stmt.setString(2, jsonStr);
				stmt.setString(3, jsonStr);
				int rowsChanged = stmt.executeUpdate();
				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when updating key  " + key + " in " + msg);
				} else {
					logger.debug("Successfully updated value for key " + key + " in " + msg);
				}
			} catch(Exception ex) {
				throw new IOException(ex);
			}
		}
	}
	
	
	private void putStringValueForKey(String sql, String key, String value, String msg) throws IOException {
		if(key == null || key.equals("")) throw new IOException("key cannot be null when persisting " + msg);
		if(value == null || value.equals("")) throw new IOException("value cannot be null when persisting " + msg);
		
		synchronized(this) {
			try(PreparedStatement stmt = theConnection.prepareStatement(sql)) {
				stmt.setString(1, key);
				stmt.setString(2, value);
				stmt.setString(3, value);
				int rowsChanged = stmt.executeUpdate();
				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when updating key  " + key + " in " + msg);
				} else {
					logger.debug("Successfully updated value for key " + key + " in " + msg);
				}
			} catch(Exception ex) {
				throw new IOException(ex);
			}
		}
	}
	
	
	private void removeKey(String sql, String key, String msg) throws IOException {
		synchronized(this) {
			try(PreparedStatement stmt = theConnection.prepareStatement(sql)) {
				stmt.setString(1, key);
				int rowsChanged = stmt.executeUpdate();
				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when removing key  " + key + " in " + msg);
				} else {
					logger.debug("Successfully removed key " + key + " in " + msg);
				}
			} catch(SQLException ex) {
				throw new IOException(ex);
			}
		}
	}
}
