package org.epics.archiverappliance.config.persistence;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigPersistence;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Persistence layer ontop of MySQL TEXT(blobs) on InnoDB
 * Bear in mind, an untuned mysql server does poorly in terms of performance.
 * At the minimum consider using innodb_flush_log_at_trx_commit=0 if you plan to import large numbers of pvs etc.
 * @author mshankar
 *
 */
public class MySQLPersistence implements ConfigPersistence {
	private static Logger configlogger = LogManager.getLogger("config." + MySQLPersistence.class.getName());
	private static Logger logger = LogManager.getLogger(MySQLPersistence.class.getName());
	private DataSource theDataSource;

	private static enum dialect_t {
		MySQL,
		SQLite,
	};
	private dialect_t dialect;
	
	public MySQLPersistence() throws ConfigException {
		try {
			configlogger.info("Looking up datasource called jdbc/archappl in the java:/comp/env namespace using JDNI");
			Context initContext = new InitialContext();
			Context envContext  = (Context) initContext.lookup("java:/comp/env");

            String dbname = System.getenv().get("ARCHAPPL_DB_NAME");
            if ( dbname != null && dbname.length() > 0 ) {
                configlogger.info("Using DB name from environment variable:" + dbname);
            }
            else {
                configlogger.info("Using default MySQL database name.");
                dbname = "archappl";
            }
            String db_string = "jdbc/" +  dbname;
			theDataSource = (DataSource)envContext.lookup(db_string);
			configlogger.info("Found datasource called jdbc/archappl in the java:/comp/env namespace using JDNI");

			// test RDB connection and probe type
			try(Connection conn = theDataSource.getConnection()) {
				DatabaseMetaData meta = conn.getMetaData();
				String name = meta.getDatabaseProductName();

				configlogger.info(String.format("RDB Engine is '%s' '%s'", name, meta.getDatabaseProductVersion()));
				configlogger.info(String.format("RDB Driver is '%s' '%s'", meta.getDriverName(), meta.getDriverVersion()));

				if(Pattern.compile(".*sqlite.*", Pattern.CASE_INSENSITIVE).matcher(name).matches()) {
					dialect = dialect_t.SQLite;
				} else {
					dialect = dialect_t.MySQL;
				}
				configlogger.info(String.format("SQL Dialect %s", dialect.toString()));
			}
		} catch(Exception ex) {
			throw new ConfigException("Exception initializing MySQLPersistence ", ex);
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
			try(Connection conn = theDataSource.getConnection()) {
				try(PreparedStatement stmt = conn.prepareStatement("SELECT typeInfoJSON AS typeInfoJSON FROM PVTypeInfo WHERE typeInfoJSON LIKE ?;")) {
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
		String sql;
		switch(dialect) {
		default:
		case MySQL:
			sql = "INSERT INTO PVTypeInfo (pvName, typeInfoJSON) VALUES (?, ?) ON DUPLICATE KEY UPDATE typeInfoJSON = ?;";
			break;
		case SQLite:
			sql = "INSERT INTO PVTypeInfo (pvName, typeInfoJSON) VALUES (?, ?) ON CONFLICT(pvName) DO UPDATE SET typeInfoJSON = ?;";
			break;
		}
		putValueForKey(sql, pvName, typeInfo, PVTypeInfo.class, "putTypeInfo");
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
		String sql;
		switch(dialect) {
		default:
		case MySQL:
			sql = "INSERT INTO ArchivePVRequests (pvName, userParams) VALUES (?, ?) ON DUPLICATE KEY UPDATE userParams = ?;";
			break;
		case SQLite:
			sql = "INSERT INTO ArchivePVRequests (pvName, userParams) VALUES (?, ?) ON CONFLICT(pvName) DO UPDATE SET userParams = ?;";
			break;
		}
		putValueForKey(sql, pvName, userParams, UserSpecifiedSamplingParams.class, "putArchivePVRequest");
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
		String sql;
		switch(dialect) {
		default:
		case MySQL:
			sql = "INSERT INTO ExternalDataServers (serverid, serverinfo) VALUES (?, ?) ON DUPLICATE KEY UPDATE serverinfo = ?;";
			break;
		case SQLite:
			sql = "INSERT INTO ExternalDataServers (serverid, serverinfo) VALUES (?, ?) ON CONFLICT(serverid) DO UPDATE SET serverinfo = ?;";
			break;
		}
		putStringValueForKey(sql, serverId, serverInfo, "putExternalDataServer");	
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
		String sql;
		switch(dialect) {
		default:
		case MySQL:
			sql = "INSERT INTO PVAliases (pvName, realName) VALUES (?, ?) ON DUPLICATE KEY UPDATE realName = ?;";
			break;
		case SQLite:
			sql = "INSERT INTO PVAliases (pvName, realName) VALUES (?, ?) ON CONFLICT(pvName) DO UPDATE SET realName = ?;";
			break;
		}
		putStringValueForKey(sql, pvName, realName, "putAliasNamesToRealName");
	}

	@Override
	public void removeAliasName(String pvName, String realName) throws IOException {
		removeKey("DELETE FROM PVAliases WHERE pvName = ?;", pvName, "removeAliasName");
	}



	private List<String> getKeys(String sql, String msg) throws IOException {
		LinkedList<String> ret = new LinkedList<String>();
		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				try(ResultSet rs = stmt.executeQuery()) {
					while(rs.next()) {
						String pvName = rs.getString(1);
						ret.add(pvName);
					}
				}
			}
		} catch(SQLException ex) {
			throw new IOException(ex);
		}
		logger.debug(msg + " returns " + ret.size() + " keys");
		return ret;
	}



	private <T> T getValueForKey(String sql, String key, T obj, Class<T> clazz, String msg) throws IOException {
		if(key == null || key.equals("")) return null;

		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
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
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}

		return null;
	}

	private String getStringValueForKey(String sql, String key, String msg) throws IOException {
		if(key == null || key.equals("")) return null;

		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, key);
				try(ResultSet rs = stmt.executeQuery()) {
					while(rs.next()) {
						return rs.getString(1);
					}
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}

		return null;
	}



	private <T> void putValueForKey(String sql, String key, T obj, Class<T> clazz, String msg) throws IOException {
		if(key == null || key.equals("")) throw new IOException("key cannot be null when persisting " + msg);
		if(obj == null || obj.equals("")) throw new IOException("value cannot be null when persisting " + msg);

		try(Connection conn = theDataSource.getConnection()) {
			JSONEncoder<T> encoder = JSONEncoder.getEncoder(clazz);
			JSONObject jsonObj = encoder.encode(obj);
			String jsonStr = jsonObj.toJSONString();

			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, key);
				stmt.setString(2, jsonStr);
				stmt.setString(3, jsonStr);
				int rowsChanged = stmt.executeUpdate();
				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when updating key  " + key + " in " + msg);
				} else {
					logger.debug("Successfully updated value for key " + key + " in " + msg);
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
	}


	private void putStringValueForKey(String sql, String key, String value, String msg) throws IOException {
		if(key == null || key.equals("")) throw new IOException("key cannot be null when persisting " + msg);
		if(value == null || value.equals("")) throw new IOException("value cannot be null when persisting " + msg);

		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, key);
				stmt.setString(2, value);
				stmt.setString(3, value);
				int rowsChanged = stmt.executeUpdate();
				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when updating key  " + key + " in " + msg);
				} else {
					logger.debug("Successfully updated value for key " + key + " in " + msg);
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
	}


	private void removeKey(String sql, String key, String msg) throws IOException {
		try(Connection conn = theDataSource.getConnection()) {
			try(PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, key);
				int rowsChanged = stmt.executeUpdate();
				if(rowsChanged != 1) {
					logger.warn(rowsChanged + " rows changed when removing key  " + key + " in " + msg);
				} else {
					logger.debug("Successfully removed key " + key + " in " + msg);
				}
			}
		} catch(SQLException ex) {
			throw new IOException(ex);
		}
	}
}
