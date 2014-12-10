package org.epics.archiverappliance.config.persistence;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
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
	private static Logger configlogger = Logger.getLogger("config." + MySQLPersistence.class.getName());
	private static Logger logger = Logger.getLogger(MySQLPersistence.class.getName());
	private DataSource theDataSource;
	
	public MySQLPersistence() throws ConfigException {
		try {
			configlogger.info("Looking up datasource called jdbc/archappl in the java:/comp/env namespace using JDNI");
			Context initContext = new InitialContext();
			Context envContext  = (Context) initContext.lookup("java:/comp/env");
			theDataSource = (DataSource)envContext.lookup("jdbc/archappl");
			configlogger.info("Found datasource called jdbc/archappl in the java:/comp/env namespace using JDNI");
		} catch(Exception ex) {
			throw new ConfigException("Exception initializing MySQLPersistence ", ex);
		}
	}

	@Override
	public List<String> getTypeInfoKeys() throws IOException {
		return getKeys("SELECT pvName AS pvName FROM PVTypeInfo ORDER BY pvName;", "getTypeInfoKeys");
	}

	@Override
	public PVTypeInfo getTypeInfo(String pvName) throws IOException {
		return getValueForKey("SELECT typeInfoJSON AS typeInfoJSON FROM PVTypeInfo WHERE pvName = ?;", pvName, new PVTypeInfo(), PVTypeInfo.class, "getTypeInfo");
	}

	@Override
	public void putTypeInfo(String pvName, PVTypeInfo typeInfo) throws IOException {
		putValueForKey("INSERT INTO PVTypeInfo (pvName, typeInfoJSON) VALUES (?, ?) ON DUPLICATE KEY UPDATE typeInfoJSON = ?;", pvName, typeInfo, PVTypeInfo.class, "putTypeInfo");
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
		putValueForKey("INSERT INTO ArchivePVRequests (pvName, userParams) VALUES (?, ?) ON DUPLICATE KEY UPDATE userParams = ?;", pvName, userParams, UserSpecifiedSamplingParams.class, "putArchivePVRequest");
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
		putStringValueForKey("INSERT INTO ExternalDataServers (serverid, serverinfo) VALUES (?, ?) ON DUPLICATE KEY UPDATE serverinfo = ?;", serverId, serverInfo, "putExternalDataServer");	
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
		putStringValueForKey("INSERT INTO PVAliases (pvName, realName) VALUES (?, ?) ON DUPLICATE KEY UPDATE realName = ?;", pvName, realName, "putAliasNamesToRealName");
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
