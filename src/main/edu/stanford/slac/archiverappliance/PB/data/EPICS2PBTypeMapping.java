package edu.stanford.slac.archiverappliance.PB.data;

import gov.aps.jca.dbr.DBR;

import java.lang.reflect.Constructor;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.pva.data.PVAStructure;

/**
 * Separate out the JCA/EPICS v4 =&gt; PB mapping into a separate class so that clients (read ArchiveViewer) do not have to include the entire JCA/EPICS v4 jars.
 * This relies on its sister class DBR2PBTypeMapping...
 * @author mshankar
 *
 */
public class EPICS2PBTypeMapping {
	private static Logger logger = LogManager.getLogger(EPICS2PBTypeMapping.class.getName());
	private static HashMap<ArchDBRTypes, EPICS2PBTypeMapping> typemap = new HashMap<ArchDBRTypes, EPICS2PBTypeMapping>();

	// Initialization stuff from now on
	static {
		for(ArchDBRTypes t : ArchDBRTypes.values()) {
			DBR2PBTypeMapping pbClass = DBR2PBTypeMapping.getPBClassFor(t);
			if(pbClass == null) {
				throw new RuntimeException("We have a type in DBR type that does have an equivalent PB type");
			}
			typemap.put(t, new EPICS2PBTypeMapping(pbClass.pbclass));
		}
	}
	
	private Constructor<? extends DBRTimeEvent> JCADBRConstructor;
	private Constructor<? extends DBRTimeEvent> EPICSV4DBRConstructor;
	private EPICS2PBTypeMapping(Class<? extends DBRTimeEvent> pbClass) {
		try {
			JCADBRConstructor = pbClass.getConstructor(DBR.class);
		} catch (Exception ex) {
		}

		try {
			EPICSV4DBRConstructor = pbClass.getConstructor(PVAStructure.class);
		} catch (Exception ex) {
		}
		

		if(JCADBRConstructor == null && EPICSV4DBRConstructor == null) {
			String msg = "Cannot get a DBR constructor for PB event for class " + pbClass.getName() + ". We should either have something that comes from JCA or from Epics V4";
			logger.error(msg);
			throw new RuntimeException(msg);
		}
	}

	
	
	/**
	 * Get to the constructors that construct DBRTimeEvents from EPICS v3/v4 (JCA DBR etc) types this way.
	 * @param type ArchDBRTypes
	 * @return typemap.get(type)
	 */
	public static EPICS2PBTypeMapping getPBClassFor(ArchDBRTypes type) {
		return typemap.get(type);
	}

	/**
	 * Get a constructor that takes a JCA DBR and spits out a DBRTimeEvent
	 * @return JCADBRConstructor
	 */
	public Constructor<? extends DBRTimeEvent> getJCADBRConstructor() {
		return JCADBRConstructor;
	}

	/**
	 * Get a constructor that takes a Data_EPICSV4 and spits out a DBRTimeEvent
	 * @return EPICSV4DBRConstructor
	 */
	public Constructor<? extends DBRTimeEvent> getEPICSV4DBRConstructor() {
		return EPICSV4DBRConstructor;
	}
	
	
	
}
