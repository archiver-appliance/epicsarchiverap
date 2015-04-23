package org.epics.archiverappliance.engine.pv;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;

public class PVFactory {
	/**
	 * This is the constructor used by the MetaGet's - this is the initial step in adding a PV to the archiver where we determine some facts about the PV for the policies
	 * @param name
	 * @param configservice
	 * @param jcaCommandThreadId
	 * @return
	 */
	public static PV createPV(String name, ConfigService configservice, int jcaCommandThreadId) { 
		if(PVNames.isEPICSV4PVName(name)) { 
			return new EPICS_V4_PV(PVNames.stripPrefixFromName(name), configservice, jcaCommandThreadId);
		} else { 
			return new EPICS_V3_PV(name, configservice, jcaCommandThreadId);
		}
	}
	
	/**
	 * This is the constructor used by the ArchiveChannel to create the main PV.
	 * @param name
	 * @param configservice
	 * @param isControlPV
	 * @param archDBRTypes
	 * @param jcaCommandThreadId
	 * @return
	 */
	public static PV createPV(final String name, ConfigService configservice, boolean isControlPV, ArchDBRTypes archDBRTypes, int jcaCommandThreadId) {
		if(PVNames.isEPICSV4PVName(name)) { 
			return new EPICS_V4_PV(PVNames.stripPrefixFromName(name), configservice, isControlPV, archDBRTypes, jcaCommandThreadId);			
		} else { 
			return new EPICS_V3_PV(name, configservice, isControlPV, archDBRTypes, jcaCommandThreadId);
		}
	}

}
