package org.epics.archiverappliance.engine.pv;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;

public class PVFactory {
	/**
	 * This is the constructor used by the MetaGet's - this is the initial step in adding a PV to the archiver where we determine some facts about the PV for the policies
	 * @param name  The PV name. 
	 * @param configservice  The config service used by this pv 
	 * @param jcaCommandThreadId The JCA Command thread.
	 * @param usePVAccess  &emsp;
	 * @return PV  &emsp;
	 */
	public static PV createPV(String name, ConfigService configservice, int jcaCommandThreadId, boolean usePVAccess) { 
		if(usePVAccess) { 
			return new EPICS_V4_PV(name, configservice, jcaCommandThreadId);
		} else { 
			return new EPICS_V3_PV(name, configservice, jcaCommandThreadId);
		}
	}
	
	/**
	 * This is the constructor used by the ArchiveChannel to create the main PV.
	 * @param name The PV name.  
	 * @param configservice The config service used by this pv 
	 * @param isControlPV  &emsp;
	 * @param archDBRTypes ArchDBRTypes
	 * @param jcaCommandThreadId  The JCA Command thread.
	 * @param usePVAccess Should we use PVAccess to connect to this PV.
	 * @param useDBEProperties &emsp;
	 * @return PV &emsp;
	 */
	public static PV createPV(final String name, ConfigService configservice, boolean isControlPV, ArchDBRTypes archDBRTypes, int jcaCommandThreadId, boolean usePVAccess, boolean useDBEProperties) {
		if(usePVAccess) { 
			return new EPICS_V4_PV(name, configservice, isControlPV, archDBRTypes, jcaCommandThreadId);			
		} else { 
			return new EPICS_V3_PV(name, configservice, isControlPV, archDBRTypes, jcaCommandThreadId, useDBEProperties);
		}
	}
	
	public static ControllingPV createControllingPV(final String name, ConfigService configservice, boolean isControlPV, ArchDBRTypes archDBRTypes, int jcaCommandThreadId, boolean usePVAccess) {
//		if(usePVAccess) {
//			// TODO Make EPICS_V4_PV implement controlling PV.
//			// return new EPICS_V4_PV(name, configservice, isControlPV, archDBRTypes, jcaCommandThreadId);
//		} else { 
		return new EPICS_V3_PV(name, configservice, isControlPV, archDBRTypes, jcaCommandThreadId, false);
//		}
	}
}
