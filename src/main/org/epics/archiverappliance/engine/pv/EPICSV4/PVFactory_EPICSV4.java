package org.epics.archiverappliance.engine.pv.EPICSV4;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.pv.PV;

public class PVFactory_EPICSV4 {

	public static PV createEPICSPV(String name, ConfigService configservice) {
		return new EPICS_V4_PV(name, configservice);
	}
}
