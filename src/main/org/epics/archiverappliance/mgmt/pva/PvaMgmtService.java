package org.epics.archiverappliance.mgmt.pva;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetAllPVs;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetApplianceInfo;
import org.epics.archiverappliance.mgmt.pva.actions.PvaAction;
import org.epics.nt.NTURI;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvaccess.server.rpc.RPCServiceAsync;
import org.epics.pvdata.pv.PVStructure;

/**
 * 
 * @author Kunal Shroff
 *
 */
public class PvaMgmtService implements RPCServiceAsync {

	private static Logger logger = Logger.getLogger(PvaMgmtService.class.getName());
	private final ConfigService configService;

	public static final String PVA_MGMT_SERVICE = "pvaMgmtService";

	/**
	 * List of supported operations
	 */
	Map<String, PvaAction> actions = new HashMap<String, PvaAction>();

	public PvaMgmtService(ConfigService configService) {
		this.configService = configService;
		logger.info("Creating an instance of PvaMgmtService");
		actions.put(PvaGetAllPVs.NAME, new PvaGetAllPVs());
		actions.put(PvaGetApplianceInfo.NAME, new PvaGetApplianceInfo());
	}

	/**
	 * 
	 */
	@Override
	public void request(PVStructure args, RPCResponseCallback callback) {
		NTURI uri = NTURI.wrap(args);
		actions.get(uri.getPath().get()).request(args, callback, configService);
	}

}
