package org.epics.archiverappliance.mgmt.pva;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.pva.actions.PvaAction;
import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.nt.NTTable;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvaccess.server.rpc.RPCServiceAsync;
import org.epics.pvdata.pv.PVStructure;

/**
 * A service to manage the addition of pv's for archiving
 * @author Kunal Shroff
 *
 */
public class PvaPvMgmtService implements RPCServiceAsync {

	private static Logger logger = Logger.getLogger(PvaPvMgmtService.class.getName());
	private final ConfigService configService;

	public static final String PVA_PV_MGMT_SERVICE = "pvaPvMgmtService";

	/**
	 * List of supported operations
	 */
	Map<String, PvaAction> actions = new HashMap<String, PvaAction>();

	public PvaPvMgmtService(ConfigService configService) {
		this.configService = configService;
		logger.info("Creating an instance of PvaPvMgmtService");
		PvaArchivePVAction pva = new PvaArchivePVAction();
		logger.info("Completed creation of action");
		actions.put(PvaArchivePVAction.NAME, pva);
		logger.info("Completed cration of PvaPvMgmtService");
	}

	/**
	 * 
	 */
	@Override
	public void request(PVStructure args, RPCResponseCallback callback) {
		if (NTTable.isCompatible(args)) {
			actions.get(NTTable.wrap(args).getDescriptor().get()).request(args, callback, configService);
		}
	}

}
