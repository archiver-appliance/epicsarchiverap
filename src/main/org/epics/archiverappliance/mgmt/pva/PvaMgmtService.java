package org.epics.archiverappliance.mgmt.pva;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetAllPVs;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetApplianceInfo;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetArchivedPVs;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetPVStatus;
import org.epics.archiverappliance.mgmt.pva.actions.PvaAction;
import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.nt.NTTable;
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
		actions.put(PvaArchivePVAction.NAME, new PvaArchivePVAction());
		actions.put(PvaGetArchivedPVs.NAME, new PvaGetArchivedPVs());
		actions.put(PvaGetPVStatus.NAME, new PvaGetPVStatus());
	}

	/**
	 * 
	 */
	@Override
	public void request(PVStructure args, RPCResponseCallback callback) {
		if (NTURI.isCompatible(args)) {
			NTURI uri = NTURI.wrap(args);
			if (actions.get(uri.getPath().get()) != null) {
				actions.get(uri.getPath().get()).request(args, callback, configService);
			} else {
				new UnsupportedOperationException("The requested operation is not supported " + uri.getPath().get());
			}
		} else if (NTTable.isCompatible(args)) {
			NTTable ntTable = NTTable.wrap(args);
			if (actions.get(ntTable.getDescriptor().get()) != null) {
				actions.get(ntTable.getDescriptor().get()).request(args, callback, configService);
			} else {
				new UnsupportedOperationException(
						"The requested operation is not supported " + ntTable.getDescriptor().get());
			}
		} else {
			// Unable to handle the request args
			new IllegalArgumentException(PVA_MGMT_SERVICE + " only supports request args of type NTURI or NTTable ");
		}
	}

}
