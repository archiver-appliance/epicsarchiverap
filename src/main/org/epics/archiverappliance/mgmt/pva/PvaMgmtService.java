package org.epics.archiverappliance.mgmt.pva;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetAllPVs;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetApplianceInfo;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetArchivedPVs;
import org.epics.archiverappliance.mgmt.pva.actions.PvaGetPVStatus;
import org.epics.archiverappliance.mgmt.pva.actions.PvaAction;
import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVATable;
import org.epics.pva.data.nt.PVAURI;
import org.epics.pva.server.RPCService;

/**
 * PVAccess API to the Mgmt Service
 *
 * @author Kunal Shroff
 *
 */
public class PvaMgmtService implements RPCService {

	private static Logger logger = LogManager.getLogger(PvaMgmtService.class.getName());
	private final ConfigService configService;

	public static final String PVA_MGMT_SERVICE = "pvaMgmtService";

	/**
	 * List of supported operations
	 */
	Map<String, PvaAction> actions = new HashMap<String, PvaAction>();

	/**
	 * Construct the mgmt service.
	 *
	 * @param configService
	 */
	public PvaMgmtService(ConfigService configService) {
		this.configService = configService;
		logger.info("Creating an instance of PvaMgmtService");
		actions.put(PvaGetAllPVs.NAME, new PvaGetAllPVs());
		actions.put(PvaGetApplianceInfo.NAME, new PvaGetApplianceInfo());
		actions.put(PvaArchivePVAction.NAME, new PvaArchivePVAction());
		actions.put(PvaGetArchivedPVs.NAME, new PvaGetArchivedPVs());
		actions.put(PvaGetPVStatus.NAME, new PvaGetPVStatus());
	}

	@Override
	public PVAStructure call(PVAStructure args) throws Exception {
		PVAURI uri = PVAURI.fromStructure(args);
		PVATable table = PVATable.fromStructure(args);
		if (uri != null) {
			if (actions.get(uri.getPath()) != null) {
				return actions.get(uri.getPath()).request(args, configService);
			} else {
				throw new UnsupportedOperationException("The requested operation is not supported " + uri.getPath());
			}
		} else if (table != null) {
			if (actions.get(table.getDescriptor().get()) != null) {
				return actions.get(table.getDescriptor().get()).request(args,  configService);
			} else {
				throw new UnsupportedOperationException(
						"The requested operation is not supported " + table.getDescriptor().get());
			}
		} else {
			// Unable to handle the request args
			throw new IllegalArgumentException(PVA_MGMT_SERVICE + " only supports request args of type NTURI or NTTable ");
		}
	}

}
