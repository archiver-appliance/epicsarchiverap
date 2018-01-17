package org.epics.archiverappliance.mgmt.pva;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.pva.actions.GetAllPVs;
import org.epics.archiverappliance.mgmt.pva.actions.PVAAction;
import org.epics.nt.NTURI;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvaccess.server.rpc.RPCServiceAsync;
import org.epics.pvdata.pv.PVStructure;

/**
 * 
 * @author Kunal Shroff
 *
 */
public class PVAMgmtService implements RPCServiceAsync {

	private static Logger logger = Logger.getLogger(PVAMgmtService.class.getName());
	private final ConfigService configService;

	public static final String PVA_MGMT_SERVICE = "pvaMgmtService";

	/**
	 * List of supported operations
	 */
	Map<String, PVAAction> actions = new HashMap<String, PVAAction>();

	public PVAMgmtService(ConfigService configService) {
		this.configService = configService;
		logger.info("Creating an instance of PVAMgmtService");
		// TODO find and register all the supported services
		actions.put(GetAllPVs.GET_ALL_PVS, new GetAllPVs());
	}

	/**
	 * 
	 */
	@Override
	public void request(PVStructure args, RPCResponseCallback callback) {
		System.out.println("11111");
		NTURI uri = NTURI.wrap(args);
		System.out.println("22222" + uri.toString());
		System.out.println(uri.getPath().get());
		actions.get(uri.getPath().get()).request(args, callback, configService);
	}

}
