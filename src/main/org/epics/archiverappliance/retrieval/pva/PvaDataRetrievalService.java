package org.epics.archiverappliance.retrieval.pva;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
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
public class PvaDataRetrievalService implements RPCServiceAsync {

	public static final String PVA_DATA_SERVICE = "pvaDataRetrievalService";

	private static Logger logger = LogManager.getLogger(PvaDataRetrievalService.class.getName());
	private final ConfigService configService;

	/**
	 * List of supported operations
	 */
	Map<String, PvaAction> actions = new HashMap<String, PvaAction>();

	public PvaDataRetrievalService(ConfigService configService) {
		this.configService = configService;
		logger.info("Creating an instance of PvaDataRetrievalService");
		actions.put(PvaGetPVData.NAME, new PvaGetPVData());
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
				throw new UnsupportedOperationException("The requested operation is not supported " + uri.getPath().get());
			}
		} else {
			// Unable to handle the request args
			throw new IllegalArgumentException(PVA_DATA_SERVICE + " only supports request args of type NTURI");
		}
	}

}
