package org.epics.archiverappliance.mgmt.pva.actions;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.server.RPCService;

/**
 * Wrapper around the {@link RPCService} for the Archiver
 * 
 * @author Kunal Shroff
 *
 */
public interface PvaAction {

	/**
	 * Name of the action
	 * @return the name of the service
	 */
	String getName();

	/**
	 * Handles an RPC request to the archiver.
	 *
	 * @param args Input arguments
	 * @param configService Current config service
	 * @throws PvaActionException which is then passed to the serverPV to return the error to the user.
	 */
	PVAStructure request(PVAStructure args, ConfigService configService) throws PvaActionException;
}
