package org.epics.archiverappliance.mgmt.pva.actions;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvdata.pv.PVStructure;

/**
 * 
 * @author Kunal Shroff
 *
 */
public interface PvaAction {

	/**
	 * Name of the action
	 * @return the name of the service
	 */
	public String getName();

	/**
	 * 
	 * @param args
	 * @param callback
	 * @param configService
	 */
	public void request(PVStructure args, RPCResponseCallback callback, ConfigService configService);
}
