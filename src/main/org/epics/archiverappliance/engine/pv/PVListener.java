
package org.epics.archiverappliance.engine.pv;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

/** A listener for PV updates. 
 */
public interface PVListener
{
    /**
     * We issued a connection request to the underlying PV. 
     * @param pv PV
     */
    public void pvConnectionRequestMade(PV pv);


    /**
     * Notification of a connection being successfully made.
     * @param pv PV
     */
    public void pvConnected(PV pv);
    
    /** Notification of a PV disconnect.
     *  <p>
     *  This event may be the immediate result of a
     *  control system library callback,
     *  i.e. it may arrive in a non-UI thread.
     *  
     *  @param pv The disconnected PV
     */
    public void pvDisconnected(PV pv);
    
    
    /** Notification of a new value.
     *  <p>
     *  This event may be the immediate result of a
     *  control system library callback,
     *  i.e. it may arrive in a non-UI thread.
     *  
     *  @param pv The PV which has a new value
     */
    public void pvValueUpdate(PV pv, DBRTimeEvent ev);
    
    /**
     * Notification of a sample being dropped because of a type change
     * Use to maintain counters
     * @param pv PV
     * @param newDBRType The new DBR type from the control system
     */
    public void sampleDroppedTypeChange(PV pv, ArchDBRTypes newDBRType);
    
    
}
