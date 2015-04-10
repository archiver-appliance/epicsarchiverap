
package org.epics.archiverappliance.engine.pv;

/** A listener for PV updates. 
 */
public interface PVListener
{
    /** Notification of a new value.
     *  <p>
     *  This event may be the immediate result of a
     *  control system library callback,
     *  i.e. it may arrive in a non-UI thread.
     *  
     *  @param pv The PV which has a new value
     */
    public void pvValueUpdate(PV pv);
    
    /** Notification of a PV disconnect.
     *  <p>
     *  This event may be the immediate result of a
     *  control system library callback,
     *  i.e. it may arrive in a non-UI thread.
     *  
     *  @param pv The disconnected PV
     */
    public void pvDisconnected(PV pv);
    
    public void pvConnected(PV pv);
    
    /**
     * We issues a connection request to the underlying PV. 
     * @param pv
     */
    public void pvConnectionRequestMade(PV pv);
}
