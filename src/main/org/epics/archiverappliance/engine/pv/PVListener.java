
package org.epics.archiverappliance.engine.pv;

/** A listener for PV updates. 
 */
public interface PVListener
{
    /**
     * We issued a connection request to the underlying PV. 
     * @param pv
     */
    public void pvConnectionRequestMade(PV pv);


    /**
     * Notification of a connection being successfully made.
     * @param pv
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
    public void pvValueUpdate(PV pv);
    
    public enum DroppedReason { 
    	TYPE_CHANGE  
    };
    /**
     * Notification of a sample being dropped for some reason from within the PV.
     * Use to maintain counters
     * @param pv
     * @param reason
     */
    public void pvDroppedSample(PV pv, DroppedReason reason);
    
    
}
