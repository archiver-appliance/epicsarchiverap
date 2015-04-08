package org.epics.archiverappliance.engine.pv;

/**
 * PV's that can control the archiving of other PV's will need to implement this interface. 
 * @author luofeng
 * Refactored by mshankar
 *
 */
public interface ControllingPV {
	
	/**
	 * @return the status of all pvs controlled by this pv
	 */
	public boolean isEnableAllPV();

	/**
	 * add pv controlled by this pv
	 * @param pvName  the name of pv controlled by this pv
	 */
	void addControledPV(String pvName);

    /**
     * Should be the same as the method in PV
     * @throws Exception
     */
    public void start() throws Exception;
    /**
     * Should be the same as the method in PV
     * @throws Exception
     */
    public void stop();

}
