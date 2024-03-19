package org.epics.archiverappliance.etl;

import java.io.IOException;

/**
 * Interface providing details for the storage metrics pages...
 * @author mshankar
 *
 */
public interface StorageMetrics {
	/**
	 * The name is also used to identify the storage in the storage report.
	 * This method is shared with the StoragePlugin interface.
	 * @return name The name of stroage
	 */
	public String getName();
	
	/**
	 * Gets the total space left on this device.
	 * @param storageMetricsContext  StorageMetricsContext
	 * @throws IOException &emsp;
	 * @return getTotalSpac &emsp;
	 */
	public long getTotalSpace(StorageMetricsContext storageMetricsContext) throws IOException;
	/**
	 * Gets the space available to this VM on this device 
	 * @param storageMetricsContext  StorageMetricsContext
	 * @throws IOException &emsp;
	 * @return getUsableSpace &emsp;
	 */
	public long getUsableSpace(StorageMetricsContext storageMetricsContext) throws IOException;
	/**
	 * Gets an estimate of the space consumed by this PV on this device.
	 * @param pvName The name of PV.
	 * @throws IOException &emsp;
	 * @return spaceConsumedByPV &emsp;
	 */
	public long spaceConsumedByPV(String pvName) throws IOException;
}
