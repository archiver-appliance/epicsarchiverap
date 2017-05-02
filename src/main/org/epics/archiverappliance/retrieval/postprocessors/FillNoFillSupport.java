package org.epics.archiverappliance.retrieval.postprocessors;

/**
 * Add ability for PostProcessors to support optional behavior where we fill empty bins with values from the previous bins.
 * For historical reasons, by default, we inherit values from the previous bins
 * We override this behavior by calling doNotInheritValuesFromPrevioisBins
 * @author mshankar
 *
 */
public interface FillNoFillSupport {
	public void doNotInheritValuesFromPrevioisBins();
	/**
	 * For some post processors, we do fill empty bins but with zeroes instead.
	 * @return boolean True or False
	 */
	public boolean zeroOutEmptyBins();
}
