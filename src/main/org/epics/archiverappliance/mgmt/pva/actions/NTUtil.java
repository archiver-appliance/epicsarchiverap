package org.epics.archiverappliance.mgmt.pva.actions;

import org.epics.pva.data.PVAStringArray;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * A helper class to easily extract value from nt structures
 * 
 * @author Kunal Shroff
 *
 */
public class NTUtil {
	private NTUtil() {}

	/**
	 * Extract the string array values from a pvStringArray
	 * @param pvStringArray
	 * @return
	 */
	@SuppressWarnings("unused")
	public static String[] extractStringArray(PVAStringArray pvStringArray) {
		if (pvStringArray != null)
			return pvStringArray.get();
		return new String[0];
	}
	

	/**
	 * Extract a list from a pvStringArray
	 * @param pvStringArray
	 * @return return a list representation of the array data or an empty list
	 */
	@SuppressWarnings("unused")
	public static List<String> extractStringList(PVAStringArray pvStringArray) {
		if (pvStringArray.get() != null) {
			return Arrays.asList(pvStringArray.get());
		} else {
			return Collections.emptyList();
		}
	}


}
