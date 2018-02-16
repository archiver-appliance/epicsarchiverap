package org.epics.archiverappliance.mgmt.pva.actions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.StringArrayData;

/**
 * A helper class to easily extract value from nt structures
 * 
 * @author Kunal Shroff
 *
 */
public class NTUtil {

	/**
	 * extract the string array values from a pvStringArray
	 * @param pvStringArray
	 * @return
	 */
	@SuppressWarnings("unused")
	public static String[] extractStringArray(PVStringArray pvStringArray) {
		StringArrayData data = new StringArrayData();
		if(pvStringArray != null) {
			int len = pvStringArray.get(0, pvStringArray.getLength(), data);
		}
		return data.data;
	}
	

	/**
	 * extract a list from a pvStringArray
	 * @param pvStringArray
	 * @return return a list representation of the array data or an empty list
	 */
	@SuppressWarnings("unused")
	public static List<String> extractStringList(PVStringArray pvStringArray) {
		StringArrayData data = new StringArrayData();
		if (pvStringArray != null) {
			int len = pvStringArray.get(0, pvStringArray.getLength(), data);
		}
		if (data.data != null) {
			return Arrays.asList(data.data);
		} else {
			return Collections.emptyList();
		}
	}


}
