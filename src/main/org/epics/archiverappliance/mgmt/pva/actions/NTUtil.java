package org.epics.archiverappliance.mgmt.pva.actions;

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

}
