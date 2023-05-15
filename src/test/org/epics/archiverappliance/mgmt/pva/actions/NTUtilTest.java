package org.epics.archiverappliance.mgmt.pva.actions;


import org.epics.pva.data.PVAStringArray;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.StringArrayData;

public class NTUtilTest {

    /**
     * extract the string array values from a pvStringArray
     * @param pvStringArray
     * @return
     */
    @SuppressWarnings("unused")
    public static String[] extractStringArray(PVAStringArray pvStringArray) {
        return pvStringArray.get();
    }

}