package org.epics.archiverappliance.data;

import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAStructure;

public class DBRAlarm {
    public int severity;
    public int status;

    public DBRAlarm(int severity, int status) {
            this.severity = severity;
            this.status = status;
        }

    public static DBRAlarm convertPVAlarm(PVAStructure alarmPVStructure) {
        return new DBRAlarm(
                alarmPVStructure == null ? 0 : ((PVAInt) alarmPVStructure.get("severity")).get(),
                alarmPVStructure == null ? 0 : ((PVAInt) alarmPVStructure.get("status")).get());
    }
}
