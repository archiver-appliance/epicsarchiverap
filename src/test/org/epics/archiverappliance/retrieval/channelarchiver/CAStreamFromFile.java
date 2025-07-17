package org.epics.archiverappliance.retrieval.channelarchiver;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.File;
import java.io.FileInputStream;

/**
 * Process a results file and returns an event stream
 * Use to quickly check what CA sends you back.
 * @author mshankar
 *
 */
public class CAStreamFromFile {

    public static EventStream getEventStreamFromFile(String fileName) throws Exception {
        return new ArchiverValuesHandler(
                "DummyPVName", new FileInputStream(new File(fileName)), fileName, ArchDBRTypes.DBR_SCALAR_DOUBLE);
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        try (EventStream strm = getEventStreamFromFile(args[0])) {
            for (Event e : strm) {
                DBRTimeEvent evnt = (DBRTimeEvent) e;
                System.out.println(TimeUtils.convertToHumanReadableString(evnt.getEventTimeStamp()) + "\t"
                        + evnt.getSampleValue().toString() + "\t"
                        + evnt.getSeverity() + "\t"
                        + evnt.getStatus());
            }
        }
    }
}
