package edu.stanford.slac.archiverappliance.plain.utils;

import edu.stanford.slac.archiverappliance.plain.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.plain.PBFileInfo;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Seek to a position in the file and then print some info about each event.
 * Very useful for debugging seek/search issues.
 * @author mshankar
 *
 */
public class SeekandPrintTimes {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                    "Usage: java edu.stanford.slac.archiverappliance.PlainPB.utils.SeekandPrintTimes <File> <Position (long)>");
            return;
        }
        String fileName = args[0];
        long position = Long.parseLong(args[1]);
        Path path = Paths.get(fileName);
        System.out.println("Printing times for file " + path.toAbsolutePath().toString());
        PBFileInfo info = new PBFileInfo(path);
        try (FileBackedPBEventStream strm =
                new FileBackedPBEventStream(info.getPVName(), path, info.getType(), position, Files.size(path))) {
            for (Event ev : strm) {
                System.out.println(TimeUtils.convertToISO8601String(((DBRTimeEvent) ev).getEventTimeStamp())
                        + "\t" + TimeUtils.convertToHumanReadableString((((DBRTimeEvent) ev).getEventTimeStamp()))
                        + "\t" + ev.getSampleValue().toString());
            }
        }
    }
}
