package edu.stanford.slac.archiverappliance.plain.utils;

import edu.stanford.slac.archiverappliance.PB.data.PBTypeSystem;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;

/**
 * Small utilty to read a .pb file as the specfied DBR type.
 * This skips the PBFileInfo; so it should work better against corrupted files.
 * @author mshankar
 *
 */
public class ReadAsDBRType {

    public static void main(String[] args)
            throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException,
                    InvocationTargetException {
        if (args.length < 3) {
            System.err.println(
                    "Usage: java edu.stanford.slac.archiverappliance.PlainPB.utils.ReadAsDBRType <DBRType> <Year> <File>");
            System.err.println(
                    "DBRType is something that can be passed into ArchDBRTypes.valueOf. For example, DBR_SCALAR_DOUBLE ");
            return;
        }

        ArchDBRTypes archDBRType = ArchDBRTypes.valueOf(args[0]);
        short year = Short.parseShort(args[1]);
        String path = args[2];

        ByteArray bar = new ByteArray(LineByteStream.MAX_LINE_SIZE);
        PBTypeSystem typeSys = new PBTypeSystem();
        Constructor<? extends DBRTimeEvent> cons = typeSys.getUnmarshallingFromByteArrayConstructor(archDBRType);
        try (LineByteStream lis = new LineByteStream(Paths.get(path))) {
            lis.readLine(bar);
            while (lis.readLine(bar) != null && !bar.isEmpty()) {
                DBRTimeEvent event = cons.newInstance(year, bar);
                System.out.println(TimeUtils.convertToHumanReadableString(event.getEpochSeconds()) + " ==> "
                        + event.getSampleValue().toString());
            }
        }
    }
}
