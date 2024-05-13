/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB.utils;

import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.json.simple.JSONObject;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author mshankar
 *	Prints a JSON version of the data in a PB file.
 */
public class PB2JSON {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.err.println("Usage: java edu.stanford.slac.archiverappliance.PlainPB.utils.PB2JSON <PBFiles>");
            return;
        }

        System.out.println('[');
        boolean firstline = true;
        for (String fileName : args) {
            Path path = Paths.get(fileName);
            PBFileInfo info = new PBFileInfo(path);
            try (FileBackedPBEventStream strm = new FileBackedPBEventStream(info.getPVName(), path, info.getType())) {
                for (Event ev : strm) {
                    DBRTimeEvent tev = (DBRTimeEvent) ev;
                    JSONObject obj = new JSONObject();
                    obj.put("timeStamp", TimeUtils.convertToISO8601String(tev.getEventTimeStamp()));
                    obj.put("value", tev.getSampleValue().toString());
                    obj.put("status", tev.getStatus());
                    obj.put("severity", tev.getSeverity());
                    if (tev.hasFieldValues()) {
                        JSONObject fieldValues = new JSONObject();
                        fieldValues.putAll(tev.getFields());
                        obj.put("fields", fieldValues);
                    }
                    if (firstline) {
                        firstline = false;
                    } else {
                        System.out.println(",");
                    }
                    System.out.print(obj);
                }
            }
        }
        System.out.println();
        System.out.println(']');
    }
}
