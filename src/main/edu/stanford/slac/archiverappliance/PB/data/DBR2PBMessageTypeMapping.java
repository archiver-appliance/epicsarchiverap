/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.util.HashMap;

/**
 * Maps ArchDBRTypes to PB classes.
 * This functions much like a methodtable.
 *
 * @author mshankar
 */
public class DBR2PBMessageTypeMapping {
    private static final HashMap<ArchDBRTypes, DBR2PBMessageTypeMapping> typemap = new HashMap<>();

    static {
        typemap.put(ArchDBRTypes.DBR_SCALAR_STRING, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarString.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_SHORT, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarShort.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_FLOAT, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarFloat.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_ENUM, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarEnum.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_BYTE, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarByte.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_INT, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarInt.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_DOUBLE, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarDouble.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_STRING, new DBR2PBMessageTypeMapping(EPICSEvent.VectorString.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_SHORT, new DBR2PBMessageTypeMapping(EPICSEvent.VectorShort.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_FLOAT, new DBR2PBMessageTypeMapping(EPICSEvent.VectorFloat.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_ENUM, new DBR2PBMessageTypeMapping(EPICSEvent.VectorEnum.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_BYTE, new DBR2PBMessageTypeMapping(EPICSEvent.VectorChar.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_INT, new DBR2PBMessageTypeMapping(EPICSEvent.VectorInt.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_DOUBLE, new DBR2PBMessageTypeMapping(EPICSEvent.VectorDouble.class));
        typemap.put(ArchDBRTypes.DBR_V4_GENERIC_BYTES, new DBR2PBMessageTypeMapping(EPICSEvent.V4GenericBytes.class));

        for (ArchDBRTypes t : ArchDBRTypes.values()) {
            if (typemap.get(t) == null) {
                throw new RuntimeException("We have a type in DBR type that does have an equivalent PB type");
            }
        }
    }

    Class<? extends Message> pbclass;

    private DBR2PBMessageTypeMapping(Class<? extends Message> pblass) {
        this.pbclass = pblass;
    }

    public static Class<? extends Message> getMessageClass(ArchDBRTypes type) {
        return typemap.get(type).pbclass;
    }
}
