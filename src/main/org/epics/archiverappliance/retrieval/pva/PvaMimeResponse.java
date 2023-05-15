/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.pva;

import com.google.common.primitives.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.pva.data.*;
import org.epics.pva.data.nt.PVAAlarm;
import org.epics.pva.data.nt.PVAEnum;
import org.epics.pva.data.nt.PVAScalar;
import org.epics.pva.data.nt.PVATimeStamp;

import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

/**
 * The response is an array of PV elements, each PV has a meta and data section. The data section has timestamp in epoch
 * seconds and the value
 *
 * @author mshankar
 */
public class PvaMimeResponse implements MimeResponse {
    private static final Logger logger = LogManager.getLogger(PvaMimeResponse.class.getName());

    boolean firstPV = true;
    boolean closePV = false;

    private ArchDBRTypes streamDBRType;
    private PVAStructure resultStruct;
    private PVAny pvValueStruct;

    @SuppressWarnings("unchecked")
    @Override
    public void consumeEvent(Event e) throws Exception {

        DBRTimeEvent evnt = (DBRTimeEvent) e;

        logger.debug("The event stream type is: " + evnt.getDBRType());

        PVAScalar.Builder builder;

        switch (streamDBRType) {
            case DBR_SCALAR_FLOAT -> builder = PVAScalar.floatScalarBuilder(evnt.getSampleValue().getValue().floatValue());
            case DBR_SCALAR_DOUBLE -> builder = PVAScalar.doubleScalarBuilder(evnt.getSampleValue().getValue().doubleValue());
            case DBR_SCALAR_BYTE -> builder = PVAScalar.byteScalarBuilder(false, evnt.getSampleValue().getValue().byteValue());
            case DBR_SCALAR_SHORT -> builder = PVAScalar.shortScalarBuilder(false, evnt.getSampleValue().getValue().shortValue());
            case DBR_SCALAR_INT -> builder = PVAScalar.intScalarBuilder(evnt.getSampleValue().getValue().intValue());
            case DBR_SCALAR_STRING -> builder = PVAScalar.stringScalarBuilder(evnt.getSampleValue().toString());
            case DBR_SCALAR_ENUM -> {
                PVAEnum pvaEnum = new PVAEnum("value", evnt.getSampleValue().getValue().intValue(), new String[]{});
                builder = (new PVAScalar.Builder<PVAEnum>()).value(pvaEnum);
            }
            case DBR_WAVEFORM_FLOAT -> {
                List<Float> values = evnt.getSampleValue().getValues();
                builder = PVAScalar.floatArrayScalarBuilder(Floats.toArray(values));
            }
            case DBR_WAVEFORM_DOUBLE -> {
                List<Double> values = evnt.getSampleValue().getValues();
                builder = PVAScalar.doubleArrayScalarBuilder(Doubles.toArray(values));
            }
            case DBR_WAVEFORM_SHORT -> {
                List<Short> values = evnt.getSampleValue().getValues();
                builder = PVAScalar.shortArrayScalarBuilder(false, Shorts.toArray(values));
            }
            case DBR_WAVEFORM_BYTE -> {
                List<Byte> values = evnt.getSampleValue().getValues();
                builder = PVAScalar.byteArrayScalarBuilder(false, Bytes.toArray(values));
            }
            case DBR_WAVEFORM_INT -> {
                List<Integer> values = evnt.getSampleValue().getValues();
                builder = PVAScalar.intArrayScalarBuilder(false, Ints.toArray(values));
            }
            case DBR_WAVEFORM_STRING -> {
                List<String> values = evnt.getSampleValue().getValues();
                builder = PVAScalar.stringArrayScalarBuilder(values.toArray(new String[values.size()]));
            }
            case DBR_WAVEFORM_ENUM, DBR_V4_GENERIC_BYTES -> throw new UnsupportedOperationException("Unsupported DBR type " + streamDBRType);

            default -> throw new UnsupportedOperationException("Unknown DBR type " + streamDBRType);
        }

        PVAStructure s = builder.name("sample")
                .alarm(alarmInfo(evnt))
                .timeStamp(timeInfo(evnt))
                .build();
        if (this.pvValueStruct.get() == null) {
            this.pvValueStruct.setValue(new PVAStructureArray("result", s.cloneType("element_type")));
        }
        appendStructureArray(this.pvValueStruct.get(), s);
    }

    private PVAAlarm alarmInfo(DBRTimeEvent evnt) {
        // convert the alarm info
        return new PVAAlarm(new PVAInt("severity", evnt.getSeverity()),
                new PVAInt("status", evnt.getStatus()),
                new PVAString("message"));
    }

    private PVATimeStamp timeInfo(DBRTimeEvent evnt) {
        // convert the time info
        return new PVATimeStamp(TimeUtils.convertTimestampToInstant(evnt.getEventTimeStamp()));
    }

    @Override
    public void setOutputStream(OutputStream os) {
        // TODO
    }

    @Override
	public void close() {
	}

	@Override
	public void processingPV(BasicContext retrievalContext, String pv, Timestamp start, Timestamp end, EventStreamDesc streamDesc) {
		if (firstPV) {
			firstPV = false;
		}
		RemotableEventStreamDesc remoteDesc = (RemotableEventStreamDesc) streamDesc;
		this.streamDBRType = remoteDesc.getArchDBRType();

		// Process the stream description to create the appropriate label/s
		PVAStringArray labels = resultStruct.get("labels");
        appendStringArray(labels, pv);

		PVAAnyArray value = resultStruct.get("value");
		this.pvValueStruct = new PVAny("any");
        extendPVAnyArray(value, this.pvValueStruct);

		closePV = true;
	}

    private void appendStringArray(PVAStringArray pvaStringArray, String newString) {
        String[] elements = pvaStringArray.get();
        String[] newElements = ArrayUtils.add(elements, newString);
        pvaStringArray.set(newElements);
    }

    private void appendStructureArray(PVAStructureArray pvaStructureArray, PVAStructure newStructure) throws ElementTypeException {
        PVAStructure[] elements = pvaStructureArray.get();
        PVAStructure[] newElements = ArrayUtils.add(elements, newStructure);
        pvaStructureArray.set(newElements);
    }

    private void extendPVAnyArray(PVAAnyArray pvaAnyArray, PVAny newValue) {
        PVAny[] elements = pvaAnyArray.get();
        PVAny[] newElements = ArrayUtils.add(elements, newValue);
        pvaAnyArray.set(newElements);
    }

	public void swicthingToStream(EventStream strm) {
		// Not much to do here for now.
	}

	@Override
	public HashMap<String, String> getExtraHeaders() {
		HashMap<String, String> ret = new HashMap<String, String>();
		// Allow applications served from other URL's to access the JSON data from this
		// server.
		ret.put(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return ret;
	}


    public void setOutputStruct(PVAStructure resultStruct) {
        this.resultStruct = resultStruct;
    }

}
