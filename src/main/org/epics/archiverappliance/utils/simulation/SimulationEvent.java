/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.simulation;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.HashMap;

/**
 * An event typically used in the unit tests.
 * @author mshankar
 *
 */
public class SimulationEvent implements DBRTimeEvent {
    private static final Logger logger = LogManager.getLogger(SimulationEvent.class.getName());
    private final int secondsIntoYear;
    private final ArchDBRTypes type;
    private final DBR2PBTypeMapping mapping;
    short yearofdata;
    SampleValue sampleValue;
    private int nanos = 0;

    public SimulationEvent(Instant instant, ArchDBRTypes type, SimulationValueGenerator valueGenerator) {
        this.secondsIntoYear = TimeUtils.getSecondsIntoYear(instant);
        this.yearofdata = TimeUtils.getYear(instant);
        this.type = type;
        this.mapping = DBR2PBTypeMapping.getPBClassFor(this.type);
        this.sampleValue = valueGenerator.getSampleValue(this.type, secondsIntoYear);
    }

    public SimulationEvent(int secondsIntoYear, short yearofdata, ArchDBRTypes type, SampleValue sampleValue) {
        this.secondsIntoYear = secondsIntoYear;
        this.yearofdata = yearofdata;
        this.type = type;
        this.mapping = DBR2PBTypeMapping.getPBClassFor(this.type);
        this.sampleValue = sampleValue;
    }

    public SimulationEvent(Instant instant, ArchDBRTypes type, SampleValue sampleValue) {
        this.secondsIntoYear = TimeUtils.getSecondsIntoYear(instant);
        this.yearofdata = TimeUtils.getYear(instant);
        this.type = type;
        this.mapping = DBR2PBTypeMapping.getPBClassFor(this.type);
        this.sampleValue = sampleValue;
    }

    public SimulationEvent(YearSecondTimestamp yts, ArchDBRTypes type, SampleValue sampleValue) {
        this(yts.getSecondsintoyear(), yts.getYear(), type, sampleValue);
        this.nanos = yts.getNano();
    }

    public SimulationEvent(SimulationEvent src) {
        this.secondsIntoYear = src.secondsIntoYear;
        this.nanos = src.nanos;
        this.yearofdata = src.yearofdata;
        this.type = src.type;
        this.mapping = src.mapping;
        this.sampleValue = src.sampleValue;
    }

    @Override
    public Event makeClone() {
        return new SimulationEvent(this);
    }

    @Override
    public SampleValue getSampleValue() {
        return sampleValue;
    }

    @Override
    public Instant getEventTimeStamp() {
        return TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(yearofdata, secondsIntoYear, nanos));
    }

    @Override
    public int getStatus() {
        return 0;
    }

    @Override
    public void setStatus(int status) {
        throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
    }

    @Override
    public int getSeverity() {
        return 0;
    }

    @Override
    public void setSeverity(int severity) {
        throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
    }

    @Override
    public int getRepeatCount() {
        return 0;
    }

    @Override
    public void setRepeatCount(int repeatCount) {}

    @Override
    public long getEpochSeconds() {
        return TimeUtils.convertToEpochSeconds(
                TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(yearofdata, secondsIntoYear, 0)));
    }

    @Override
    public ByteArray getRawForm() {

        DBRTimeEvent ev = getDbrTimeEvent();
        return ev.getRawForm();
    }

    private DBRTimeEvent getDbrTimeEvent() {
        // We do have a mechanism to avoid inclusion of DBR2PBTypeMapping by going thru the config service.
        // But as the simulation events are mainly for unit tests, we stick to raw PB..
        // In the future, if you need to break the dependency on DBR2PBTypeMapping, please do so....
        DBRTimeEvent ev = null;
        try {
            ev = mapping.getSerializingConstructor().newInstance(this);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {

            logger.error("Exception creating event object", e);
            throw new RuntimeException("Unable to serialize a simulation event stream");
        }
        return ev;
    }

    @Override
    public boolean hasFieldValues() {
        return false;
    }

    @Override
    public boolean isActualChange() {
        return false;
    }

    @Override
    public HashMap<String, String> getFields() {
        throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
    }

    @Override
    public String getFieldValue(String fieldName) {
        throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
    }

    @Override
    public void addFieldValue(String fieldName, String fieldValue) {
        throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
    }

    @Override
    public void markAsActualChange() {
        throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
    }

    @Override
    public void setFieldValues(HashMap<String, String> fieldValues, boolean markAsActualChange) {
        throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
    }

    @Override
    public ArchDBRTypes getDBRType() {
        return this.type;
    }

    public int getNanos() {
        return nanos;
    }

    public void setNanos(int nanos) {
        this.nanos = nanos;
    }
}
