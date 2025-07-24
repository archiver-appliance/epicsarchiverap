package edu.stanford.slac.archiverappliance.plain;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.time.Instant;

public abstract class FileInfo {
    protected DBRTimeEvent firstEvent = null;
    protected DBRTimeEvent lastEvent = null;

    protected FileInfo() throws IOException {}

    public abstract String getPVName();

    public abstract short getDataYear();

    @Override
    public String toString() {
        return "FileInfo{" + "firstEvent=" + firstEvent + ", lastEvent=" + lastEvent + '}';
    }

    public abstract ArchDBRTypes getType();

    public abstract DBRTimeEvent getFirstEvent();

    public abstract DBRTimeEvent getLastEvent();

    public Instant getLastEventInstant() {
        return (getLastEvent() != null) ? getLastEvent().getEventTimeStamp() : Instant.EPOCH;
    }

    public Instant getFirstEventInstant() {
        return (getFirstEvent() != null) ? getFirstEvent().getEventTimeStamp() : Instant.EPOCH;
    }
}
