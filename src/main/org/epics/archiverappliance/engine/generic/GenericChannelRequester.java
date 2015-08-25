package org.epics.archiverappliance.engine.generic;

import java.util.concurrent.ConcurrentHashMap;

import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.generic.ScopedLogger;

public interface GenericChannelRequester
{
    ScopedLogger getLogger();

    void initSampleBuffer(double expectedFrequency);

    boolean addSample(DBRTimeEvent timeevent);

    void reportConnected(boolean connected);
    
    ConcurrentHashMap<String, String> getMetadataMap();
    
    class BulkStoreException extends Exception
    {
        public BulkStoreException(String msg) { super(msg); }
        public BulkStoreException(String msg, Throwable th) { super(msg, th); }
    }
    
    BulkStore startBulkStore(int buffer_size) throws BulkStoreException;
    
    public interface BulkStore
    {
        void addSample(DBRTimeEvent timeevent);
        void complete();
    }
    
    void logTrouble(String message);
}
