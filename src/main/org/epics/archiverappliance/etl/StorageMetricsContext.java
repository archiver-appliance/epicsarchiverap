package org.epics.archiverappliance.etl;

import java.io.IOException;

/**
 * Performance optimization for storage metrics methods.
 * Mostly used to lookup the filestore given the root folder - this can take a lot of time and we cache it for ETL.
 * @author mshankar
 *
 */
public interface StorageMetricsContext {
    public long getUsableSpaceFromCache(String rootFolder) throws IOException;

    public long getTotalSpaceFromCache(String rootFolder) throws IOException;
}
