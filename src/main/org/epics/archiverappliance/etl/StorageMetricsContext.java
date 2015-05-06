package org.epics.archiverappliance.etl;

import java.io.IOException;
import java.nio.file.FileStore;

/**
 * Performance optimization for storage metrics methods.
 * Mostly used to lookup the filestore given the root folder - this can take a lot of time and we cache it for ETL.
 * @author mshankar
 *
 */
public interface StorageMetricsContext {
	public FileStore getFileStore(String rootFolder) throws IOException;
}
