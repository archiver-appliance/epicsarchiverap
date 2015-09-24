package org.epics.archiverappliance.etl.common;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.etl.StorageMetricsContext;
import org.epics.archiverappliance.utils.nio.ArchPaths;

public class DefaultStorageMetricsContext implements StorageMetricsContext {
	private static Logger logger = Logger.getLogger(DefaultStorageMetricsContext.class.getName());
	
	private HashMap<String, FileStore> storageMetricsFileStores = new HashMap<String, FileStore>();
	
	@Override
	public synchronized FileStore getFileStore(String rootFolder) throws IOException {
		FileStore fileStore = this.storageMetricsFileStores.get(rootFolder);
		if(fileStore == null) { 
			try(ArchPaths paths = new ArchPaths()) {
				Path rootF = paths.get(rootFolder);
				fileStore = Files.getFileStore(rootF);
				this.storageMetricsFileStores.put(rootFolder, fileStore);
				logger.debug("Adding filestore to ETLMetricsForLifetime cache for rootFolder " + rootFolder);
			}
		} else { 
			logger.debug("Filestore for rootFolder " + rootFolder + " is already in ETLMetricsForLifetime cache");
		}
		return fileStore;
	}
}
