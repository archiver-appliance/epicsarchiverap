/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;


import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.exception.ConfigException;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Some common setup for testing PB files
 * @author mshankar
 *
 */
public class PBCommonSetup {
	private static Logger logger = LogManager.getLogger(PBCommonSetup.class.getName());
	private File tempFolderForTests;
	private String testSpecificFolder;
    static ConfigServiceForTests configService;

    static {
        try {
            configService = new ConfigServiceForTests(1);
        } catch (ConfigException e) {
            throw new RuntimeException(e);
        }
    }


    public void setUpRootFolder() throws Exception {

		String rootFolder = System.getProperty("edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin.rootFolder");

		if(rootFolder != null)  {
			logger.info("Setting PB root folder to " + rootFolder);
			configService.setPBRootFolder(rootFolder);
		}

	}

	public void setUpRootFolder(PlainStoragePlugin pbplugin) throws Exception {
		setUpRootFolder();
		tempFolderForTests = new File(configService.getPBRootFolder());
		pbplugin.initialize("pb://localhost?name=UnitTest&rootFolder="+tempFolderForTests+"&partitionGranularity=PARTITION_YEAR", configService);
		pbplugin.setRootFolder(tempFolderForTests.getAbsolutePath());
		pbplugin.setName(tempFolderForTests.getAbsolutePath());
	}
	
	public void setUpRootFolder(PlainStoragePlugin pbplugin, String testSpecificFolder) throws Exception {
		setUpRootFolder();
		this.testSpecificFolder = testSpecificFolder;
		
		tempFolderForTests = new File(configService.getPBRootFolder() + File.separator + this.testSpecificFolder);
		if(tempFolderForTests.exists()) {
			FileUtils.deleteDirectory(tempFolderForTests);
		}
		tempFolderForTests.mkdirs();

		pbplugin.initialize("pb://localhost?name=UnitTest&rootFolder="+tempFolderForTests+"&partitionGranularity=PARTITION_YEAR", configService);

		
		pbplugin.setRootFolder(tempFolderForTests.getAbsolutePath());
		pbplugin.setName(tempFolderForTests.getAbsolutePath());
	}
	
	public void setUpRootFolder(PlainStoragePlugin pbplugin, String testSpecificFolder, PartitionGranularity partitionGranularity) throws Exception {
		setUpRootFolder();
		this.testSpecificFolder = testSpecificFolder;
		
		tempFolderForTests = new File(configService.getPBRootFolder() + File.separator + this.testSpecificFolder);
		if(tempFolderForTests.exists()) {
			FileUtils.deleteDirectory(tempFolderForTests);
		}
		tempFolderForTests.mkdirs();

		pbplugin.initialize("pb://localhost?name=UnitTest&rootFolder="+tempFolderForTests+"&partitionGranularity="+partitionGranularity.toString(), configService);

		
		pbplugin.setRootFolder(tempFolderForTests.getAbsolutePath());
		pbplugin.setPartitionGranularity(partitionGranularity);
		pbplugin.setName(partitionGranularity.toString());
	}


	public void deleteTestFolder() throws IOException {
		if(this.testSpecificFolder == null) {
			logger.warn("Not deleting the folder " + tempFolderForTests + " as the setup did not include a test specific folder..");
		} else {
			logger.info("Deleting folder " + tempFolderForTests.toString());
			FileUtils.deleteDirectory(tempFolderForTests);
		}
	}
	
	public File getRootFolder() {
		return tempFolderForTests;
	}


	public Set<String> listTestFolderContents() {
		HashSet<String> ret = new HashSet<String>();
		for(File f : FileUtils.listFiles(tempFolderForTests, new String[] { "*" }, true)) {
			ret.add(f.getAbsolutePath().toString());
		}
		return ret;
	}
}
