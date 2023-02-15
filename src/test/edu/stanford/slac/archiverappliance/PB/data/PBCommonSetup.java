/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;


import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ConfigServiceForTests;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * Some common setup for testing PB files
 * @author mshankar
 *
 */
public class PBCommonSetup {
	private static Logger logger = Logger.getLogger(PBCommonSetup.class.getName());
	private File tempFolderForTests;
	private String testSpecificFolder;
	ConfigServiceForTests configService;

	
	public void setUpRootFolder() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"), 1);
		String rootFolder = System.getProperty("edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.rootFolder");
		 
		if(rootFolder != null)  {
			logger.info("Setting PB root folder to " + rootFolder);
			configService.setPBRootFolder(rootFolder);
		}

	}

	public void setUpRootFolder(PlainPBStoragePlugin pbplugin) throws Exception {
		setUpRootFolder();
		tempFolderForTests = new File(configService.getPBRootFolder());
		pbplugin.initialize("pb://localhost?name=UnitTest&rootFolder="+tempFolderForTests+"&partitionGranularity=PARTITION_YEAR", configService);
		pbplugin.setRootFolder(tempFolderForTests.getAbsolutePath());
		pbplugin.setName(tempFolderForTests.getAbsolutePath());
	}
	
	public void setUpRootFolder(PlainPBStoragePlugin pbplugin, String testSpecificFolder) throws Exception {
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
	
	public void setUpRootFolder(PlainPBStoragePlugin pbplugin, String testSpecificFolder, PartitionGranularity partitionGranularity) throws Exception {
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

	public File getTempFolderForTests() {
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
