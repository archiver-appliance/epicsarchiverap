package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.etl.bpl.reports.ApplianceMetricsDetails;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;

/**
 * An ETL benchmark. Generate some data for PVs and then time the movement to the next store. 
 * Use the following exports to control ETL src and ETL dest.
 * <code><pre>
 * export ARCHAPPL_SHORT_TERM_FOLDER=/dev/shm/test
 * export ARCHAPPL_MEDIUM_TERM_FOLDER=/scratch/LargeDisk/ArchiverStore
 * </pre></code>
 * 
 * @author mshankar
 *
 */
@Tag("slow")
public class ETLTimeTest {
	private static Logger logger = LogManager.getLogger(ETLTimeTest.class.getName());
	String shortTermFolderName=ConfigServiceForTests.getDefaultShortTermFolder()+"/shortTerm";
	String mediumTermFolderName=ConfigServiceForTests.getDefaultPBTestFolder()+"/mediumTerm";

	PlainPBStoragePlugin storageplugin1;
	PlainPBStoragePlugin storageplugin2;
	short currentYear = TimeUtils.getCurrentYear();
	ArchDBRTypes type = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	private  ConfigServiceForTests configService;

	@BeforeEach
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		if(new File(shortTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(shortTermFolderName));
		}
		if(new File(mediumTermFolderName).exists()) {
			FileUtils.deleteDirectory(new File(mediumTermFolderName));
		}
		
		new File(shortTermFolderName).mkdirs();
		new File(mediumTermFolderName).mkdirs();
		

		storageplugin1 = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + shortTermFolderName + "/&partitionGranularity=PARTITION_HOUR", configService);
		storageplugin2 = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + mediumTermFolderName + "/&partitionGranularity=PARTITION_YEAR", configService);
	}

	@AfterEach
	public void tearDown() throws Exception {
		configService.shutdownNow();
	}
	@Test
	public void testTime() throws AlreadyRegisteredException, IOException, InterruptedException{
		ArrayList<String> pvs=new ArrayList<String>();
		for(int i=0;i<200000;i++) {
			int tableName=i/200;
			String pvName = "ArchUnitTest" +tableName+ ":ETLTimeTest"+i;
			PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			String[] dataStores = new String[] { storageplugin1.getURLRepresentation(), storageplugin2.getURLRepresentation()};
			typeInfo.setDataStores(dataStores);
			configService.updateTypeInfoForPV(pvName, typeInfo);
			configService.registerPVToAppliance(pvName, configService.getMyApplianceInfo());
			pvs.add(pvName);
		}
		configService.getETLLookup().manualControlForUnitTests();

		logger.info("Generating data for " + pvs.size() + " pvs");
		for(int m=0;m<pvs.size();m++){
			String pvnameTemp=pvs.get(m);                  
			try(BasicContext context = new BasicContext()) {
				// Generate subset of data for one hour. We vary the amount of data we generate to mimic LCLS distribution... 
				int totalNum=1;
				if(m < 500) { 
					totalNum = 10*60*60;
				} else if(m < 5000) { 
					totalNum = 60*60;
				}
				
				ArrayListEventStream testData = new ArrayListEventStream(totalNum, new RemotableEventStreamDesc(type, pvnameTemp, currentYear));
				for(int s = 0; s < totalNum; s++) {
					testData.add(new SimulationEvent(s*10, currentYear, type, new ScalarValue<Double>((double) s*10)));
				}
				storageplugin1.appendData(context, pvnameTemp, testData);
			}
		}
		logger.info("Done generating data for " + pvs.size() + " pvs");
		CountFiles sizeVisitor = new CountFiles();
		Files.walkFileTree(Paths.get(shortTermFolderName), sizeVisitor);

		long time1=System.currentTimeMillis();
		YearSecondTimestamp yts = new YearSecondTimestamp((short) (currentYear+1), 6*60*24*10+100, 0);
        Instant etlTime = TimeUtils.convertFromYearSecondTimestamp(yts);
		logger.info("Running ETL as if it was " + TimeUtils.convertToHumanReadableString(etlTime));
		ETLExecutor.runETLs(configService, etlTime);
		
		for(int i = 0; i < 5; i++) {
			logger.info("Calling sync " + i);
			ProcessBuilder pBuilder = new ProcessBuilder("sync");
			pBuilder.inheritIO();
			int exitValue = pBuilder.start().waitFor();
			Assertions.assertEquals(0, exitValue, "Nonzero exit from sync " + exitValue);
		}
		
	
		long time2=System.currentTimeMillis();
		DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
		
		double hundredKPVEstimateTimeSecs = ((time2-time1)/1000.0)*(200000.0/pvs.size());
		double dataSizeInGBPerHour = sizeVisitor.totalSize/(1024.0*1024.0*1024.0);
		double fudgeFactor = 5.0; // Inner sectors; read/write; varying event rates etc. 
		logger.info("Time for moving " 
				+ pvs.size() + " pvs" 
				+ " with data " + twoSignificantDigits.format(dataSizeInGBPerHour) + "(GB/Hr) and " +  twoSignificantDigits.format(dataSizeInGBPerHour*24) + "(GB/day)"
				+ " from " + shortTermFolderName 
				+ " to " + mediumTermFolderName 
				+ " in " + (time2-time1) + "(ms)." 
				+ " Estimated time for 200K PVs is " + twoSignificantDigits.format(hundredKPVEstimateTimeSecs) + "(s)"
				+ " Estimated capacity consumed = " + twoSignificantDigits.format(hundredKPVEstimateTimeSecs*100*fudgeFactor/3600.0));
		
		// No pb files should exist in short term folder...
		CountFiles postETLSrcVisitor = new CountFiles();
		Files.walkFileTree(Paths.get(shortTermFolderName), postETLSrcVisitor);
		CountFiles postETLDestVisitor = new CountFiles();
		Files.walkFileTree(Paths.get(mediumTermFolderName), postETLDestVisitor);
		Assertions.assertEquals(0, postETLSrcVisitor.filesPresent, "We have some files that have not moved " + postETLSrcVisitor.filesPresent);
		Assertions.assertEquals(postETLDestVisitor.filesPresent, pvs.size(), "Dest file count " + postETLDestVisitor.filesPresent + " is not the same as PV count " + pvs.size());
		
		if(postETLSrcVisitor.filesPresent == 0) { 
			FileUtils.deleteDirectory(new File(shortTermFolderName));
			FileUtils.deleteDirectory(new File(mediumTermFolderName));
		}
		
		logger.info(ApplianceMetricsDetails.getETLMetricsDetails(configService));
	}
}


class CountFiles implements FileVisitor<Path> {
	public long filesPresent = 0;
	public long totalSize = 0;
	@Override
	public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
		filesPresent++;
		totalSize += Files.size(file);
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
		return FileVisitResult.CONTINUE;
	}
}
