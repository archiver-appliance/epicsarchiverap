package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadType;
import edu.stanford.slac.archiverappliance.PB.data.BoundaryConditionsSimulationValueGenerator;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.ScalarValue;

import java.io.File;
import java.io.FileOutputStream;
import java.time.Instant;
import java.util.Random;

/**
 * Generates sample data used for testing the pb raw client interface...
 * @author mshankar
 *
 */
public class GenerateSampleClientData {
	private static Logger logger = LogManager.getLogger(GenerateSampleClientData.class.getName());
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if(args == null || args.length != 1) { 
			System.err.println("Usage: java org.epics.archiverappliance.retrieval.client.GenerateSampleClientData <Folder>");
			return;
		}
		
		File destFolder = new File(args[0]);
		if(!destFolder.isDirectory()) { 
			System.err.println(destFolder.getPath() + " is not a directory.");
			return;
		}
		
		generateOneFileWithWellKnownPoints(destFolder);
		generateMultipleChunksInSameYear(destFolder);
		generateMultipleChunksOfRandomSizeInSameYear(destFolder);
		generateMultipleChunksInMultipleYears(destFolder);
		generateFilesForDBRTypes(destFolder);
		generateOneDaysWorthOfDBRDoubleData(destFolder);
	}
	
	
	
	/**
	 * We generate a file with one data point per day for 2012. All data points are for 09:43:37 UTC. 
	 * 
	 * @param destFolder
	 */
	private static void generateOneFileWithWellKnownPoints(File destFolder) throws Exception {
		String destFileName = "singleFileWithWellKnownPoints";
		File destFile = new File(destFolder, destFileName);
		logger.info("Generating one file will well known points into " + destFile.getPath());
		try(FileOutputStream fos = new FileOutputStream(destFile)) { 
			writeHeader(destFileName, ArchDBRTypes.DBR_SCALAR_DOUBLE, (short) 2012, fos);
			Instant ts = TimeUtils.convertFromISO8601String("2012-01-01T09:43:37.000Z");
			for(int day = 0; day < 366; day++) { 
				POJOEvent event = new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>((double)day), 0, 0);
				ByteArray val = event.getRawForm();
				fos.write(val.data, val.off, val.len);
				fos.write(LineEscaper.NEWLINE_CHAR);
				ts = Instant.ofEpochMilli(ts.toEpochMilli() + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 1000);
			}
		}
	}
	
	
	private static void writeHeader(String pvName, ArchDBRTypes dbrType, short year, FileOutputStream fos) throws Exception { 
		byte[] headerBytes = LineEscaper.escapeNewLines(PayloadInfo.newBuilder()
				.setPvname(pvName)
				.setType(dbrType.getPBPayloadType())
				.setYear(year)
				.build().toByteArray());
		fos.write(headerBytes);
		fos.write(LineEscaper.NEWLINE_CHAR);
	}
	
	
	/**
	 * We generate a file with one data point per day for 2012; however, there is a header after each datapoint. All data points are for 09:43:37 UTC. 
	 * 
	 * @param destFolder
	 */
	private static void generateMultipleChunksInSameYear(File destFolder) throws Exception {
		String destFileName = "multipleChunksInSameYear";
		File destFile = new File(destFolder, destFileName);
		logger.info("Generating multiple chunks in same year into " + destFile.getPath());
		try(FileOutputStream fos = new FileOutputStream(destFile)) { 
			writeHeader(destFileName, ArchDBRTypes.DBR_SCALAR_DOUBLE, (short) 2012, fos);
			Instant ts = TimeUtils.convertFromISO8601String("2012-01-01T09:43:37.000Z");
			for(int day = 0; day < 366; day++) { 
				POJOEvent event = new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>((double)day), 0, 0);
				ByteArray val = event.getRawForm();
				fos.write(val.data, val.off, val.len);
				fos.write(LineEscaper.NEWLINE_CHAR);
				// Now insert an empty line followed by a header.
				fos.write(LineEscaper.NEWLINE_CHAR);
				writeHeader(destFileName, ArchDBRTypes.DBR_SCALAR_DOUBLE, (short) 2012, fos);
				ts = Instant.ofEpochMilli(ts.toEpochMilli() + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 1000);
			}
		}
	}
	
	/**
	 * We generate a file with one data point per day for 2012; this is broken down into chunks of random sizes All data points are for 09:43:37 UTC. 
	 * 
	 * @param destFolder
	 */
	private static void generateMultipleChunksOfRandomSizeInSameYear(File destFolder) throws Exception {
		String destFileName = "multipleChunksOfRandomSizeInSameYear";
		File destFile = new File(destFolder, destFileName);
		logger.info("Generating multiple chunks of random size in same year into " + destFile.getPath());
		try(FileOutputStream fos = new FileOutputStream(destFile)) {
			Instant ts = TimeUtils.convertFromISO8601String("2012-01-01T09:43:37.000Z");
			Random rand = new Random();
			for(int day = 0; day < 366; ) { 
				int chunkSize = rand.nextInt(5);
				writeHeader(destFileName, ArchDBRTypes.DBR_SCALAR_DOUBLE, (short) 2012, fos);
				for(int c = 0; c < chunkSize; c++) { 
					if(day >= 366) continue;
					POJOEvent event = new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>((double)day++), 0, 0);
					ByteArray val = event.getRawForm();
					fos.write(val.data, val.off, val.len);
					fos.write(LineEscaper.NEWLINE_CHAR);
					ts = Instant.ofEpochMilli(ts.toEpochMilli() + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 1000);
				}
				fos.write(LineEscaper.NEWLINE_CHAR);
			}
		}
	}

	/**
	 * We generate a file with one data point per day from 1970-1970+2000. 
	 * 
	 * @param destFolder
	 */
	private static void generateMultipleChunksInMultipleYears(File destFolder) throws Exception {
		String destFileName = "multipleChunksInMultipleYears";
		File destFile = new File(destFolder, destFileName);
		logger.info("Generating multiple chunks in multiple years into " + destFile.getPath());
		try(FileOutputStream fos = new FileOutputStream(destFile)) { 
			for(short year = 1970; year < 1970+2000; year++) { 
				writeHeader(destFileName, ArchDBRTypes.DBR_SCALAR_DOUBLE, (short) year, fos);
				Instant ts = TimeUtils.convertFromISO8601String(year + "-01-01T09:43:37.000Z");
				for(int day = 0; day < 365; day++) { 
					POJOEvent event = new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>((double)day), 0, 0);
					ByteArray val = event.getRawForm();
					fos.write(val.data, val.off, val.len);
					fos.write(LineEscaper.NEWLINE_CHAR);
					ts = Instant.ofEpochMilli(ts.toEpochMilli() + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 1000);
				}
				fos.write(LineEscaper.NEWLINE_CHAR);
			}
		}
	}
	
	/**
	 * We generate a file with some data points for 2012 for each DBR type. All data points are for 09:43:37 UTC. 
	 * 
	 * @param destFolder
	 */
	private static void generateFilesForDBRTypes(File destFolder) throws Exception {
		BoundaryConditionsSimulationValueGenerator dataGen = new BoundaryConditionsSimulationValueGenerator();
		for(ArchDBRTypes dbrType : ArchDBRTypes.values()) {
			PayloadType payloadType = dbrType.getPBPayloadType();
			String destFileName = payloadType + "_sampledata";
			File destFile = new File(destFolder, destFileName);
			logger.info("Generating file for " + payloadType + " into " + destFile.getPath());
			try(FileOutputStream fos = new FileOutputStream(destFile)) { 
				writeHeader(destFileName, dbrType, (short) 2012, fos);
				Instant ts = TimeUtils.convertFromISO8601String("2012-01-01T09:43:37.000Z");
				int totalDataPoints = 366;
				if(dbrType.isWaveForm()) { 
					totalDataPoints = 2;
				}
				for(int day = 0; day < totalDataPoints; day++) { 
					POJOEvent event = new POJOEvent(dbrType, ts, dataGen.getSampleValue(dbrType, day), 0, 0);
					ByteArray val = event.getRawForm();
					fos.write(val.data, val.off, val.len);
					fos.write(LineEscaper.NEWLINE_CHAR);
					ts = Instant.ofEpochMilli(ts.toEpochMilli() + PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk() * 1000);
				}
			}
		}
	}
	
	
	/**
	 * We generate a days worth of data.
	 * 
	 * @param destFolder
	 */
	private static void generateOneDaysWorthOfDBRDoubleData(File destFolder) throws Exception {
		String destFileName = "onedaysdbrdouble";
		File destFile = new File(destFolder, destFileName);
		logger.info("Generating a days worth of data into " + destFile.getPath());
		try(FileOutputStream fos = new FileOutputStream(destFile)) { 
			writeHeader(destFileName, ArchDBRTypes.DBR_SCALAR_DOUBLE, (short) 2011, fos);
			Instant ts = TimeUtils.convertFromISO8601String("2011-02-01T00:00:00.000Z");
			for (int seconds = 0; seconds < PartitionGranularity.PARTITION_DAY.getApproxSecondsPerChunk(); seconds++) {
				POJOEvent event = new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, ts, new ScalarValue<Double>((double)seconds), 0, 0);
				ByteArray val = event.getRawForm();
				fos.write(val.data, val.off, val.len);
				fos.write(LineEscaper.NEWLINE_CHAR);
				ts = Instant.ofEpochMilli(ts.toEpochMilli() + 1000);
			}
		}
	}


}
