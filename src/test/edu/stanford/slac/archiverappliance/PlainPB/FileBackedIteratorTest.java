package edu.stanford.slac.archiverappliance.PlainPB;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The FileBackedPBEventStream supports two iterators - one is a file-position based one and the other is a time based one.
 * For performance reasons, we should use the file-position based iterator in cases where the query start time is after the timestamp of the first sample; defaulting to the time based one in case of unexpected circumstances.
 * This test makes sure we have this behavior; these are the test cases we are expected to test.
 * <ol>
 * <li>FKTS - The timestamp of the first sample in the file</li>
 * <li>LKTS - The timestamp of the last sample in the file</li>
 * <li>QTS - The starttime of the query/request</li>
 * <li>QTE - The endtime of the query/request</li>
 * <li></li>
 * </ol>
 * <pre>
 *                 FKTS                                                      LKTS
 *  1 - QTS -- QTE  |                                                         |
 *  2 - QTS --------|-------- QTE                                             |
 *  3 - QTS --------|---------------------------------------------------------|-------------------------------- QTE
 *  4 -             |          QTS ----------------- QTE                      |
 *  5 -             |          QTS -------------------------------------------|-------------------------------- QTE
 *  6 -             |                                                         |           QTS ----------------- QTE
 * 
 * </pre>
 * 
 * @author mshankar
 *
 */
public class FileBackedIteratorTest {
	private static Logger logger = Logger.getLogger(FileBackedIteratorTest.class.getName());
	File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "FileBackedIteratorTest");
	String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":FileBackedIteratorTest";
	short currentYear = TimeUtils.getCurrentYear();
	Path pbFilePath = Paths.get(testFolder.getAbsolutePath(), pvName.replace(":", "/").replace("--", "") + ":" + currentYear + ".pb");
	ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	PlainPBStoragePlugin storagePlugin;
	private ConfigServiceForTests configService;
	private Timestamp FKTS = null;
	private Timestamp LKTS = null;

	@Before
	public void setUp() throws Exception {
		configService = new ConfigServiceForTests(new File("./bin"));
		storagePlugin = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=FileBackedIteratorTest&rootFolder=" + testFolder.getAbsolutePath() + "&partitionGranularity=PARTITION_YEAR", configService);

		// Add data with gaps every month
		DecimalFormat monthFmt = new DecimalFormat("00");
		for(int month = 1; month < 12; month++) {
			long startOfMonthEpochSeconds = TimeUtils.convertToEpochSeconds(TimeUtils.convertFromISO8601String(currentYear + "-" + monthFmt.format(month+1) + "-01T08:00:00.000Z"));
			// Generate data for  10 days
			for(int day = 0; day < 10; day++) { 
				ArrayListEventStream strm = new ArrayListEventStream(86400, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));
				for(int second = 0; second < 86400; second+=15) { 
					strm.add(new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, TimeUtils.convertFromEpochSeconds(startOfMonthEpochSeconds + day*86400 + second, 0), new ScalarValue<Double>((double)second), 0, 0));
				}
				try(BasicContext context = new BasicContext()) { 
					storagePlugin.appendData(context, pvName, strm);
				}
			}
		}
		
		PBFileInfo fileInfo = new PBFileInfo(pbFilePath);
		FKTS = fileInfo.getFirstEvent().getEventTimeStamp();
		LKTS = fileInfo.getLastEvent().getEventTimeStamp();
		logger.info("After generating data," + 
				"FKTS = " + TimeUtils.convertToISO8601String(FKTS) +
				"LKTS = " + TimeUtils.convertToISO8601String(LKTS)
				);
	}

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(testFolder);
	}

	@Test
	public void testIterator() throws IOException {
		makeSureWeGetCorrectIterator("Case 1", TimeUtils.minusDays(FKTS, 60), TimeUtils.minusDays(FKTS, 2), TimeUtils.minusDays(FKTS, 59), FKTS, FileBackedPBEventStreamTimeBasedIterator.class);
		makeSureWeGetCorrectIterator("Case 2", TimeUtils.minusDays(FKTS, 60), TimeUtils.minusDays(FKTS, 2), TimeUtils.plusDays(FKTS,1), TimeUtils.plusDays(FKTS, 90), FileBackedPBEventStreamPositionBasedIterator.class);
		makeSureWeGetCorrectIterator("Case 3", TimeUtils.minusDays(FKTS, 60), TimeUtils.minusDays(FKTS, 1), TimeUtils.plusDays(LKTS,1), TimeUtils.plusDays(LKTS, 90), FileBackedPBEventStreamPositionBasedIterator.class);
		makeSureWeGetCorrectIterator("Case 4", FKTS, TimeUtils.plusDays(FKTS, 60), TimeUtils.minusDays(LKTS,90), TimeUtils.minusDays(LKTS, 1), FileBackedPBEventStreamPositionBasedIterator.class);
		makeSureWeGetCorrectIterator("Case 5", FKTS, TimeUtils.plusDays(FKTS, 60), LKTS, TimeUtils.plusDays(LKTS, 90), FileBackedPBEventStreamPositionBasedIterator.class);
		makeSureWeGetCorrectIterator("Case 6", LKTS, TimeUtils.plusDays(LKTS, 10), TimeUtils.plusDays(LKTS,1), TimeUtils.plusDays(LKTS, 90), FileBackedPBEventStreamPositionBasedIterator.class);
	}
	
	
	/**
	 * Make sure we get the expected iterator
	 * @param testCase
	 * @param minQTS
	 * @param maxQTS
	 * @param expectedIteratorClass
	 */
	private void makeSureWeGetCorrectIterator(String testCase, 
			Timestamp minQTS, Timestamp maxQTS, 
			Timestamp minQTE, Timestamp maxQTE, 
			Class<? extends Iterator<Event>> expectedIteratorClass) throws IOException {
		for(Timestamp QTS = minQTS; QTS.before(maxQTS); QTS = TimeUtils.plusDays(QTS, 1)) { 
			for(Timestamp QTE = minQTE; QTE.before(maxQTE); QTE = TimeUtils.plusDays(QTE, 1)) {
				if(QTS.equals(QTE) || QTS.after(QTE)) { 
					// logger.info("Skipping " + " for QTS " + TimeUtils.convertToISO8601String(QTS) + " and QTE " + TimeUtils.convertToISO8601String(QTE));
					continue;
				}
				logger.debug("Checking " + testCase + " for QTS " + TimeUtils.convertToISO8601String(QTS) + " and QTE " + TimeUtils.convertToISO8601String(QTE));
				try(FileBackedPBEventStream strm = new FileBackedPBEventStream(pvName, pbFilePath, dbrType, QTS, QTE, false)) { 
					assertTrue("We are not getting the expeected iterator " + expectedIteratorClass.getName()
							+ " for " + testCase
							+ " for QTS " + TimeUtils.convertToISO8601String(QTS) 
							+ " and QTE " + TimeUtils.convertToISO8601String(QTE), 
							expectedIteratorClass.isInstance(strm.iterator()));
				}
			}			
		}
	}
}
