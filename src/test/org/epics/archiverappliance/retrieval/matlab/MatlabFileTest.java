package org.epics.archiverappliance.retrieval.matlab;

import static edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin.PB_PLUGIN_IDENTIFIER;
import static org.epics.archiverappliance.utils.ui.URIUtils.pluginString;

import com.jmatio.io.MatFileReader;
import com.jmatio.io.MatFileWriter;
import com.jmatio.types.MLArray;
import com.jmatio.types.MLChar;
import com.jmatio.types.MLDouble;
import com.jmatio.types.MLStructure;
import com.jmatio.types.MLUInt64;
import com.jmatio.types.MLUInt8;
import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadWorkerEventStream;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.LinkedList;
import java.util.Map;

/**
 * Test generation of Matlab files from event streams
 * @author mshankar
 *
 */
public class MatlabFileTest {
    private static Logger logger = LogManager.getLogger(MatlabFileTest.class.getName());
    ConfigService configService;
    PlainStoragePlugin storageplugin;
    String rootFolderName = ConfigServiceForTests.getDefaultPBTestFolder() + "/" + "MatlabFileTest";
    String pvName = "Test_MatlabPV";

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
        storageplugin = (PlainStoragePlugin) StoragePluginURLParser.parseStoragePlugin(
                pluginString(
                        PB_PLUGIN_IDENTIFIER,
                        "localhost",
                        "name=STS&rootFolder=" + rootFolderName + "/&partitionGranularity=PARTITION_YEAR"),
                configService);
        if (new File(rootFolderName).exists()) {
            FileUtils.deleteDirectory(new File(rootFolderName));
        }

        try (BasicContext context = new BasicContext()) {
            int totalNum = 6 * 60 * 24;
            short currentYear = TimeUtils.getCurrentYear();
            ArrayListEventStream testData = new ArrayListEventStream(
                    totalNum, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));
            for (int s = 0; s < totalNum; s++) {
                testData.add(new SimulationEvent(
                        s * 10, currentYear, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>((double) s * 10)));
            }
            storageplugin.appendData(context, pvName, testData);
        }
    }

    @AfterEach
    public void tearDown() throws Exception {}

    interface GenMLArray {
        public MLArray generateColumn(ArrayListEventStream dest);
    }

    class GenTimeArray implements GenMLArray {
        public MLArray generateColumn(ArrayListEventStream dest) {
            MLUInt64 ret = new MLUInt64("epochSeconds", new int[] {dest.size(), 1});
            int i = 0;
            for (Event e : dest) {
                ret.set(e.getEpochSeconds(), i++);
            }
            return ret;
        }
    }

    class GenValueArray implements GenMLArray {
        public MLArray generateColumn(ArrayListEventStream dest) {
            MLDouble ret = new MLDouble("values", new int[] {dest.size(), 1});
            int i = 0;
            for (Event e : dest) {
                ret.set(e.getSampleValue().getValue().doubleValue(), i++);
            }
            return ret;
        }
    }

    class GenNanosArray implements GenMLArray {
        public MLArray generateColumn(ArrayListEventStream dest) {
            MLUInt64 ret = new MLUInt64("nanos", new int[] {dest.size(), 1});
            int i = 0;
            for (Event e : dest) {
                ret.set((long) e.getEventTimeStamp().getNano(), i++);
            }
            return ret;
        }
    }

    class GenisDSTArray implements GenMLArray {
        public MLArray generateColumn(ArrayListEventStream dest) {
            MLUInt8 ret = new MLUInt8("isDST", new int[] {dest.size(), 1});
            int i = 0;
            for (Event e : dest) {
                ret.set(TimeUtils.isDST(e.getEventTimeStamp()) ? (byte) 1 : (byte) 0, i++);
            }
            return ret;
        }
    }

    @SuppressWarnings("unused")
    @Test
    public void testGenerateMatlabFile() throws Exception {
        short currentYear = TimeUtils.getCurrentYear();
        String fileName = rootFolderName + "/" + "sample1.mat";
        try (BasicContext context = new BasicContext();
                EventStream strm = new CurrentThreadWorkerEventStream(
                        pvName,
                        storageplugin.getDataForPV(
                                context,
                                pvName,
                                TimeUtils.getStartOfYear(currentYear),
                                TimeUtils.getEndOfYear(currentYear)))) {

            ArrayListEventStream dest = new ArrayListEventStream(
                    0, new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, pvName, currentYear));
            for (Event e : strm) {
                dest.add(e.makeClone());
            }

            logger.info("Generating matlab file to " + fileName);

            MLStructure dataStruct = new MLStructure("data", new int[] {1, 1});
            dataStruct.setField("epochSeconds", new GenTimeArray().generateColumn(dest));
            dataStruct.setField("values", new GenValueArray().generateColumn(dest));
            dataStruct.setField("nanos", new GenNanosArray().generateColumn(dest));
            dataStruct.setField("isDST", new GenisDSTArray().generateColumn(dest));
            MLStructure headerStruct = new MLStructure("header", new int[] {1, 1});
            headerStruct.setField("source", new MLChar("source", "Archiver appliance"));
            headerStruct.setField("pvName", new MLChar("pvName", pvName));
            headerStruct.setField(
                    "from",
                    new MLChar("pvName", TimeUtils.convertToISO8601String(TimeUtils.getStartOfYear(currentYear))));
            headerStruct.setField(
                    "to", new MLChar("pvName", TimeUtils.convertToISO8601String(TimeUtils.getEndOfYear(currentYear))));
            LinkedList<MLArray> dataList = new LinkedList<MLArray>();
            dataList.add(headerStruct);
            dataList.add(dataStruct);
            new MatFileWriter(new File(fileName), dataList);
            logger.info("Done generating matlab file to " + fileName);
        }

        MatFileReader reader = new MatFileReader(fileName);
        Map<String, MLArray> content = reader.getContent();
        logger.info(fileName + " has " + content.size() + " arrays");
        for (String key : content.keySet()) {
            logger.info("Key: " + key);
        }
        MLStructure pvDataFileFile = (MLStructure) content.get("data");
        Assertions.assertTrue(pvDataFileFile != null, "Cannot find data for pv in file ");
        MLArray epochSecondsArray = pvDataFileFile.getField("epochSeconds");
        Assertions.assertTrue(epochSecondsArray != null, "Cannot find epochSeconds for pv in file ");
        MLArray valueArray = pvDataFileFile.getField("values");
        Assertions.assertTrue(valueArray != null, "Cannot find value for pv in file ");
    }
}
