package org.epics.archiverappliance.retrieval;

import static org.epics.archiverappliance.config.ConfigServiceForTests.MGMT_URL;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.PlainStorageType;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.Assertions;

import java.beans.IntrospectionException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class TypeInfoUtil {
    private static final String ltsFolder = System.getenv("ARCHAPPL_LONG_TERM_FOLDER");

    public static void updatePVStorageType(String pvName, PlainStorageType plainStorageType)
            throws ConfigException, IOException, IntrospectionException, NoSuchMethodException, IllegalAccessException,
                    InvocationTargetException, InstantiationException {
        PlainStoragePlugin pbplugin = new PlainStoragePlugin(plainStorageType);

        ConfigService configService = new ConfigServiceForTests(-1);

        // Set up pbplugin so that data can be retrieved using the instance
        pbplugin.initialize(
                plainStorageType.plainFileHandler().pluginIdentifier() + "://localhost?name=LTS&rootFolder=" + ltsFolder
                        + "&partitionGranularity=PARTITION_YEAR",
                configService);

        updateTypeInfo(configService, pbplugin, pvName, null, null);
    }

    public static void updateTypeInfo(
            ConfigService configService,
            PlainStoragePlugin pbplugin,
            String pvName,
            String creationTime,
            String appliance)
            throws IntrospectionException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
                    InstantiationException, IOException {
        // Load a sample PVTypeInfo from a prototype file.
        JSONObject srcPVTypeInfoJSON = (JSONObject) JSONValue.parse(new InputStreamReader(new FileInputStream(
                "src/test/org/epics/archiverappliance/retrieval/postprocessor/data/PVTypeInfoPrototype.json")));

        // Create target for decoded type info from JSON
        PVTypeInfo srcPVTypeInfo = new PVTypeInfo();

        // Decoder for PVTypeInfo
        JSONDecoder<PVTypeInfo> decoder = JSONDecoder.getDecoder(PVTypeInfo.class);

        // Create type info from the data
        decoder.decode(srcPVTypeInfoJSON, srcPVTypeInfo);

        PVTypeInfo pvTypeInfo1 = new PVTypeInfo(pvName, srcPVTypeInfo);
        Assertions.assertEquals(pvTypeInfo1.getPvName(), pvName);

        JSONEncoder<PVTypeInfo> encoder = JSONEncoder.getEncoder(PVTypeInfo.class);

        pvTypeInfo1.setPaused(true);
        pvTypeInfo1.setChunkKey(configService.getPVNameToKeyConverter().convertPVNameToKey(pvName));

        if (creationTime != null) pvTypeInfo1.setCreationTime(TimeUtils.convertFromISO8601String(creationTime));
        pvTypeInfo1.setModificationTime(TimeUtils.now());

        if (appliance != null) pvTypeInfo1.setApplianceIdentity(appliance);

        if (pbplugin != null) {
            var dataStores = pvTypeInfo1.getDataStores();
            dataStores[2] = pbplugin.getURLRepresentation();
            pvTypeInfo1.setDataStores(dataStores);
        }

        GetUrlContent.postDataAndGetContentAsJSONObject(
                MGMT_URL + "/putPVTypeInfo?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8)
                        + "&override=false&createnew=true",
                encoder.encode(pvTypeInfo1));
    }
}
