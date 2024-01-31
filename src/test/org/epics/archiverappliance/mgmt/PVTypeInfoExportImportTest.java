package org.epics.archiverappliance.mgmt;

import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class PVTypeInfoExportImportTest {

	@BeforeEach
	public void setUp() throws Exception {
	}

	@AfterEach
	public void tearDown() throws Exception {
	}

	@Test
	public void testEncodePVTypeInfo() throws Exception {
        ConfigServiceForTests configService = new ConfigServiceForTests(-1);
        String pvName = "UnitTestNoNamingConvention:sine";
        MetaInfo info = new MetaInfo();
        info.addOtherMetaInfo("RTYP", "ai");
        info.setArchDBRTypes(ArchDBRTypes.DBR_SCALAR_DOUBLE);
        info.setCount(1);
		PolicyConfig policyConfig = configService.computePolicyForPV(pvName, info, new UserSpecifiedSamplingParams());
		PVTypeInfo origTypeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
		origTypeInfo.setSamplingMethod(policyConfig.getSamplingMethod());
		origTypeInfo.setSamplingPeriod(policyConfig.getSamplingPeriod());
		origTypeInfo.setDataStores(policyConfig.getDataStores());
		origTypeInfo.setCreationTime(TimeUtils.now());
		origTypeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
		configService.updateTypeInfoForPV(pvName, origTypeInfo);
        PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
        JSONEncoder<PVTypeInfo> typeInfoEncoder = JSONEncoder.getEncoder(PVTypeInfo.class);
        JSONObject pvTypeJson = typeInfoEncoder.encode(typeInfo);
        String jsonString = pvTypeJson.toJSONString();
        JSONObject unmarshalledJSONObject = (JSONObject) JSONValue.parse(jsonString);
        PVTypeInfo unmarshalledTypeInfo = new PVTypeInfo();
        JSONDecoder<PVTypeInfo> typeInfoDecoder = JSONDecoder.getDecoder(PVTypeInfo.class);
        typeInfoDecoder.decode((JSONObject) unmarshalledJSONObject, unmarshalledTypeInfo);
        Assertions.assertTrue(typeInfo.getPvName().equals(unmarshalledTypeInfo.getPvName()), "Expecting pvName to be " + typeInfo.getPvName() + "; instead it is " + unmarshalledTypeInfo.getPvName());
        Assertions.assertTrue(typeInfo.getSamplingPeriod() == unmarshalledTypeInfo.getSamplingPeriod(), "Expecting samplingPeriod to be " + typeInfo.getSamplingPeriod() + "; instead it is " + unmarshalledTypeInfo.getSamplingPeriod());
        Assertions.assertTrue(typeInfo.getSamplingMethod().equals(unmarshalledTypeInfo.getSamplingMethod()), "Expecting samplingMethod to be " + typeInfo.getSamplingMethod() + "; instead it is " + unmarshalledTypeInfo.getSamplingMethod());
        Assertions.assertTrue(Arrays.equals(typeInfo.getDataStores(), unmarshalledTypeInfo.getDataStores()), "Expecting dataStores to be " + Arrays.toString(typeInfo.getDataStores()) + "; instead it is " + Arrays.toString(unmarshalledTypeInfo.getDataStores()));
	}
	
	@Test
	public void testEncodeMetaInfo() throws Exception {
		MetaInfo metaInfo = new MetaInfo();
		metaInfo.setArchDBRTypes(ArchDBRTypes.DBR_SCALAR_DOUBLE);
        JSONEncoder<MetaInfo> metaInfoEncoder = JSONEncoder.getEncoder(MetaInfo.class);
        JSONObject metaInfoJSON = metaInfoEncoder.encode(metaInfo);
        String jsonString = metaInfoJSON.toJSONString();
        JSONObject unmarshalledJSONObject = (JSONObject) JSONValue.parse(jsonString);
        MetaInfo unmarshalledMetaInfo = new MetaInfo();
        JSONDecoder<MetaInfo> metaInfoDecoder = JSONDecoder.getDecoder(MetaInfo.class);
        metaInfoDecoder.decode((JSONObject) unmarshalledJSONObject, unmarshalledMetaInfo);
        Assertions.assertTrue(metaInfo.getArchDBRTypes().equals(unmarshalledMetaInfo.getArchDBRTypes()), "Expecting DBRType to be " + metaInfo.getArchDBRTypes() + "; instead it is " + unmarshalledMetaInfo.getArchDBRTypes());
	}
}
