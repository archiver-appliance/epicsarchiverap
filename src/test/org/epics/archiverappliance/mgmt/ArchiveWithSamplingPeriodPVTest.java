package org.epics.archiverappliance.mgmt;

import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.engine.V4.PVAccessUtil;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

@Tag("integration")
@Tag("localEpics")
public class ArchiveWithSamplingPeriodPVTest {
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();

    @BeforeEach
    public void setUp() throws Exception {
        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
    }

    @AfterEach
    public void tearDown() throws Exception {
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    @Test
    public void testArchiveWithSamplingPeriod() throws Exception {
        String pvNameToArchive = "UnitTestNoNamingConvention:sine";
        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        GetUrlContent.postDataAndGetContentAsJSONArray(
                mgmtURL + "/archivePV",
                GetUrlContent.from(List.of(new JSONObject(Map.of("pv", pvNameToArchive, "samplingperiod", "10.0")))));
        PVAccessUtil.waitForStatusChange(pvNameToArchive, "Being archived", 10, mgmtURL, 15);
        @SuppressWarnings("unchecked")
        Map<String, String> status = (Map<String, String>) GetUrlContent.getURLContentWithQueryParametersAsJSONArray(
                        mgmtURL + "getPVStatus", Map.of("pv", pvNameToArchive))
                .get(0);
        Assertions.assertTrue(
                "10.0".equals(status.get("samplingPeriod")),
                "Expecting sampling rate to be 10.0; instead it is " + status.get("samplingPeriod"));
    }
}
