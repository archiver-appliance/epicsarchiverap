package org.epics.archiverappliance.mgmt.appxml;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Map;

/**
 * Test various versions of appliances.xml and make sure we can start the config service correctly.
 * @author mshankar
 *
 */
@Tag("integration")
public class IPApplianceXMLTest {
    private static Logger logger = LogManager.getLogger(IPApplianceXMLTest.class.getName());
    File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ApplianceXMLTest");
    TomcatSetup tomcatSetup = new TomcatSetup();

    @BeforeEach
    public void setUp() throws Exception {
        if (testFolder.exists()) {
            FileUtils.deleteDirectory(testFolder);
        }
        testFolder.mkdirs();
    }

    @AfterEach
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(testFolder);
        tomcatSetup.tearDown();
    }

    @Test
    public void testIPAddressAppliancesXML() throws Exception {
        String ipaddress = InetAddress.getLocalHost().getHostAddress();
        logger.info("Testing appliances.xml with ip address using " + ipaddress);
        String appliancesFilename = testFolder.getAbsolutePath() + File.separator + "ipaddress_appliances.xml";
        try (PrintWriter out = new PrintWriter(new File(appliancesFilename))) {
            out.println("<appliances>\n\t<appliance>\n\t\t<identity>appliance0</identity>\n\t\t" + "<cluster_inetport>"
                    + ipaddress + ":16670</cluster_inetport>\n\t\t<mgmt_url>http://" + ipaddress
                    + ":17665/mgmt/bpl</mgmt_url>\n\t\t" + "<engine_url>http://"
                    + ipaddress + ":17665/engine/bpl</engine_url>\n\t\t<etl_url>http://" + ipaddress
                    + ":17665/etl/bpl</etl_url>" + "<retrieval_url>http://"
                    + ipaddress + ":17665/retrieval/bpl</retrieval_url>\n\t\t<data_retrieval_url>http://" + ipaddress
                    + ":17665/retrieval</data_retrieval_url>\n\t" + "</appliance>\n\t</appliances>");
        }
        System.getProperties().put(ConfigService.ARCHAPPL_APPLIANCES, appliancesFilename);

        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());

        String mgmtURL = "http://localhost:17665/mgmt/bpl/";
        JSONArray statuses = GetUrlContent.getURLContentWithQueryParametersAsJSONArray(mgmtURL + "getPVStatus", Map.of("pv", "*"));
        Assertions.assertTrue(statuses != null);
        Assertions.assertTrue(statuses.size() == 0);
    }
}
