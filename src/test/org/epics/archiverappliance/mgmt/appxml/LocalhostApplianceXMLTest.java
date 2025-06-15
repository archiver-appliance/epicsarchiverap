package org.epics.archiverappliance.mgmt.appxml;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;

/**
 * Test various versions of appliances.xml and make sure we can start the config service correctly.
 * @author mshankar
 *
 */
@Tag("integration")
public class LocalhostApplianceXMLTest {
    private static Logger logger = LogManager.getLogger(LocalhostApplianceXMLTest.class.getName());
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
    }

    @Test
    public void testIPAddressAppliancesXML() throws Exception {
        String localhost = "localhost";
        logger.info("Testing appliances.xml with localhost using " + localhost);
        String appliancesFilename = testFolder.getAbsolutePath() + File.separator + "localhostaddress_appliances.xml";
        try (PrintWriter out = new PrintWriter(new File(appliancesFilename))) {
            out.println("<appliances>\n\t<appliance>\n\t\t<identity>appliance0</identity>\n\t\t" + "<cluster_inetport>"
                    + localhost + ":16670</cluster_inetport>\n\t\t<mgmt_url>http://" + localhost
                    + ":17665/mgmt/bpl</mgmt_url>\n\t\t" + "<engine_url>http://"
                    + localhost + ":17665/engine/bpl</engine_url>\n\t\t<etl_url>http://" + localhost
                    + ":17665/etl/bpl</etl_url>" + "<retrieval_url>http://"
                    + localhost + ":17665/retrieval/bpl</retrieval_url>\n\t\t<data_retrieval_url>http://" + localhost
                    + ":17665/retrieval</data_retrieval_url>\n\t" + "</appliance>\n\t</appliances>");
        }
        System.getProperties().put(ConfigService.ARCHAPPL_APPLIANCES, appliancesFilename);

        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
        boolean found = false;
        try(LineNumberReader rdr = new LineNumberReader(new InputStreamReader(GetUrlContent.getURLContentAsStream("http://localhost:17665/mgmt/ui/index.html")))) {
            String line = rdr.readLine();
            while(line != null) {
                if(line.contains("id=\"archstatpVNames\"")) {
                    found = true;
                    break;
                }
                line = rdr.readLine();
            }
        }

        Assertions.assertTrue(found, "Cannot find the element with id archstatpVNames");
        tomcatSetup.tearDown();
    }
}
