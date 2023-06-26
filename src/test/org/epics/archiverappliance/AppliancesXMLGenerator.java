package org.epics.archiverappliance;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.RenderResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigServiceForTests;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;


public class AppliancesXMLGenerator {
    private static final Logger logger = LogManager.getLogger(AppliancesXMLGenerator.class);
    static final int CLUSTER_INETPORT = 16670;

    record AppliancePorts(String hostname, String identity,
                          int clusterInetport,
                          int mgmtPort, int enginePort, int etlPort,
                          int retrievalPort, int serverStartUpPort) {
        String getHostname() {
            return this.hostname();
        }
    }

    record ApplianceXMLConfig(String testName, int clusterSize) {

        List<AppliancePorts> appliancePortsList() {
            return IntStream.range(0, this.clusterSize).mapToObj(i -> new AppliancePorts(
                    "localhost",
                    "appliance" + i,
                    CLUSTER_INETPORT + i,
                    ConfigServiceForTests.RETRIEVAL_TEST_PORT + i,
                    ConfigServiceForTests.RETRIEVAL_TEST_PORT + i,
                    ConfigServiceForTests.RETRIEVAL_TEST_PORT + i,
                    ConfigServiceForTests.RETRIEVAL_TEST_PORT + i,
                    TomcatSetup.DEFAULT_SERVER_STARTUP_PORT + i
            )).toList();
        }

        public Path writeAppliancesXML() throws IOException {
            File appliancesXMLFile = File.createTempFile("appliances_" + this.testName, ".xml");
            String xml = generateXML(this.appliancePortsList());
            Files.asCharSink(appliancesXMLFile, StandardCharsets.UTF_8).write(xml);
            logger.info(String.format("Wrote appliances xml file %s %s", appliancesXMLFile.getAbsolutePath(), xml));
            return appliancesXMLFile.toPath();
        }
    }

    static String generateXML(List<AppliancePorts> appliancePorts) throws IOException {
        Jinjava jinjava = new Jinjava();
        String template = Resources.toString(Resources.getResource("appliances.xml.j2"), StandardCharsets.UTF_8);
        RenderResult renderResult = jinjava.renderForResult(template, Map.of("cluster", appliancePorts));
        return renderResult.getOutput();
    }

}
