package org.epics.archiverappliance.docker;

import org.awaitility.Awaitility;
import org.epics.archiverappliance.ArchiveTestUtils;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end smoke test of the packaged Docker image.
 */
@Testcontainers
@Tag("docker")
public class DockerSmokeTest {
    private static final Logger logger = LoggerFactory.getLogger(DockerSmokeTest.class);

    private static final String ARCHAPPL_SERVICE = "archappl";
    private static final String MGMT_URL = "http://localhost:8080/mgmt/bpl/";

    private static final String PV_NAME = "IOCtest_0";

    @Container
    static ComposeContainer environment = new ComposeContainer(new File("docker/docker-compose.smoketest.yml"))
            .withLocalCompose(true)
            .waitingFor(
                    ARCHAPPL_SERVICE,
                    Wait.forLogMessage(".*Server startup in.*", 1).withStartupTimeout(Duration.ofMinutes(5)))
            .withLogConsumer(ARCHAPPL_SERVICE, new Slf4jLogConsumer(logger).withPrefix(ARCHAPPL_SERVICE))
            .withLogConsumer("example-ioc", new Slf4jLogConsumer(logger).withPrefix("example-ioc"));

    @Test
    void testArchivePVInContainer() {
        // Tomcat reports startup once webapps are deployed; give the mgmt servlet a brief grace
        // period to start answering before submitting the archive request.
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> GetUrlContent.getURLContentAsJSONObject(MGMT_URL + "getApplianceInfo") != null);

        GetUrlContent.getURLContentAsJSONArray(
                MGMT_URL + "archivePV?pv=" + PV_NAME + "&samplingperiod=1&samplingmethod=MONITOR");

        // Poll getPVStatus until the workflow has promoted the PV out of "Initial sampling".
        // The transition is observed at ~60-70s locally; budget 180s as ArchivePVTest does (150s).
        ArchiveTestUtils.waitForStatusChange(PV_NAME, "Being archived", 12, MGMT_URL, 15);
    }
}
