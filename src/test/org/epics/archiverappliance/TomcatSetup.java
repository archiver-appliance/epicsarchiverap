/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.config.persistence.InMemoryPersistence;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Setup an embedded Tomcat server programmatically for integration tests.
 * This is similar to how Spring Boot integration testing works.
 * @author mshankar
 */
public class TomcatSetup {
    private static final Logger logger = LogManager.getLogger(TomcatSetup.class.getName());
    // Store Tomcat instances instead of Processes
    private final Map<String, Tomcat> tomcatInstances = new HashMap<>();
    private final LinkedList<File> cleanupFolders = new LinkedList<>();
    private final Properties originalSystemProperties = new Properties();

    protected static final int DEFAULT_SERVER_STARTUP_PORT = 16000;

    /**
     * Set up an individual tomcat with the webapps loaded.
     * We create a work folder appropriate for the test and start an embedded Tomcat instance.
     * @throws Exception
     */
    public void setUpWebApps(String testName) throws Exception {
        initialSetup(testName);
        AppliancesXMLGenerator.ApplianceXMLConfig applianceXMLConfig =
                new AppliancesXMLGenerator.ApplianceXMLConfig(testName, 1);
        Path xml = applianceXMLConfig.writeAppliancesXML();
        AppliancesXMLGenerator.AppliancePorts ports =
                applianceXMLConfig.appliancePortsList().get(0);
        createAndStartTomcatInstance(testName, ports.identity(), ports.retrievalPort(), ports.serverStartUpPort(), xml);
    }

    /**
     * Set up a cluster of tomcat instances.
     * @param clusterCount
     * @throws Exception
     */
    public void setUpClusterWithWebApps(String testName, int clusterCount) throws Exception {
        initialSetup(testName);
        AppliancesXMLGenerator.ApplianceXMLConfig applianceXMLConfig =
                new AppliancesXMLGenerator.ApplianceXMLConfig(testName, clusterCount);
        Path xml = applianceXMLConfig.writeAppliancesXML();
        for (AppliancesXMLGenerator.AppliancePorts appliancePorts : applianceXMLConfig.appliancePortsList()) {
            logger.info(String.format("Starting up %s", appliancePorts.identity()));
            createAndStartTomcatInstance(
                    testName,
                    appliancePorts.identity(),
                    appliancePorts.retrievalPort(),
                    appliancePorts.serverStartUpPort(),
                    xml);
            logger.info(String.format("Done starting up %s", appliancePorts.identity()));
        }
    }

    /**
     * Set up a dest/other pair to test failover.
     * @param testName
     * @throws Exception
     */
    public void setUpFailoverWithWebApps(String testName) throws Exception {
        initialSetup(testName);
        logger.info("Starting up dest appliance");
        // We are in the tomcat logs folder...
        System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER", "../sts");
        System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", "../mts");
        System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", "../lts");

        createAndStartTomcatInstance(
                testName,
                "dest_appliance",
                ConfigServiceForTests.RETRIEVAL_TEST_PORT,
                DEFAULT_SERVER_STARTUP_PORT,
                Path.of("src/sitespecific/tests/classpathfiles/failover_dest.xml"));
        logger.info("Done starting up dest appliance");
        logger.info("Starting up other appliance");
        createAndStartTomcatInstance(
                testName,
                "other_appliance",
                ConfigServiceForTests.RETRIEVAL_TEST_PORT + 4,
                DEFAULT_SERVER_STARTUP_PORT + 4,
                Path.of("src/sitespecific/tests/classpathfiles/failover_other.xml"));
        logger.info("Done starting up other appliance");
    }

    private void initialSetup(String testName) throws IOException {
        // Backup original system properties to prevent test pollution
        originalSystemProperties.putAll(System.getProperties());

        File testFolder = new File("build/tomcats/tomcat_" + testName);
        if (testFolder.exists()) {
            FileUtils.deleteDirectory(testFolder);
        }
        assert testFolder.mkdirs();
        cleanupFolders.add(testFolder);
    }

    public void shutDownAppliance(String applianceName) throws Exception {
        Tomcat tomcat = tomcatInstances.get(applianceName);
        if (tomcat == null) {
            throw new Exception("Cannot find Tomcat instance for appliance " + applianceName);
        }
        logger.info("Stopping Tomcat instance for " + applianceName);
        try {
            tomcat.stop();
            tomcat.destroy();
        } catch (LifecycleException e) {
            logger.error("Error stopping Tomcat instance " + applianceName, e);
        }
        tomcatInstances.remove(applianceName);
    }

    public void tearDown() throws Exception {
        stopTomcatInstances();
        deleteUsedDirectories();
        // Restore original system properties
        System.setProperties(originalSystemProperties);
    }

    private void deleteUsedDirectories() throws IOException {
        for (File f : cleanupFolders) {
            if (f.exists()) {
                FileUtils.deleteDirectory(f);
            }
        }
    }

    private void stopTomcatInstances() {
        for (Map.Entry<String, Tomcat> entry : tomcatInstances.entrySet()) {
            logger.info("Stopping Tomcat instance for " + entry.getKey());
            try {
                entry.getValue().stop();
                entry.getValue().destroy();
            } catch (LifecycleException e) {
                logger.error("Error stopping Tomcat instance " + entry.getKey(), e);
            }
        }
        tomcatInstances.clear();
    }

    private void createAndStartTomcatInstance(
            String testName, final String applianceName, int port, int startupPort, Path appliancesXML)
            throws IOException, LifecycleException {

        Tomcat tomcat = new Tomcat();
        File workFolder = makeTomcatFolders(testName, applianceName);
        tomcat.setBaseDir(workFolder.getAbsolutePath());
        tomcat.setPort(port);
        tomcat.getServer().setPort(startupPort);
        // This is required to initialize the connector
        tomcat.getConnector();

        // Deploy all webapps
        tomcat.addWebapp("/mgmt", new File("./build/libs/mgmt.war").getAbsolutePath());
        tomcat.addWebapp("/retrieval", new File("./build/libs/retrieval.war").getAbsolutePath());
        tomcat.addWebapp("/etl", new File("./build/libs/etl.war").getAbsolutePath());
        tomcat.addWebapp("/engine", new File("./build/libs/engine.war").getAbsolutePath());

        // Configure system properties for this instance before starting
        configureSystemPropertiesForInstance(testName, applianceName, appliancesXML);

        // Use a latch and a custom log appender to wait for the "startup complete" message
        final CountDownLatch latch = new CountDownLatch(1);
        final String startupMessage = "All components in this appliance have started up";
        LatchAppender latchAppender = new LatchAppender("LatchAppender-" + applianceName, latch, startupMessage);
        latchAppender.start();

        org.apache.logging.log4j.core.Logger rootLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getRootLogger();
        rootLogger.addAppender(latchAppender);

        try {
            tomcat.start();
            tomcatInstances.put(applianceName, tomcat);

            logger.info("Waiting for " + applianceName + " to start up...");
            boolean started = latch.await(2, TimeUnit.MINUTES);
            if (started) {
                logger.info(applianceName + " has started up successfully.");
            } else {
                logger.error(applianceName + " did not start up within the timeout.");
                // Optionally, throw an exception here to fail the test
                // throw new RuntimeException(applianceName + " failed to start.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while waiting for Tomcat to start", e);
        } finally {
            // Clean up the appender
            rootLogger.removeAppender(latchAppender);
            latchAppender.stop();
        }
    }

    private File makeTomcatFolders(String testName, String applianceName) throws IOException {
        File testFolder = new File("build/tomcats/tomcat_" + testName);
        assert (testFolder.exists());
        File workFolder = new File(testFolder, applianceName);
        assert (workFolder.mkdir());

        // Tomcat needs these directories to exist
        new File(workFolder, "webapps").mkdir();
        new File(workFolder, "logs").mkdir();
        new File(workFolder, "temp").mkdir();
        new File(workFolder, "conf").mkdir();

        return workFolder;
    }

    private void configureSystemPropertiesForInstance(String testName, String applianceName, Path appliancesXML) {
        // Clear and set properties for the current instance.
        // This is safe because we run instances sequentially within a test method.
        System.setProperties(new Properties(originalSystemProperties));

        System.setProperty(
                "catalina.base", new File("build/tomcats/tomcat_" + testName, applianceName).getAbsolutePath());
        System.setProperty("LOG4J_CONFIGURATION_FILE", new File("src/resources/test/log4j2.xml").getAbsolutePath());
        System.setProperty(ConfigService.ARCHAPPL_CONFIGSERVICE_IMPL, ConfigServiceForTests.class.getName());
        System.setProperty(
                DefaultConfigService.SITE_FOR_UNIT_TESTS_NAME, DefaultConfigService.SITE_FOR_UNIT_TESTS_VALUE);
        System.setProperty(ConfigService.ARCHAPPL_MYIDENTITY, applianceName);

        if (!System.getProperties().containsKey(ConfigService.ARCHAPPL_APPLIANCES)) {
            System.setProperty(
                    ConfigService.ARCHAPPL_APPLIANCES,
                    appliancesXML.toAbsolutePath().toString());
        }

        if (!System.getProperties().containsKey(ConfigService.ARCHAPPL_PERSISTENCE_LAYER)
                || System.getProperty(ConfigService.ARCHAPPL_PERSISTENCE_LAYER)
                        .equals(InMemoryPersistence.class.getName())) {
            System.setProperty(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, InMemoryPersistence.class.getName());
        } else {
            String persistenceFile = System.getProperty(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME);
            logger.info(
                    "Persistence layer is provided by " + System.getProperty(ConfigService.ARCHAPPL_PERSISTENCE_LAYER));
            assert (persistenceFile != null);
            String persistenceFileForMember = persistenceFile.replace(".jdbm2", "_" + applianceName + ".jdbm2");
            System.setProperty(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFileForMember);
            logger.info("Persistence file for member " + persistenceFileForMember);
        }
    }

    /**
     * A custom Log4j2 Appender that waits for a specific message and counts down a latch.
     */
    private static class LatchAppender extends AbstractAppender {
        private final CountDownLatch latch;
        private final String messageToWatchFor;

        protected LatchAppender(String name, CountDownLatch latch, String messageToWatchFor) {
            super(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);
            this.latch = latch;
            this.messageToWatchFor = messageToWatchFor;
        }

        @Override
        public void append(LogEvent event) {
            if (event.getMessage().getFormattedMessage().contains(messageToWatchFor)) {
                latch.countDown();
            }
        }
    }
}
