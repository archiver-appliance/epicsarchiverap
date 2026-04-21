/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.core.StandardContext;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Manages embedded Tomcat instances for integration tests.
 *
 * <p>Implements {@link AutoCloseable} so it can be used in try-with-resources or registered as a
 * JUnit 5 root-store {@code CloseableResource} (see {@code PvaTestSetupExtension}).
 *
 * @author mshankar
 */
public class TomcatSetup implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(TomcatSetup.class);

    public static final int DEFAULT_SERVER_STARTUP_PORT = 16000;
    private static final int BASE_STARTUP_PORT = DEFAULT_SERVER_STARTUP_PORT;
    private static final int STARTUP_TIMEOUT_MINUTES = 2;
    private static final String STARTUP_COMPLETE_MSG = "All components in this appliance have started up";

    /** Tomcat instances keyed by appliance name, in startup order. */
    private final Map<String, Tomcat> instances = new LinkedHashMap<>();

    /** Additional system properties to re-apply after each per-instance property reset. */
    private final Map<String, String> extraProperties = new HashMap<>();

    private File testFolder;
    private Properties savedProperties;

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /** Start a single-appliance setup for {@code testName}. */
    public void setUpWebApps(String testName) throws Exception {
        prepare(testName);
        AppliancesXMLGenerator.ApplianceXMLConfig config = new AppliancesXMLGenerator.ApplianceXMLConfig(testName, 1);
        Path xml = config.writeAppliancesXML();
        AppliancesXMLGenerator.AppliancePorts ports =
                config.appliancePortsList().get(0);
        start(ports.identity(), ports.retrievalPort(), BASE_STARTUP_PORT, xml);
    }

    /** Start a cluster of {@code clusterCount} appliances for {@code testName}. */
    public void setUpClusterWithWebApps(String testName, int clusterCount) throws Exception {
        prepare(testName);
        AppliancesXMLGenerator.ApplianceXMLConfig config =
                new AppliancesXMLGenerator.ApplianceXMLConfig(testName, clusterCount);
        Path xml = config.writeAppliancesXML();
        int startupPort = BASE_STARTUP_PORT;
        for (AppliancesXMLGenerator.AppliancePorts ports : config.appliancePortsList()) {
            logger.info("Starting up {}", ports.identity());
            start(ports.identity(), ports.retrievalPort(), startupPort++, xml);
            logger.info("Done starting up {}", ports.identity());
        }
    }

    /**
     * Start a dest/other appliance pair for failover testing.
     *
     * <p>Each appliance gets isolated storage folders under its own work directory; see
     * {@link #configureProperties} for details.
     */
    public void setUpFailoverWithWebApps(String testName) throws Exception {
        prepare(testName);

        logger.info("Starting up dest appliance");
        start(
                "dest_appliance",
                ConfigServiceForTests.RETRIEVAL_TEST_PORT,
                BASE_STARTUP_PORT,
                Path.of("src/sitespecific/tests/classpathfiles/failover_dest.xml"));
        logger.info("Starting up other appliance");
        start(
                "other_appliance",
                ConfigServiceForTests.RETRIEVAL_TEST_PORT + 4,
                BASE_STARTUP_PORT + 4,
                Path.of("src/sitespecific/tests/classpathfiles/failover_other.xml"));
    }

    /** Stop a single appliance by name, leaving the others running. */
    public void shutDownAppliance(String applianceName) throws Exception {
        Tomcat tomcat = instances.get(applianceName);
        if (tomcat == null) {
            throw new IllegalArgumentException("No Tomcat instance found for appliance: " + applianceName);
        }
        logger.info("Stopping Tomcat instance for {}", applianceName);
        try {
            tomcat.stop();
            tomcat.destroy();
        } catch (LifecycleException e) {
            logger.error("Error stopping {}", applianceName, e);
        }
        instances.remove(applianceName);
    }

    /** Stop all appliances, delete the test folder, and restore system properties. */
    public void tearDown() throws Exception {
        stopAll();
        deleteTestFolder();
        restoreSystemProperties();
    }

    @Override
    public void close() throws Exception {
        tearDown();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void prepare(String testName) throws IOException {
        // Snapshot system properties using putAll so the copy is a flat map, not a
        // defaults-chain. Properties.containsKey() does not search defaults, so using
        // new Properties(original) as a "reset" would silently hide keys.
        savedProperties = new Properties();
        savedProperties.putAll(System.getProperties());

        testFolder = new File("build/tomcats/tomcat_" + testName);
        if (testFolder.exists()) {
            FileUtils.deleteDirectory(testFolder);
        }
        if (!testFolder.mkdirs()) {
            throw new IOException("Could not create test folder: " + testFolder.getAbsolutePath());
        }
    }

    private void start(String applianceName, int port, int startupPort, Path appliancesXML)
            throws IOException, LifecycleException {

        File workFolder = makeWorkFolder(applianceName);

        Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir(workFolder.getAbsolutePath());
        tomcat.setPort(port);
        tomcat.getServer().setPort(startupPort);
        // Required to initialise the connector before start()
        tomcat.getConnector();

        addWebapp(tomcat, "/mgmt", "./build/exploded/mgmt");
        addWebapp(tomcat, "/retrieval", "./build/exploded/retrieval");
        addWebapp(tomcat, "/etl", "./build/exploded/etl");
        addWebapp(tomcat, "/engine", "./build/exploded/engine");

        configureProperties(applianceName, appliancesXML, workFolder);

        CountDownLatch latch = new CountDownLatch(1);
        LatchAppender appender = new LatchAppender("latch-" + applianceName, latch, STARTUP_COMPLETE_MSG);
        appender.start();

        org.apache.logging.log4j.core.Logger rootLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getRootLogger();
        rootLogger.addAppender(appender);
        try {
            tomcat.start();
            instances.put(applianceName, tomcat);
            logger.info("Waiting for {} to start...", applianceName);
            if (!latch.await(STARTUP_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                throw new RuntimeException(
                        applianceName + " did not start within " + STARTUP_TIMEOUT_MINUTES + " minutes");
            }
            logger.info("{} started successfully.", applianceName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for " + applianceName + " to start", e);
        } finally {
            rootLogger.removeAppender(appender);
            appender.stop();
        }
    }

    private void addWebapp(Tomcat tomcat, String contextPath, String docBase) throws IOException {
        File dir = new File(docBase);
        if (!dir.exists()) {
            throw new IOException("Exploded webapp not found at " + dir.getAbsolutePath()
                    + " — run the 'explodeWars' Gradle task first.");
        }
        Context ctx = tomcat.addWebapp(contextPath, dir.getAbsolutePath());
        // Delegate class loading to the parent (test) classloader first. Without this, each
        // webapp loads Log4j2 from its own WEB-INF/lib and creates a separate LoggerContext,
        // so LatchAppender (registered on the test's root logger) never sees webapp log events.
        ((StandardContext) ctx).setDelegate(true);
    }

    private File makeWorkFolder(String applianceName) throws IOException {
        File workFolder = new File(testFolder, applianceName);
        if (!workFolder.mkdir()) throw new IOException("Could not create: " + workFolder);
        for (String sub : new String[] {"webapps", "logs", "temp", "conf"}) {
            File d = new File(workFolder, sub);
            if (!d.mkdir()) throw new IOException("Could not create: " + d.getAbsolutePath());
        }
        return workFolder;
    }

    /**
     * Reset system properties to the saved snapshot, then layer instance-specific settings on top.
     *
     * <p>Uses a fresh flat copy of {@code savedProperties} so that {@code containsKey()} behaves
     * correctly (the defaults-chain form of {@link Properties} would fool it).
     */
    private void configureProperties(String applianceName, Path appliancesXML, File workFolder) {
        Properties fresh = new Properties();
        fresh.putAll(savedProperties);
        System.setProperties(fresh);

        System.setProperty("catalina.base", new File(testFolder, applianceName).getAbsolutePath());
        System.setProperty("LOG4J_CONFIGURATION_FILE", new File("src/resources/test/log4j2.xml").getAbsolutePath());
        System.setProperty(ConfigService.ARCHAPPL_CONFIGSERVICE_IMPL, ConfigServiceForTests.class.getName());
        System.setProperty(
                DefaultConfigService.SITE_FOR_UNIT_TESTS_NAME, DefaultConfigService.SITE_FOR_UNIT_TESTS_VALUE);
        System.setProperty(ConfigService.ARCHAPPL_MYIDENTITY, applianceName);

        if (!fresh.containsKey(ConfigService.ARCHAPPL_APPLIANCES)) {
            System.setProperty(
                    ConfigService.ARCHAPPL_APPLIANCES,
                    appliancesXML.toAbsolutePath().toString());
        }

        String persistenceLayer = fresh.getProperty(ConfigService.ARCHAPPL_PERSISTENCE_LAYER);
        if (persistenceLayer == null || persistenceLayer.equals(InMemoryPersistence.class.getName())) {
            System.setProperty(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, InMemoryPersistence.class.getName());
        } else {
            logger.info("Persistence layer for {}: {}", applianceName, persistenceLayer);
            String persistenceFile = fresh.getProperty(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME);
            if (persistenceFile != null) {
                System.setProperty(
                        JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME,
                        persistenceFile.replace(".jdbm2", "_" + applianceName + ".jdbm2"));
                logger.info(
                        "Persistence file for {}: {}",
                        applianceName,
                        System.getProperty(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME));
            }
        }

        // With embedded Tomcat the JVM working directory is the project root, so any relative
        // (or absent) storage folder property must be converted to an absolute path inside the
        // appliance's own work folder to restore that per-appliance isolation.
        for (String[] pair : new String[][] {
            {"ARCHAPPL_SHORT_TERM_FOLDER", "sts"},
            {"ARCHAPPL_MEDIUM_TERM_FOLDER", "mts"},
            {"ARCHAPPL_LONG_TERM_FOLDER", "lts"}
        }) {
            String current = System.getProperty(pair[0]);
            if (current == null || !new File(current).isAbsolute()) {
                File dir = new File(workFolder, pair[1]);
                dir.mkdirs();
                System.setProperty(pair[0], dir.getAbsolutePath());
            }
        }

        // Re-apply any extra properties that must survive the reset above.
        extraProperties.forEach(System::setProperty);
    }

    private void stopAll() {
        for (Map.Entry<String, Tomcat> entry : instances.entrySet()) {
            logger.info("Stopping Tomcat instance for {}", entry.getKey());
            try {
                entry.getValue().stop();
                entry.getValue().destroy();
            } catch (LifecycleException e) {
                logger.error("Error stopping {}", entry.getKey(), e);
            }
        }
        instances.clear();
    }

    private void deleteTestFolder() throws IOException {
        if (testFolder != null && testFolder.exists()) {
            FileUtils.deleteDirectory(testFolder);
        }
    }

    private void restoreSystemProperties() {
        if (savedProperties != null) {
            Properties restored = new Properties();
            restored.putAll(savedProperties);
            System.setProperties(restored);
        }
    }

    // -------------------------------------------------------------------------
    // Inner class
    // -------------------------------------------------------------------------

    /** Log4j2 appender that counts down a latch when a specific message is logged. */
    private static class LatchAppender extends AbstractAppender {
        private final CountDownLatch latch;
        private final String trigger;

        LatchAppender(String name, CountDownLatch latch, String trigger) {
            super(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);
            this.latch = latch;
            this.trigger = trigger;
        }

        @Override
        public void append(LogEvent event) {
            if (event.getMessage().getFormattedMessage().contains(trigger)) {
                latch.countDown();
            }
        }
    }
}
