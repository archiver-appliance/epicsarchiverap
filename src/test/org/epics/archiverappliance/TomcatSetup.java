/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.config.persistence.InMemoryPersistence;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;

/**
 * Setup Tomcat without having to import all the tomcat jars into your project.
 * @author mshankar
 *
 */
public class TomcatSetup {
	private static final Logger logger = LogManager.getLogger(TomcatSetup.class.getName());
	LinkedList<Process> watchedProcesses = new LinkedList<Process>();
	LinkedList<File> cleanupFolders = new LinkedList<File>();
	protected static final int DEFAULT_SERVER_STARTUP_PORT = 16000;
	
	
	private void initialSetup(String testName) throws IOException { 
		File testFolder = new File("tomcat_" + testName);
		if(testFolder.exists()) { 
			FileUtils.deleteDirectory(testFolder);
		}
		assert(testFolder.mkdir());
		cleanupFolders.add(testFolder);
		
	}
	

	/**
	 * Set up an individual tomcat with the webapps loaded.
	 * We create a work folder appropriate for the test; create the webapps/logs/conf folders and then call catalina.sh run.
	 * @throws Exception
	 */
	public void setUpWebApps(String testName) throws Exception {
		initialSetup(testName);
		AppliancesXMLGenerator.ApplianceXMLConfig applianceXMLConfig = new AppliancesXMLGenerator.ApplianceXMLConfig(testName, 1);
		Path xml = applianceXMLConfig.writeAppliancesXML();
		AppliancesXMLGenerator.AppliancePorts ports = applianceXMLConfig.appliancePortsList().get(0);
		createAndStartTomcatInstance(testName, ports.identity(), ports.retrievalPort(), ports.serverStartUpPort(), xml);
	}
	
	/**
	 * Set up a cluster of tomcat instances.
	 * Note that these are NOT clustered using tomcat's clustering technology (which is geared towards session replication and the such).
	 * @param clusterCount
	 * @throws Exception
	 */
	public void setUpClusterWithWebApps(String testName, int clusterCount) throws Exception {
		initialSetup(testName);
		AppliancesXMLGenerator.ApplianceXMLConfig applianceXMLConfig = new AppliancesXMLGenerator.ApplianceXMLConfig(testName, clusterCount);
		Path xml = applianceXMLConfig.writeAppliancesXML();
		for (AppliancesXMLGenerator.AppliancePorts appliancePorts : applianceXMLConfig.appliancePortsList()) {
			logger.info(String.format("Starting up %s", appliancePorts.identity()));
			createAndStartTomcatInstance(testName, appliancePorts.identity(), appliancePorts.retrievalPort(), appliancePorts.serverStartUpPort(), xml);
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

		createAndStartTomcatInstance(testName, "dest_appliance", ConfigServiceForTests.RETRIEVAL_TEST_PORT, DEFAULT_SERVER_STARTUP_PORT, Path.of("src/sitespecific/tests/classpathfiles/failover_dest.xml"));
		logger.info("Done starting up dest appliance");
		logger.info("Starting up other appliance");
		createAndStartTomcatInstance(testName, "other_appliance", ConfigServiceForTests.RETRIEVAL_TEST_PORT + 4, DEFAULT_SERVER_STARTUP_PORT + 4, Path.of("src/sitespecific/tests/classpathfiles/failover_other.xml"));
		logger.info("Done starting up other appliance");
	}
	

	public void tearDown() throws Exception {
		for(Process process : watchedProcesses) {
			// First try to kill the process cleanly
			long pid = process.pid();
			String cmd = "kill -s SIGINT " + pid;
			logger.info("Sending a signal using " + cmd);
			Runtime.getRuntime().exec(cmd);
			try {
				Thread.sleep(15 * 1000);
			} catch (Exception ignored) {
			}
			if (process.isAlive()) {
				logger.warn("Tomcat process did not stop propoerly within time. Forcibly stopping it.");
				process.destroyForcibly();
				try {
					Thread.sleep(3 * 60 * 1000);
				} catch (Exception ignored) {
				}
			}
		}

		for (File cleanupFolder : cleanupFolders) {
			logger.debug("Cleaning up folder " + cleanupFolder.getAbsolutePath());
			FileUtils.deleteDirectory(cleanupFolder);
		}
	}


	private void createAndStartTomcatInstance(String testName, final String applianceName,
											  int port, int startupPort, Path appliancesXML) throws IOException {
		File workFolder = makeTomcatFolders(testName, applianceName, port, startupPort);
		File logsFolder = new File(workFolder, "logs");
		assert (logsFolder.exists());

		ProcessBuilder pb = new ProcessBuilder(System.getenv("TOMCAT_HOME") + File.separator + "bin" + File.separator + "catalina.sh", "run");
		createEnvironment(testName, applianceName, pb.environment(), appliancesXML);
		pb.directory(logsFolder);
		pb.redirectErrorStream(true);
		pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
		Process p = pb.start();
		watchedProcesses.add(p);

		final CountDownLatch latch = new CountDownLatch(1);

		final BufferedReader li = new BufferedReader(new InputStreamReader(p.getInputStream()));
		Thread t = new Thread(() -> {
			catchApplianceLog(applianceName, p, latch, li);
		});
		t.start();

		// We wait for some time to make sure the server started up
		try { latch.await(2, TimeUnit.MINUTES); } catch(InterruptedException ignored) {}
		logger.info("Done starting " + applianceName + " Releasing latch");
	}

	private static void catchApplianceLog(String applianceName, Process p, CountDownLatch latch, BufferedReader li) {
		Logger applianceLogger = LogManager.getLogger("APP" + applianceName);
		try {
			String msg;
			while((msg = li.readLine()) != null && p.isAlive()) {
				applianceLogger.info(applianceName + " | " + msg);
				if(msg.contains("All components in this appliance have started up")) {
					logger.info(applianceName + " has started up.");
					latch.countDown();
				}
			}
		} catch(Exception ex) {
			logger.error("Exception starting Tomcat", ex);
		}
	}


	/**
	 * @param testName
	 * @param applianceName
	 * @param port
	 * @param startupPort
	 * @return Returns the CATALINA_BASE for this instance
	 * @throws IOException
	 */
	private File makeTomcatFolders(String testName, String applianceName, int port, int startupPort) throws IOException {
		File testFolder = new File("tomcat_" + testName);
		assert(testFolder.exists());
		File workFolder = new File(testFolder, applianceName);
		assert(workFolder.mkdir());
		
		File webAppsFolder = new File(workFolder, "webapps");
		assert(webAppsFolder.mkdir());
		File logsFolder = new File(workFolder, "logs");
		assert(logsFolder.mkdir());

		FileUtils.copyFile(new File("src/resources/test/log4j2.xml"), new File(logsFolder, "log4j2.xml"));
		File tempFolder = new File(workFolder, "temp");
		assert(tempFolder.mkdir());
		
		logger.debug("Copying the webapps wars to " + webAppsFolder.getAbsolutePath());
		FileUtils.copyFile(new File("./build/mgmt.war"), new File(webAppsFolder, "mgmt.war"));
		FileUtils.copyFile(new File("./build/retrieval.war"), new File(webAppsFolder, "retrieval.war"));
		FileUtils.copyFile(new File("./build/etl.war"), new File(webAppsFolder, "etl.war"));
		FileUtils.copyFile(new File("./build/engine.war"), new File(webAppsFolder, "engine.war"));
		
		File confOriginal = new File(System.getenv("TOMCAT_HOME"), "conf_original");
		File confFolder = new File(workFolder, "conf");
		logger.debug("Copying the config from " + confOriginal.getAbsolutePath() + " to " + confFolder.getAbsolutePath());
		if(!confOriginal.exists()) { 
			throw new IOException("For the tomcat tests to work, we expect that when you extract the tomcat distrbution to " + System.getenv("TOMCAT_HOME") + ", you copy the pristine conf folder to a folder called conf_original. This folder " + confOriginal.getAbsolutePath() + " does not seem to exist.");
		}
		// We expect that when you set up TOMCAT_HOME, 
		FileUtils.copyDirectory(confOriginal, confFolder);
		
		// We then replace the server.xml with one we generate.
		// TomcatSampleServer.xml is a simple MessageFormat template with entries for 
		// 0) server http port
		// 1) server startup port
		String serverXML = new String(Files.readAllBytes(Paths.get("./src/test/org/epics/archiverappliance/TomcatSampleServer.xml")));
		String formattedServerXML = MessageFormat.format(serverXML, port, startupPort);
		Files.write(Paths.get(confFolder.getAbsolutePath(), "server.xml"), formattedServerXML.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
		logger.debug("Done generating server.xml");

		return workFolder;
	}


	private void createEnvironment(String testName, String applianceName, Map<String, String> environment, Path appliancesXML) {
		environment.putAll(System.getenv());
		environment.remove("CLASSPATH");
		environment.put("CATALINA_HOME", System.getenv("TOMCAT_HOME"));
		File workFolder = new File("tomcat_" + testName + File.separator + applianceName);
		assert (workFolder.exists());
		environment.put("CATALINA_BASE", workFolder.getAbsolutePath());

		environment.put("LOG4J_CONFIGURATION_FILE", (new File("src/resources/test/log4j2.xml")).getAbsolutePath());
		environment.put(ConfigService.ARCHAPPL_CONFIGSERVICE_IMPL, ConfigServiceForTests.class.getName());
		environment.put(DefaultConfigService.SITE_FOR_UNIT_TESTS_NAME, DefaultConfigService.SITE_FOR_UNIT_TESTS_VALUE);

		environment.put(ConfigService.ARCHAPPL_MYIDENTITY, applianceName);
		if (!System.getProperties().containsKey(ConfigService.ARCHAPPL_APPLIANCES)) {
			environment.put(ConfigService.ARCHAPPL_APPLIANCES, appliancesXML.toAbsolutePath().toString());
		} else {
			environment.put(ConfigService.ARCHAPPL_APPLIANCES, (String) System.getProperties().get(ConfigService.ARCHAPPL_APPLIANCES));
		}

		if (!System.getProperties().containsKey(ConfigService.ARCHAPPL_PERSISTENCE_LAYER) || System.getProperties().get(ConfigService.ARCHAPPL_PERSISTENCE_LAYER).equals(InMemoryPersistence.class.getName())) {
			environment.put(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, "org.epics.archiverappliance.config.persistence.InMemoryPersistence");
		} else {
			String persistenceFile = (String) System.getProperties().get(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME);
			logger.info("Persistence layer is provided by " + System.getProperties().get(ConfigService.ARCHAPPL_PERSISTENCE_LAYER));
			assert (persistenceFile != null);
			String persistenceFileForMember = persistenceFile.replace(".jdbm2", "_" + applianceName + ".jdbm2");
			environment.put(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, (String) System.getProperties().get(ConfigService.ARCHAPPL_PERSISTENCE_LAYER));
			environment.put(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME, persistenceFileForMember);
			logger.info("Persistence file for member " + persistenceFileForMember);
		}

		overrideEnvWithSystemProperty(environment, "ARCHAPPL_SHORT_TERM_FOLDER");
		overrideEnvWithSystemProperty(environment, "ARCHAPPL_MEDIUM_TERM_FOLDER");
		overrideEnvWithSystemProperty(environment, "ARCHAPPL_LONG_TERM_FOLDER");
		overrideEnvWithSystemProperty(environment, "ARCHAPPL_POLICIES");

		if(logger.isDebugEnabled()) { 
			for(String key : environment.keySet()) { 
				logger.debug("Env " + key + "=" + environment.get(key));
			}
		}

	}
	
	
	private static void overrideEnvWithSystemProperty(Map<String, String> environment, String key) { 
		if(System.getProperties().containsKey(key)) { 
			String value = (String) System.getProperties().get(key);
			logger.debug("Overriding " + key + " from the system properties " + value);
			environment.put(key, value);
		}

	}
	
}
