/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;


import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.config.persistence.InMemoryPersistence;
import org.epics.archiverappliance.config.persistence.JDBM2Persistence;
import org.python.google.common.io.LineReader;

/**
 * Setup Tomcat without having to import all the tomcat jars into your project.
 * @author mshankar
 *
 */
public class TomcatSetup {
	private static Logger logger = Logger.getLogger(TomcatSetup.class.getName());
	LinkedList<Process> watchedProcesses = new LinkedList<Process>();
	LinkedList<File> cleanupFolders = new LinkedList<File>();
	private static int DEFAULT_SERVER_STARTUP_PORT = 16000;
	
	
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
		createAndStartTomcatInstance(testName, ConfigServiceForTests.TESTAPPLIANCE0, ConfigServiceForTests.RETRIEVAL_TEST_PORT, DEFAULT_SERVER_STARTUP_PORT);
	}
	
	/**
	 * Set up a cluster of tomcat instances.
	 * Note that these are NOT clustered using tomcat's clustering technology (which is geared towards session replication and the such).
	 * @param clusterCount
	 * @throws Exception
	 */
	public void setUpClusterWithWebApps(String testName, int clusterCount) throws Exception {
		initialSetup(testName);
		for(int clusterIndex = 0; clusterIndex < clusterCount; clusterIndex++) {
			logger.info("Starting up " + "appliance" + clusterIndex);
			createAndStartTomcatInstance(testName, "appliance" + clusterIndex, ConfigServiceForTests.RETRIEVAL_TEST_PORT + clusterIndex, DEFAULT_SERVER_STARTUP_PORT+clusterIndex);
			logger.info("Done starting up " + "appliance" + clusterIndex);
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
		System.getProperties().put(ConfigService.ARCHAPPL_APPLIANCES, "../webapps/mgmt/WEB-INF/classes/failover_dest.xml"); 
		System.getProperties().put("ARCHAPPL_SHORT_TERM_FOLDER", "../sts"); 
		System.getProperties().put("ARCHAPPL_MEDIUM_TERM_FOLDER", "../mts"); 
		System.getProperties().put("ARCHAPPL_LONG_TERM_FOLDER", "../lts"); 
				
		createAndStartTomcatInstance(testName, "dest_appliance", ConfigServiceForTests.RETRIEVAL_TEST_PORT, DEFAULT_SERVER_STARTUP_PORT);
		logger.info("Done starting up dest appliance");
		logger.info("Starting up other appliance");
		System.getProperties().put(ConfigService.ARCHAPPL_APPLIANCES, "../webapps/mgmt/WEB-INF/classes/failover_other.xml"); 
		createAndStartTomcatInstance(testName, "other_appliance", ConfigServiceForTests.RETRIEVAL_TEST_PORT+4, DEFAULT_SERVER_STARTUP_PORT+4);
		logger.info("Done starting up other appliance");
	}
	

	public void tearDown() throws Exception {
		for(Process process : watchedProcesses) {
			// First try to kill the process cleanly
			long pid = process.pid();
			String cmd = "kill -s SIGINT " + pid;
			logger.info("Sending a signal using " + cmd);
			Runtime.getRuntime().exec(cmd);
			try {Thread.sleep(15*1000);} catch(Exception ex) {}
			if(process.isAlive()) {
				logger.warn("Tomcat process did not stop propoerly within time. Forcibly stopping it.");
				process.destroyForcibly();
				try {Thread.sleep(3*60*1000);} catch(Exception ex) {}				
			}
		}
		
		for(File cleanupFolder : cleanupFolders) { 
			logger.debug("Cleaning up folder " + cleanupFolder.getAbsolutePath());
			FileUtils.deleteDirectory(cleanupFolder);
		}
	}
	
	
	private void createAndStartTomcatInstance(String testName, final String applianceName, int port, int startupPort) throws IOException { 
		File workFolder = makeTomcatFolders(testName, applianceName, port, startupPort);
		File logsFolder = new File(workFolder, "logs");
		assert(logsFolder.exists());
		
		ProcessBuilder pb = new ProcessBuilder(System.getenv("TOMCAT_HOME") + File.separator + "bin" + File.separator + "catalina.sh", "run");
		createEnvironment(testName, applianceName, pb.environment());
		pb.directory(logsFolder);
		pb.redirectErrorStream(true);
		pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
		Process p = pb.start();
		watchedProcesses.add(p);
		
		final CountDownLatch latch = new CountDownLatch(1);
		
		final LineReader li = new LineReader(new InputStreamReader(p.getInputStream()));
		Thread t = new Thread() {
			@Override
			public void run() {
				try {
					while(p.isAlive()) {
						String msg = li.readLine();
						if(msg != null) { 
							System.out.println(applianceName + "-->" + msg); 
							if(msg.contains("All components in this appliance have started up")) {
								logger.info(applianceName + " has started up.");
								latch.countDown();
							}
						} else {
							try {Thread.sleep(500);} catch(Exception ex) {}							
						}
					}
				} catch(Exception ex) {
					logger.error("Exception starting Tomcat", ex);
				}
			}
		};
		t.start();
		
		// We wait for some time to make sure the server started up
		try { latch.await(2, TimeUnit.MINUTES); } catch(InterruptedException ex) {} 
		logger.info("Done starting " + applianceName + " Releasing latch");
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
		FileUtils.copyFile(new File("log4j.properties"), new File(logsFolder, "log4j.properties"));
		File tempFolder = new File(workFolder, "temp");
		tempFolder.mkdir();
		
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
	
	
	private Map<String, String> createEnvironment(String testName, String applianceName, Map<String, String> environment) { 
		environment.putAll(System.getenv());
		environment.remove("CLASSPATH");
		environment.put("CATALINA_HOME", System.getenv("TOMCAT_HOME"));
		File workFolder = new File("tomcat_" + testName + File.separator + applianceName);
		assert(workFolder.exists());
		environment.put("CATALINA_BASE", workFolder.getAbsolutePath());
		
		environment.put(ConfigService.ARCHAPPL_CONFIGSERVICE_IMPL, ConfigServiceForTests.class.getName());
		environment.put(DefaultConfigService.SITE_FOR_UNIT_TESTS_NAME, DefaultConfigService.SITE_FOR_UNIT_TESTS_VALUE);

		environment.put(ConfigService.ARCHAPPL_MYIDENTITY, applianceName);
		if(!System.getProperties().containsKey(ConfigService.ARCHAPPL_APPLIANCES)) { 
			environment.put(ConfigService.ARCHAPPL_APPLIANCES, new File("./src/sitespecific/tests/classpathfiles/appliances.xml").getAbsolutePath());
		} else { 
			environment.put(ConfigService.ARCHAPPL_APPLIANCES, (String)System.getProperties().get(ConfigService.ARCHAPPL_APPLIANCES));
		}
		
		if(!System.getProperties().containsKey(ConfigService.ARCHAPPL_PERSISTENCE_LAYER) || System.getProperties().get(ConfigService.ARCHAPPL_PERSISTENCE_LAYER).equals(InMemoryPersistence.class.getName())) {
			environment.put(ConfigService.ARCHAPPL_PERSISTENCE_LAYER, "org.epics.archiverappliance.config.persistence.InMemoryPersistence");
		} else { 
			String persistenceFile = (String) System.getProperties().get(JDBM2Persistence.ARCHAPPL_JDBM2_FILENAME);
			logger.info("Persistence layer is provided by " + System.getProperties().get(ConfigService.ARCHAPPL_PERSISTENCE_LAYER));
			assert(persistenceFile != null);
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

		return environment;
	}
	
	
	private static void overrideEnvWithSystemProperty(Map<String, String> environment, String key) { 
		if(System.getProperties().containsKey(key)) { 
			String value = (String) System.getProperties().get(key);
			logger.debug("Overriding " + key + " from the system properties " + value);
			environment.put(key, value);
		}

	}
	
}
