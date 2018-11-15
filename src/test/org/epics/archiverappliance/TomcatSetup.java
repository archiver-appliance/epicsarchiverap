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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
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
	private static Logger logger = Logger.getLogger(TomcatSetup.class.getName());
	LinkedList<ExecuteWatchdog> executorWatchDogs = new LinkedList<ExecuteWatchdog>();
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


	public void tearDown() throws Exception {
		for(ExecuteWatchdog watchdog : executorWatchDogs) {
			// We brutally kill the process
			watchdog.destroyProcess();
			try {Thread.sleep(10*1000);} catch(Exception ex) {}
			assert(watchdog.killedProcess());
		}
		
		for(File cleanupFolder : cleanupFolders) { 
			logger.debug("Cleaning up folder " + cleanupFolder.getAbsolutePath());
			FileUtils.deleteDirectory(cleanupFolder);
		}
		
		// So many timeouts; if we are running multiple tests in sequence; the Tomcat socket seems to wind up in a TIME_WAIT site for about 2 minutes.
		// Setting the net.netfilter.nf_conntrack_tcp_timeout_time_wait (using sysctl) seems to hurt other behaviours...
		// Sigh!
		try {Thread.sleep(3*60*1000);} catch(Exception ex) {}
	}
	
	
	private void createAndStartTomcatInstance(String testName, final String applianceName, int port, int startupPort) throws IOException { 
		File workFolder = makeTomcatFolders(testName, applianceName, port, startupPort);
		HashMap<String, String> environment = createEnvironment(testName, applianceName);
		File logsFolder = new File(workFolder, "logs");
		assert(logsFolder.exists());
		
		CommandLine cmdLine = new CommandLine(System.getenv("TOMCAT_HOME") + File.separator + "bin" + File.separator + "catalina.sh");
		cmdLine.addArgument("run");
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
		final CountDownLatch latch = new CountDownLatch(1);
		PumpStreamHandler pump = new PumpStreamHandler(new LogOutputStream() {
			@Override
			protected void processLine(String msg, int level) {
				if(msg != null && msg.contains("All components in this appliance have started up")) {
					logger.info(applianceName + " has started up.");
					latch.countDown();
				}
				System.out.println(msg);
			}
		}, System.err);

		ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
		Executor executor = new DefaultExecutor();
		executor.setExitValue(1);
		executor.setWatchdog(watchdog);
		executor.setStreamHandler(pump);
		executor.setWorkingDirectory(logsFolder);
		executor.execute(cmdLine, environment, resultHandler);
		executorWatchDogs.add(watchdog);
		
		// We wait for some time to make sure the server started up
		try { latch.await(2, TimeUnit.MINUTES); } catch(InterruptedException ex) {} 
		logger.info("Done starting tomcat for the testing.");

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
		FileUtils.copyFile(new File("../mgmt.war"), new File(webAppsFolder, "mgmt.war"));
		FileUtils.copyFile(new File("../retrieval.war"), new File(webAppsFolder, "retrieval.war"));
		FileUtils.copyFile(new File("../etl.war"), new File(webAppsFolder, "etl.war"));
		FileUtils.copyFile(new File("../engine.war"), new File(webAppsFolder, "engine.war"));
		
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
	
	
	private HashMap<String, String> createEnvironment(String testName, String applianceName) { 
		HashMap<String, String> environment = new HashMap<String, String>();
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
	
	
	private static void overrideEnvWithSystemProperty(HashMap<String, String> environment, String key) { 
		if(System.getProperties().containsKey(key)) { 
			String value = (String) System.getProperties().get(key);
			logger.debug("Overriding " + key + " from the system properties " + value);
			environment.put(key, value);
		}

	}
	
}
