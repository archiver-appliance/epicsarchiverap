package org.epics.archiverappliance;

import gov.aps.jca.CAException;
import gov.aps.jca.TimeoutException;
import gov.aps.jca.configuration.ConfigurationException;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.xml.sax.SAXException;

/**
 * Sets up an SIOC
 * @author mshankar
 *
 */
public class SIOCSetup {
	private static Logger logger = Logger.getLogger(SIOCSetup.class.getName());
	CommandLine cmdLine;
	DefaultExecuteResultHandler resultHandler;
	Executor executor;
	ExecuteWatchdog watchdog;
	PipedOutputStream osforstdin = new PipedOutputStream();
	PrintWriter writerforstdin = new PrintWriter(new OutputStreamWriter(osforstdin));
	
	/**
	 * We start the SIOC with the UnitTestPVs.db. 
	 * This assumes that are run in the root folder of the workspace.
	 * This assumption can be changed; please let me know if this behavior is needed.
	 */
	public void startSIOCWithDefaultDB() throws Exception {
		File f = new File("./src/test/org/epics/archiverappliance/UnitTestPVs.db");
		if(!f.exists()) throw new IOException("Cannot find SIOC database file ./src/test/org/epics/archiverappliance/UnitTestPVs.db");
		
		logger.info("Starting SIOC with DB file " + f.getAbsolutePath());
		
		cmdLine = new CommandLine("softIoc");
		cmdLine.addArgument("-d");
		cmdLine.addArgument("${file}", true);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("file", f);
		cmdLine.setSubstitutionMap(map);

		resultHandler = new DefaultExecuteResultHandler();
		PumpStreamHandler pump = new PumpStreamHandler(System.out, System.err, new PipedInputStream(osforstdin));

		watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
		executor = new DefaultExecutor();
		executor.setExitValue(1);
		executor.setWatchdog(watchdog);
		executor.setStreamHandler(pump);
		executor.execute(cmdLine, resultHandler);

		// Show all the records in the sioc.
		// writerforstdin.println("dbl");
		
	
	}
	
	public void stopSIOC() throws Exception {
		writerforstdin.println("exit");
		writerforstdin.close();
		
		// We brutally kill the process. 
		watchdog.destroyProcess();
		// some time later the result handler callback was invoked so we
		// can safely request the exit value
		resultHandler.waitFor();
	}
	
	
	public static void caput(String pvName, double value) throws IllegalStateException, CAException, TimeoutException, SAXException, IOException, ConfigurationException, ConfigException {
		new PVCaPut().caPut(pvName,value);
	}
	
	public static void caput(String pvName, String value) throws IllegalStateException, CAException, TimeoutException, SAXException, IOException, ConfigurationException, ConfigException {
		new PVCaPut().caPut(pvName,value);
	}

}
