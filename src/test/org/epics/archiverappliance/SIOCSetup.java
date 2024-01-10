package org.epics.archiverappliance;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedOutputStream;
import java.io.PrintWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.xml.sax.SAXException;

import gov.aps.jca.CAException;
import gov.aps.jca.TimeoutException;
import gov.aps.jca.configuration.ConfigurationException;

/**
 * Sets up an SIOC
 * @author mshankar
 *
 */
public class SIOCSetup {
	private static Logger logger = LogManager.getLogger(SIOCSetup.class.getName());
	Process watchedProcess;
	PipedOutputStream osforstdin = new PipedOutputStream();
	PrintWriter writerforstdin = new PrintWriter(new OutputStreamWriter(osforstdin));
	String prefix;
	public SIOCSetup() {
		this("");
	}
	public SIOCSetup(String prefix) {
		this.prefix = prefix;
	}
	/**
	 * We start the SIOC with the UnitTestPVs.db. 
	 * This assumes that are run in the root folder of the workspace.
	 * This assumption can be changed; please let me know if this behavior is needed.
	 */
	public void startSIOCWithDefaultDB() throws Exception {
		String path = "src/resources/test/UnitTestPVs.db";
		File f = new File(path);
		if(!f.exists()) throw new IOException("Cannot find SIOC database file " + path);
		
		logger.info("Starting SIOC with DB file " + f.getAbsolutePath());
		
		ProcessBuilder pb = new ProcessBuilder("softIocPVA", "-m", "P=" + prefix, "-d", f.getAbsolutePath());
		pb.redirectErrorStream(true);
		pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
		pb.redirectInput(ProcessBuilder.Redirect.PIPE);
		watchedProcess = pb.start();
	}
	
	public void stopSIOC() throws Exception {
		PrintWriter writer = new PrintWriter(watchedProcess.getOutputStream());
		writer.println("exit");
		writer.flush();
		writer.close();
		try {Thread.sleep(5*1000);} catch(Exception ex) {}
		if(watchedProcess.isAlive()) {
			watchedProcess.destroyForcibly();
		}
	}
	
	
	public static void caput(String pvName, double value) throws IllegalStateException, CAException, TimeoutException, SAXException, IOException, ConfigurationException, ConfigException {
		new PVCaPut().caPut(pvName,value);
	}
	
	public static void caput(String pvName, String value) throws IllegalStateException, CAException, TimeoutException, SAXException, IOException, ConfigurationException, ConfigException {
		new PVCaPut().caPut(pvName,value);
	}

}
