/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.epics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.exception.ConfigException;

/**
 * Read the system environment and generate a JCA config stream
 * As of today, this seems to be a more visible way to configure JCA for this project than hidden files or other schemes
 * We'll see how this stands the test of time.
 * 
 * @author mshankar
 *
 */
public class JCAConfigGen {
	private static final String JCA_CONFIG_GEN_USE_CAJ = "org.epics.archiverappliance.engine.epics.JCAConfigGen.useCAJ";
	private static final Logger configlogger = Logger.getLogger("config." + JCAConfigGen.class.getName());
	
	/**
	 * Use environment vars to generate a JCA config that can be handed off to a JCA DefaultConfigurationBuilder
	 * @param configService ConfigService
	 * @return ByteArrayInputStream  &emsp;
	 * @throws ConfigException &emsp;
	 */
	public static ByteArrayInputStream generateJCAConfig(ConfigService configService) throws ConfigException {
		String JCACAJContext = "gov.aps.jca.jni.SingleThreadedContext";
		
		Properties props = configService.getInstallationProperties();
		configlogger.info("JCA/CAJ prop from archappl.properties is " + props.get(JCA_CONFIG_GEN_USE_CAJ));
		if(props != null 
				&& props.containsKey(JCA_CONFIG_GEN_USE_CAJ) 
				&& Boolean.parseBoolean((String) props.get(JCA_CONFIG_GEN_USE_CAJ))) { 
			JCACAJContext = "com.cosylab.epics.caj.CAJContext";
		} else {
			configlogger.fatal("We no longer support the native version of JCA in the EPICS archiver appliance. Please use CAJ instead.");
			throw new ConfigException("We no longer support the native version of JCA in the EPICS archiver appliance. Please use CAJ instead.");
		}
		
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		PrintWriter out = new PrintWriter(bos);
		out.println("<context class=\"" + JCACAJContext + "\">"); 
		out.println("  <preemptive_callback>true</preemptive_callback>");
		
		String EPICS_CA_ADDR_LIST = System.getenv("EPICS_CA_ADDR_LIST");
		if(EPICS_CA_ADDR_LIST == null) {
			configlogger.info("Setting EPICS_CA_ADDR_LIST to an empty string as it is not defined in the environment.");
			EPICS_CA_ADDR_LIST = "";
		}
		out.println("  <addr_list>" + EPICS_CA_ADDR_LIST + "</addr_list>");
		
		String EPICS_CA_AUTO_ADDR_LIST = System.getenv("EPICS_CA_AUTO_ADDR_LIST");
		if(EPICS_CA_AUTO_ADDR_LIST != null) {
			if(EPICS_CA_AUTO_ADDR_LIST.equalsIgnoreCase("yes")) {
				EPICS_CA_AUTO_ADDR_LIST = "true";
			} else if(EPICS_CA_AUTO_ADDR_LIST.equalsIgnoreCase("no")) { 
				EPICS_CA_AUTO_ADDR_LIST = "false";
			} else { 
				EPICS_CA_AUTO_ADDR_LIST = "false";
			}
		} else { 
			// Per the Channel Access reference manual, this should default to true if the variable is unset
			// LNLS also relies on this.
			EPICS_CA_AUTO_ADDR_LIST = "true";
		}
		out.println("  <auto_addr_list>" + EPICS_CA_AUTO_ADDR_LIST + "</auto_addr_list>");
		
		String EPICS_CA_CONN_TMO = System.getenv("EPICS_CA_CONN_TMO");
		if(EPICS_CA_CONN_TMO == null) EPICS_CA_CONN_TMO = "30.0";
		out.println("  <connection_timeout>" + EPICS_CA_CONN_TMO + "</connection_timeout>");
		
		String EPICS_CA_BEACON_PERIOD = System.getenv("EPICS_CA_BEACON_PERIOD");
		if(EPICS_CA_BEACON_PERIOD == null) EPICS_CA_BEACON_PERIOD = "30.0";
		out.println("  <beacon_period>" + EPICS_CA_BEACON_PERIOD + "</beacon_period>");
		
		String EPICS_CA_REPEATER_PORT = System.getenv("EPICS_CA_REPEATER_PORT");
		if(EPICS_CA_REPEATER_PORT == null) EPICS_CA_REPEATER_PORT = "5065";
		out.println("  <repeater_port>" + EPICS_CA_REPEATER_PORT + "</repeater_port>");

		String EPICS_CA_SERVER_PORT = System.getenv("EPICS_CA_SERVER_PORT");
		if(EPICS_CA_SERVER_PORT == null) EPICS_CA_SERVER_PORT = "5064";
		out.println("  <server_port>" + EPICS_CA_SERVER_PORT + "</server_port>");
		
		String EPICS_CA_MAX_ARRAY_BYTES = System.getenv("EPICS_CA_MAX_ARRAY_BYTES");
		if(EPICS_CA_MAX_ARRAY_BYTES == null) EPICS_CA_MAX_ARRAY_BYTES = "30.0";
		out.println("  <max_array_bytes>" + EPICS_CA_MAX_ARRAY_BYTES + "</max_array_bytes>");
		
		String dispatcher = props.getProperty("org.epics.archiverappliance.engine.epics.JCAConfigGen.dispatcher", "gov.aps.jca.event.QueuedEventDispatcher");
		out.println("  <event_dispatcher class=\"" + dispatcher + "\"/>");
		out.println("</context>");
		out.close();
		
		byte[] cfgbytes = bos.toByteArray();
		try {
			configlogger.info("JCA Configuration:\n" + new String(cfgbytes, "UTF-8"));
		} catch(UnsupportedEncodingException ex) {
			// This is a JVM that does not support UTF-8. It is unlikely the rest of this product will work
			configlogger.fatal(ex);
		}
		ByteArrayInputStream bis = new ByteArrayInputStream(cfgbytes);
		return bis;
	}
}
