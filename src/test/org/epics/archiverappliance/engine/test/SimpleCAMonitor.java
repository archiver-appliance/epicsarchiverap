package org.epics.archiverappliance.engine.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.JCA2ArchDBRType;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.pv.DBR_Helper;
import org.epics.archiverappliance.engine.pv.EPICS_V3_PV.MonitorMask;

import edu.stanford.slac.archiverappliance.PB.data.PBTypeSystem;
import gov.aps.jca.Channel;
import gov.aps.jca.Context;
import gov.aps.jca.JCALibrary;
import gov.aps.jca.configuration.Configuration;
import gov.aps.jca.configuration.DefaultConfigurationBuilder;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBR_TIME_Double;
import gov.aps.jca.event.ConnectionEvent;
import gov.aps.jca.event.ConnectionListener;
import gov.aps.jca.event.GetEvent;
import gov.aps.jca.event.GetListener;
import gov.aps.jca.event.MonitorEvent;
import gov.aps.jca.event.MonitorListener;

/**
 * Simple command line utility to monitor and print a Channel Access PV. May be useful for testing and debugging.
 * @author mshankar
 *
 */
public class SimpleCAMonitor {
	
	public static final ByteArrayInputStream generateJCAConfiguration() {
		String JCACAJContext = "com.cosylab.epics.caj.CAJContext";
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		PrintWriter out = new PrintWriter(bos);
		out.println("<context class=\"" + JCACAJContext + "\">"); 
		out.println("  <preemptive_callback>true</preemptive_callback>");
		
		String EPICS_CA_ADDR_LIST = System.getenv("EPICS_CA_ADDR_LIST");
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
		
		String dispatcher = "gov.aps.jca.event.QueuedEventDispatcher";
		out.println("  <event_dispatcher class=\"" + dispatcher + "\"/>");
		out.println("</context>");
		out.close();
		
		byte[] cfgbytes = bos.toByteArray();
		ByteArrayInputStream bis = new ByteArrayInputStream(cfgbytes);
		return bis;	
	}
	
	public static void main(String[] args) throws Exception {
		String pvName = args[0];
		final CountDownLatch latch0 = new CountDownLatch(1), latch1 = new CountDownLatch(1), latch2 = new CountDownLatch(1);
		System.out.println("Connecting to PV " + pvName);
		ByteArrayInputStream bis = generateJCAConfiguration();
		JCALibrary jca = JCALibrary.getInstance();
		DefaultConfigurationBuilder configBuilder = new DefaultConfigurationBuilder();
		Configuration configuration;
		configuration = configBuilder.build(bis);
		Context jca_context = jca.createContext(configuration);
		Channel channel = jca_context.createChannel(pvName, new ConnectionListener() {			
			@Override
			public void connectionChanged(ConnectionEvent ev) {
				System.out.println("Connection event");
				if (ev.isConnected()) {
					System.out.println("Connected to the PV");
					latch0.countDown();
				}
			}
		});
		
		latch0.await();
		channel.get(new GetListener() {
			@Override
			public void getCompleted(GetEvent ev) {
				System.out.println("Initial get from the PV");
				latch1.countDown();
			}
		});
		jca_context.pendIO(3.0);
		
		latch1.await();
		PBTypeSystem typeSystem = new PBTypeSystem();
		channel.addMonitor(DBR_Helper.getTimeType(false, channel.getFieldType()), channel.getElementCount(), MonitorMask.ARCHIVE.getMask(), new MonitorListener() {
			@Override
			public void monitorChanged(MonitorEvent ev) {
				System.out.println("Got monitor");
				try {
					DBR dbr = ev.getDBR();
					DBR_TIME_Double dbrd = (DBR_TIME_Double) dbr;
					System.out.println(dbrd.getTimeStamp().nsec());
							
					ArchDBRTypes generatedDBRType = JCA2ArchDBRType.valueOf(dbr);
					DBRTimeEvent dbrtimeevent = typeSystem.getJCADBRConstructor(generatedDBRType).newInstance(dbr);
					System.out.print(TimeUtils.convertToHumanReadableString(dbrtimeevent.getEventTimeStamp()));
					System.out.print("\t");
					System.out.println(dbrtimeevent.getSampleValue().toJSONString());
				} catch(Exception ex) { ex.printStackTrace(System.err); }
			}
		});
		System.out.println("Added monitor");
		jca_context.pendIO(3.0);

		latch2.await(10000, TimeUnit.SECONDS); 
	}

}
