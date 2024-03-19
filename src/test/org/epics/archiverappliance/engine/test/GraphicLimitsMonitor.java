package org.epics.archiverappliance.engine.test;

import gov.aps.jca.CAException;
import gov.aps.jca.Channel;
import gov.aps.jca.Context;
import gov.aps.jca.JCALibrary;
import gov.aps.jca.TimeoutException;
import gov.aps.jca.configuration.Configuration;
import gov.aps.jca.configuration.ConfigurationException;
import gov.aps.jca.configuration.DefaultConfigurationBuilder;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.DBR_GR_Double;
import gov.aps.jca.event.MonitorEvent;
import gov.aps.jca.event.MonitorListener;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.engine.epics.JCAConfigGen;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Establish a graphic limits monitor on a known PV and print each change.
 * @author mshankar
 *
 */
public class GraphicLimitsMonitor {
	private Context context = null;
	private JCALibrary jca =null;

	private void initialize() throws CAException, SAXException, IOException, ConfigurationException, ConfigException {
		ConfigServiceForTests configService = new ConfigServiceForTests(-1);
		jca = JCALibrary.getInstance();
		ByteArrayInputStream bis = JCAConfigGen.generateJCAConfig(configService);
		DefaultConfigurationBuilder configBuilder = new DefaultConfigurationBuilder();
		Configuration configuration;
		configuration = configBuilder.build(bis);
		context = jca.createContext(configuration);
	}

	public void establishGraphicLimitsMonitorAndPrint(String pvName) throws CAException, IllegalStateException, TimeoutException, SAXException, IOException, ConfigurationException, ConfigException {
		initialize();
		Channel channel = context.createChannel(pvName);
		context.pendIO(3.0);
		channel.addMonitor(DBRType.GR_DOUBLE, 1, 2 /* DBE_Archive */, new MonitorListener() {
			
			@Override
			public void monitorChanged(MonitorEvent event) {
				DBR_GR_Double dbr = (DBR_GR_Double) event.getDBR();
				dbr.printInfo(System.out);
			}
		});
		context.pendIO(3.0);
	}

	 public static void main(String[] args) throws Exception {
		 new GraphicLimitsMonitor().establishGraphicLimitsMonitorAndPrint(args[0]);
	 }
}