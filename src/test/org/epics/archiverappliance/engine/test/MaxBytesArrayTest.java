package org.epics.archiverappliance.engine.test;

import gov.aps.jca.Context;
import gov.aps.jca.JCALibrary;
import gov.aps.jca.configuration.Configuration;
import gov.aps.jca.configuration.DefaultConfigurationBuilder;

import java.io.ByteArrayInputStream;
import java.io.File;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.epics.JCAConfigGen;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MaxBytesArrayTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMaxBytesArray() throws Exception {
		ConfigService configService = new ConfigServiceForTests(new File("./bin"));
		ByteArrayInputStream bis = JCAConfigGen.generateJCAConfig(configService);
		JCALibrary jca = JCALibrary.getInstance();
		DefaultConfigurationBuilder configBuilder = new DefaultConfigurationBuilder();
		Configuration configuration;

		configuration = configBuilder.build(bis);

		Context jca_context = jca.createContext(configuration);
		jca_context.printInfo();
	}

}
