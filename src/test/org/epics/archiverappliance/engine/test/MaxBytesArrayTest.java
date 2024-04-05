package org.epics.archiverappliance.engine.test;

import gov.aps.jca.Context;
import gov.aps.jca.JCALibrary;
import gov.aps.jca.configuration.Configuration;
import gov.aps.jca.configuration.DefaultConfigurationBuilder;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.epics.JCAConfigGen;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

public class MaxBytesArrayTest {

    @Test
    public void testMaxBytesArray() throws Exception {
        ConfigService configService = new ConfigServiceForTests(-1);
        ByteArrayInputStream bis = JCAConfigGen.generateJCAConfig(configService);
        JCALibrary jca = JCALibrary.getInstance();
        DefaultConfigurationBuilder configBuilder = new DefaultConfigurationBuilder();
        Configuration configuration;

        configuration = configBuilder.build(bis);

        Context jca_context = jca.createContext(configuration);
        jca_context.printInfo();
    }
}
