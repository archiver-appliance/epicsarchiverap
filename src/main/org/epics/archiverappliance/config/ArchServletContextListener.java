/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;

/**
 * This is a ServletContextListener expected to be registered in web.xml that serves as the source of dependency injection.
 * For now, we hardcode the entries, but later these can be picked up from a config file/JNDI if needed.
 * @author mshankar
 *
 */
public class ArchServletContextListener implements ServletContextListener {
    private static final Logger logger = LogManager.getLogger(ArchServletContextListener.class);
    private static final Logger configlogger = LogManager.getLogger("config." + ArchServletContextListener.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        // This should hopefully trigger the log4j2 initialization
        configlogger.info("Initializing the ArchServletContextListener");
        try {
            String configServiceImplClassName = System.getProperty(ConfigService.ARCHAPPL_CONFIGSERVICE_IMPL);
            if (configServiceImplClassName == null) {
                configServiceImplClassName = System.getenv(ConfigService.ARCHAPPL_CONFIGSERVICE_IMPL);
            }

            ConfigService configService = null;
            if (configServiceImplClassName == null) {
                configlogger.info("Using the default config service implementation");
                configService = new DefaultConfigService();
            } else {
                configlogger.info("Using " + configServiceImplClassName + " as the config service implementation");
                configService = (ConfigService) Class.forName(configServiceImplClassName)
                        .getConstructor()
                        .newInstance();
            }

            configService.initialize(sce.getServletContext());
            sce.getServletContext().setAttribute(ConfigService.CONFIG_SERVICE_NAME, configService);
        } catch (Exception e) {
            logger.fatal("Exception initializing config service ", e);
            try {
                sce.getServletContext().setAttribute(ConfigService.CONFIG_SERVICE_NAME + ".exception", e.getMessage());
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                PrintWriter stackTraceOut = new PrintWriter(bos, true);
                e.printStackTrace(stackTraceOut);
                bos.flush();
                bos.close();
                sce.getServletContext().setAttribute(ConfigService.CONFIG_SERVICE_NAME + ".stacktrace", bos.toString());
            } catch (Exception ex) {
                logger.warn("Exception setting reason for failure", ex);
            }
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        ConfigService configService =
                (ConfigService) sce.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
        try {
            configService.shutdownNow();
        } catch (Throwable t) {
            logger.warn("Exception shutting down config service using shutdown hook ", t);
        }
    }
}
