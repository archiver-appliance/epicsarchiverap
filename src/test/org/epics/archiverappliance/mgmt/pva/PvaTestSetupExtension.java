package org.epics.archiverappliance.mgmt.pva;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 extension that starts a shared embedded Tomcat and SIOC once for all PVA suite tests,
 * and shuts them down when the test plan ends.
 *
 * <p>Registered on each PVA suite test class via {@code @ExtendWith}. Uses the JUnit
 * root store so the infrastructure is initialised exactly once regardless of how many test classes
 * are run.
 */
public class PvaTestSetupExtension implements BeforeAllCallback {

    private static final String STORE_KEY = PvaTestSetupExtension.class.getName();

    @Override
    public void beforeAll(ExtensionContext context) {
        context.getRoot().getStore(ExtensionContext.Namespace.GLOBAL).getOrComputeIfAbsent(STORE_KEY, key -> {
            try {
                return new PvaResources();
            } catch (Exception e) {
                throw new RuntimeException("Failed to start PVA test infrastructure", e);
            }
        });
    }

    private static class PvaResources implements ExtensionContext.Store.CloseableResource {
        private static final Logger logger = LogManager.getLogger(PvaResources.class);

        private final TomcatSetup tomcatSetup = new TomcatSetup();
        private final SIOCSetup siocSetup = new SIOCSetup();

        PvaResources() throws Exception {
            siocSetup.startSIOCWithDefaultDB();
            tomcatSetup.setUpWebApps("PvaTest");
            logger.info("PVA test infrastructure started.");
        }

        @Override
        public void close() throws Throwable {
            logger.info("Tearing down PVA test infrastructure.");
            tomcatSetup.tearDown();
            siocSetup.stopSIOC();
        }
    }
}
