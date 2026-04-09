package org.epics.archiverappliance.mgmt.pva;

import org.junit.jupiter.api.Tag;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * A test suite for the basic pvAccess Archiver service management operations.
 *
 * <p>Shared infrastructure (embedded Tomcat + SIOC) is managed by {@link PvaTestSetupExtension},
 * registered on each member class. The JUnit Platform Suite engine does not invoke
 * {@code @BeforeAll}/{@code @AfterAll} on the suite class itself, so lifecycle hooks must live in
 * an extension.
 *
 * @author Kunal Shroff
 */
@Tag("integration")
@Tag("localEpics")
@Suite
@SelectClasses({
    PvaSuiteTstGetAll.class,
    PvaSuiteTstMgmtServiceStartup.class,
    PvaSuiteTstGetApplianceInfo.class,
    PvaSuiteTstArchivePV.class
})
public class PvaTest {

    protected static final String pvPrefix = PvaTest.class.getSimpleName();
}
