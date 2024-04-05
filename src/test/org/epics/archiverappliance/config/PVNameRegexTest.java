package org.epics.archiverappliance.config;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Small unit test to make sure the regex that checks for valid pv names does something reasonable
 * @author mshankar
 *
 */
public class PVNameRegexTest {

    @Test
    public void test() {
        assertTrue("Checking for null pvName failed", !PVNames.isValidPVName(null));
        assertTrue("Checking for empty pvName failed", !PVNames.isValidPVName(""));
        String pvName = "";
        pvName = "archappl:arch:sine";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        pvName = "archappl:arch:sine1";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        pvName = "archappl:arch:sine1.HIHI";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        pvName = "archappl:arch:sine100";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        pvName = "archappl_arch_sine";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        pvName = "archappl-arch-sine";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        pvName = "archappl+arch+sine";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        pvName = "archappl[arch]sine";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        pvName = "archappl<arch>sine";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        pvName = "archappl:arch;sine";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        pvName = "archappl:ar/h:sine";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        // Add for FRIB
        pvName = "P#6:000357";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));
        // Add for BNL
        pvName = "LN-AM{RadMon:1}Lvl:Raw-I";
        assertTrue("Valid pvName is deemed invalid " + pvName, PVNames.isValidPVName(pvName));

        // Let's test some invalid ones
        pvName = "\"archappl:arch:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:arch:sine\"";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar'h:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar!h:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar$h:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar%h:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar*h:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar\\h:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar|h:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar'h:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar?h:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar&h:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:ar@ch:sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:(arch):sine";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
        pvName = "archappl:arch:sine=1.0";
        assertTrue("Invalid pvName is deemed valid " + pvName, !PVNames.isValidPVName(pvName));
    }
}
