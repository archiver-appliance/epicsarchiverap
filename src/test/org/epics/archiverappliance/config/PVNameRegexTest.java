package org.epics.archiverappliance.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Small unit test to make sure the regex that checks for valid pv names does something reasonable
 * @author mshankar
 *
 */
public class PVNameRegexTest {

    @Test
    public void test() {
        Assertions.assertTrue(!PVNames.isValidPVName(null), "Checking for null pvName failed");
        Assertions.assertTrue(!PVNames.isValidPVName(""), "Checking for empty pvName failed");
        String pvName = "";
        pvName = "archappl:arch:sine";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        pvName = "archappl:arch:sine1";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        pvName = "archappl:arch:sine1.HIHI";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        pvName = "archappl:arch:sine100";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        pvName = "archappl_arch_sine";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        pvName = "archappl-arch-sine";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        pvName = "archappl+arch+sine";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        pvName = "archappl[arch]sine";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        pvName = "archappl<arch>sine";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        pvName = "archappl:arch;sine";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        pvName = "archappl:ar/h:sine";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        // Add for FRIB
        pvName = "P#6:000357";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);
        // Add for BNL
        pvName = "LN-AM{RadMon:1}Lvl:Raw-I";
        Assertions.assertTrue(PVNames.isValidPVName(pvName), "Valid pvName is deemed invalid " + pvName);

        // Let's test some invalid ones
        pvName = "\"archappl:arch:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:arch:sine\"";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar'h:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar!h:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar$h:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar%h:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar*h:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar\\h:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar|h:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar'h:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar?h:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar&h:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:ar@ch:sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:(arch):sine";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
        pvName = "archappl:arch:sine=1.0";
        Assertions.assertTrue(!PVNames.isValidPVName(pvName), "Invalid pvName is deemed valid " + pvName);
    }
}
