package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.mgmt.policy.ExecutePolicy;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PolicyExecutionTest {
    Logger logger = LogManager.getLogger(PolicyExecutionTest.class);

    @Test
    public void testSimplePolicyExecution() throws Exception {
        DefaultConfigService configService = new ConfigServiceForTests(new File("./src/sitespecific/tests/classpathfiles"));
        HashMap<String, Object> pvInfo = new HashMap<String, Object>();
        pvInfo.put("eventRate", 1.0f);
        pvInfo.put("storageRate", 1.0f);
        pvInfo.put("RTYP", "ai");
        try (ExecutePolicy executePolicy = new ExecutePolicy(configService)) {
            PolicyConfig policyConfig = executePolicy.computePolicyForPV("test", pvInfo);
            assertNotNull("policyConfig is null", policyConfig);
            assertTrue("dataStores is null", policyConfig.getDataStores() != null && policyConfig.getDataStores().length > 1);
        }
    }

    @Test
    public void testForLeaks() throws Exception {
        DefaultConfigService configService = new ConfigServiceForTests(new File("./src/sitespecific/tests/classpathfiles"));
        for (int i = 0; i < 10000; i++) {
            HashMap<String, Object> pvInfo = new HashMap<String, Object>();
            pvInfo.put("eventRate", 1.0f);
            pvInfo.put("storageRate", 1.0f);
            pvInfo.put("RTYP", "ai");
            try (ExecutePolicy executePolicy = new ExecutePolicy(configService)) {
                PolicyConfig policyConfig = executePolicy.computePolicyForPV("test" + i, pvInfo);
                assertNotNull("policyConfig is null", policyConfig);
                assertTrue("dataStores is null", policyConfig.getDataStores() != null && policyConfig.getDataStores().length > 1);
            }
        }
    }

}
