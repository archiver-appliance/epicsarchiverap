package org.epics.archiverappliance.mgmt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.mgmt.policy.ExecutePolicy;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class PolicyExecutionTest {
    Logger logger = LogManager.getLogger(PolicyExecutionTest.class);

    @Test
    public void testSimplePolicyExecution() throws Exception {
        DefaultConfigService configService = new ConfigServiceForTests(-1);
        HashMap<String, Object> pvInfo = new HashMap<String, Object>();
        pvInfo.put("eventRate", 1.0f);
        pvInfo.put("storageRate", 1.0f);
        pvInfo.put("RTYP", "ai");
        try (ExecutePolicy executePolicy = new ExecutePolicy(configService)) {
            PolicyConfig policyConfig = executePolicy.computePolicyForPV("test", pvInfo);
            Assertions.assertNotNull(policyConfig, "policyConfig is null");
            Assertions.assertTrue(
                    policyConfig.getDataStores() != null && policyConfig.getDataStores().length > 1,
                    "dataStores is null");
        }
    }

    @Test
    public void testForLeaks() throws Exception {
        DefaultConfigService configService = new ConfigServiceForTests(-1);
        for (int i = 0; i < 10000; i++) {
            HashMap<String, Object> pvInfo = new HashMap<String, Object>();
            pvInfo.put("eventRate", 1.0f);
            pvInfo.put("storageRate", 1.0f);
            pvInfo.put("RTYP", "ai");
            try (ExecutePolicy executePolicy = new ExecutePolicy(configService)) {
                PolicyConfig policyConfig = executePolicy.computePolicyForPV("test" + i, pvInfo);
                Assertions.assertNotNull(policyConfig, "policyConfig is null");
                Assertions.assertTrue(
                        policyConfig.getDataStores() != null && policyConfig.getDataStores().length > 1,
                        "dataStores is null");
            }
        }
    }
}
