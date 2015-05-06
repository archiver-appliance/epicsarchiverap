package org.epics.archiverappliance.mgmt;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;

import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.DefaultConfigService;
import org.epics.archiverappliance.mgmt.policy.ExecutePolicy;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PolicyExecutionTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSimplePolicyExecution() throws Exception {
		DefaultConfigService configService = new ConfigServiceForTests(new File("./src/sitespecific/tests/classpathfiles"));
		try(InputStream is = configService.getPolicyText()) {
			HashMap<String, Object> pvInfo = new HashMap<String, Object>();
			pvInfo.put("eventRate", new Float(1.0));
			pvInfo.put("storageRate", new Float(1.0));
			pvInfo.put("RTYP", "ai");
			PolicyConfig policyConfig = ExecutePolicy.computePolicyForPV(is, "test", pvInfo);
			assertTrue("policyConfig is null", policyConfig != null);
			assertTrue("dataStores is null", policyConfig.getClass() != null);
		}
	}
}
