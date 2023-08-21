package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.actions.NTUtil.extractStringArray;
import static org.junit.Assert.assertArrayEquals;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.archiverappliance.mgmt.pva.actions.ResponseConstructionException;
import org.epics.pva.data.nt.PVATable;
import org.junit.Test;

public class PvaParserTest {

	private static final Logger logger = LogManager.getLogger(PvaParserTest.class.getName());

	@Test
	public void test() throws ResponseConstructionException {
		String json = """
				{ "pvName": "mshankar:arch:sine", "status": "Archive request submitted" }\r
				{ "pvName": "mshankar:arch:cosine", "status": "Archive request submitted" }\r
				""";
		PVATable result = PvaArchivePVAction.parseArchivePvResult(json);

		String[] expextedKePvNames = new String[] { "mshankar:arch:sine", "mshankar:arch:cosine" };
		String[] expectedStatus = new String[] { "Archive request submitted", "Archive request submitted" };
		logger.info("results" + result);
		assertArrayEquals(expextedKePvNames, extractStringArray(result.getColumn("pvName")));
		assertArrayEquals(expectedStatus, extractStringArray(result.getColumn("status")));

	}
}
