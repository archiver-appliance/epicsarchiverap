package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.actions.NTUtil.extractStringArray;
import static org.junit.Assert.assertArrayEquals;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.epics.archiverappliance.mgmt.pva.actions.PvaArchivePVAction;
import org.epics.nt.NTTable;
import org.epics.pvdata.pv.PVStringArray;
import org.junit.Test;

public class PvaParserTest {

	private static Logger logger = LogManager.getLogger(PvaParserTest.class.getName());

	@Test
	public void test() {
		String json = "{ \"pvName\": \"mshankar:arch:sine\", \"status\": \"Archive request submitted\" }\r\n"
				+ "{ \"pvName\": \"mshankar:arch:cosine\", \"status\": \"Archive request submitted\" }\r\n" + "";
		NTTable result = PvaArchivePVAction.parseArchivePvResult(json);

		String[] expextedKePvNames = new String[] { "mshankar:arch:sine", "mshankar:arch:cosine" };
		String[] expectedStatus = new String[] { "Archive request submitted", "Archive request submitted" };
		logger.info("results" + result.toString());
		assertArrayEquals(expextedKePvNames, extractStringArray(result.getColumn(PVStringArray.class, "pvName")));
		assertArrayEquals(expectedStatus, extractStringArray(result.getColumn(PVStringArray.class, "status")));

	}
}
