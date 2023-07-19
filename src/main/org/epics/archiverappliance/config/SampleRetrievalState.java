package org.epics.archiverappliance.config;


import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.retrieval.DataSourceforPV;
import org.epics.archiverappliance.retrieval.RetrievalState;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class SampleRetrievalState extends RetrievalState {
	private static Logger logger = LogManager.getLogger(SampleRetrievalState.class.getName());
	ConfigServiceForTests configService;
	public SampleRetrievalState(ConfigServiceForTests parentConfigService) {
		super(parentConfigService);
		this.configService = parentConfigService;
	}
	
	@Override
    public List<DataSourceforPV> getDataSources(BasicContext context, String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) throws IOException {
		if(pvName.startsWith(ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX)) {
			logger.info("Returnng unit test data sources");
			return getUnitTestDataSources(pvName);
		}
		
		return super.getDataSources(context, pvName, typeInfo, start, end, req);
	}

	/**
	 * The original unit tests were all constructed with YEAR partitions in mind. 
	 * So we change the sample config service to continue this for the unit test PV's.
	 * @param pvName
	 * @return
	 */
	private List<DataSourceforPV> getUnitTestDataSources(String pvName) throws IOException {
		ArrayList<DataSourceforPV> datasources = new ArrayList<DataSourceforPV>(); 
		
		if(pvName.equals(ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "CAYearSpan")) {
			try {
				// pwd is tomcat_CAYearSpanRetrievalTest/appliance0/logs
				File dataFile = new File("../../../src/test/org/epics/archiverappliance/retrieval/channelarchiver");
				assert(dataFile.exists());
				String dataSrcURL = "rtree://localhost?serverURL=" + URLEncoder.encode("file://" + dataFile.getAbsolutePath(), "UTF-8") + "&archiveKey=1";
				StoragePlugin caStoragePlugin = StoragePluginURLParser.parseStoragePlugin(dataSrcURL, configService);
				datasources.add(new DataSourceforPV(pvName, caStoragePlugin, 1, null, null));
				return datasources;
			} catch(Exception ex) {
				logger.error("Exception adding CA datasource for unit test", ex);
				return null;
			}
		}
		
		PlainPBStoragePlugin mediumTermStore = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=MTS&rootFolder=" + configService.rootFolder + "&partitionGranularity=PARTITION_YEAR", configService);
		datasources.add(new DataSourceforPV(pvName, mediumTermStore, 1, null, null));

		PlainPBStoragePlugin shortTermStore = (PlainPBStoragePlugin) StoragePluginURLParser.parseStoragePlugin("pb://localhost?name=STS&rootFolder=" + ConfigServiceForTests.DEFAULT_PB_SHORT_TERM_TEST_DATA_FOLDER + "&partitionGranularity=PARTITION_YEAR", configService);
		datasources.add(new DataSourceforPV(pvName, shortTermStore, 0, null, null));
		
		return datasources;
	}
}
