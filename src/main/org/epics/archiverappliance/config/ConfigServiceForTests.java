package org.epics.archiverappliance.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.config.persistence.InMemoryPersistence;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.etl.common.PBThreeTierETLPVLookup;
import org.epics.archiverappliance.mgmt.MgmtRuntimeState;

import javax.servlet.ServletContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;

public class ConfigServiceForTests extends DefaultConfigService {
    public static final String TESTAPPLIANCE0 = "appliance0";
    /**
     * Tomcat is launched listening to this port when running the unit tests
     */
    public static final int RETRIEVAL_TEST_PORT = 17665;
    /**
     * All unit test PV names are expected to begin with this.
     * This name is supposed to be something that we will not encounter in the field.
     */
    public static final String ARCH_UNIT_TEST_PVNAME_PREFIX = "--ArchUnitTest";
    protected static final String DEFAULT_PB_SHORT_TERM_TEST_DATA_FOLDER = getDefaultShortTermFolder();
    /**
     * A folder which is used to store the data for the unit tests...
     */
    protected static final String DEFAULT_PB_TEST_DATA_FOLDER = getDefaultPBTestFolder();
    static HashMap<String, ArchDBRTypes> samplePV2DBRtypemap = new HashMap<String, ArchDBRTypes>();
    private static final Logger logger = LogManager.getLogger(ConfigServiceForTests.class.getName());
    private static final Logger configlogger = LogManager.getLogger("config." + ConfigServiceForTests.class.getName());

    static {
        for (ArchDBRTypes type : ArchDBRTypes.values()) {
            samplePV2DBRtypemap.put(
                    ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX
                            + (type.isWaveForm() ? "V_" : "S_")
                            + type.getPrimitiveName(),
                    type);
        }
        System.getProperties().setProperty("log4j1.compatibility", "true");
    }

    String rootFolder = ConfigServiceForTests.DEFAULT_PB_TEST_DATA_FOLDER;
    public static final int defaultSecondsDisconnect = 10;
    private File webInfClassesFolder;

    /**
     * Special Constructor for Integration tests Do not use in unit tests.
     *
     * @throws ConfigException
     */
	public ConfigServiceForTests() throws ConfigException {
		super();
	}
    public ConfigServiceForTests(int jcaCommandThreadCount) throws ConfigException {
        this(new File("./build/classes"), jcaCommandThreadCount);
    }

	public static final int defaultMinutesDisconnect = 1;
	public ConfigServiceForTests(File WebInfClassesFolder, int jcaCommandThreadCount) throws ConfigException {
		this.webInfClassesFolder = WebInfClassesFolder;
		configlogger.info("The WEB-INF/classes folder is " + this.webInfClassesFolder.getAbsolutePath());
		appliances = new HashMap<String, ApplianceInfo>();
		pv2appliancemapping = new ConcurrentHashMap<String, ApplianceInfo>();
		namedFlags = new ConcurrentHashMap<String, Boolean>();
		typeInfos = new ConcurrentHashMap<String, PVTypeInfo>();
		archivePVRequests = new ConcurrentHashMap<String, UserSpecifiedSamplingParams>();
		aliasNamesToRealNames = new ConcurrentHashMap<String, String>();
		channelArchiverDataServers = new ConcurrentHashMap<String, String>();
		pvsForThisAppliance = new ConcurrentSkipListSet<String>();
		pausedPVsForThisAppliance = new ConcurrentSkipListSet<String>();
		pv2ChannelArchiverDataServer = new ConcurrentHashMap<String, List<ChannelArchiverDataServerPVInfo>>();
		appliancesConfigLoaded = new ConcurrentHashMap<String, Boolean>();

        myApplianceInfo = new ApplianceInfo(
                TESTAPPLIANCE0,
                "http://localhost:17665/mgmt/bpl",
                "http://localhost:17665/engine/bpl",
                "http://localhost:17665/retrieval/bpl",
                "http://localhost:17665/etl/bpl",
                "localhost:16670",
                "http://localhost:17665/retrieval");
        appliances.put(TESTAPPLIANCE0, myApplianceInfo);
        appliancesInCluster.add(TESTAPPLIANCE0);

        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(DEFAULT_ARCHAPPL_PROPERTIES_FILENAME);
            archapplproperties.load(is);
            configlogger.info(String.format("loadings properties file. %s", DEFAULT_ARCHAPPL_PROPERTIES_FILENAME));
        } catch (NullPointerException e) {
            Path config_path = Paths.get(this.webInfClassesFolder.getAbsolutePath()
                    + File.separatorChar
                    + DEFAULT_ARCHAPPL_PROPERTIES_FILENAME);
            config_path = config_path.normalize();
            String msg = String.format("Could not find config file:%s", config_path);
            configlogger.error(msg);
            throw new ConfigException(msg);
        } catch (Exception ex) {
            throw new ConfigException(
                    "Exception loading installation specific properties file " + DEFAULT_ARCHAPPL_PROPERTIES_FILENAME
                            + " from classpath",
                    ex);
        }

        if (jcaCommandThreadCount >= 1) {
            logger.warn("Overriding JCA command thread count to " + jcaCommandThreadCount);
            archapplproperties.put(
                    "org.epics.archiverappliance.engine.epics.commandThreadCount",
                    Integer.toString(jcaCommandThreadCount));
        }

        pvName2KeyConverter = new ConvertPVNameToKey();
        pvName2KeyConverter.initialize(this);

        persistanceLayer = new InMemoryPersistence();

        startupExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r);
            t.setName("Startup executor");
            return t;
        });

        this.engineContext = new EngineContext(this);
        this.engineContext.setDisconnectCheckTimeoutInSecondsForTestingPurposesOnly(defaultSecondsDisconnect);
        this.etlPVLookup = new PBThreeTierETLPVLookup(this);
        this.retrievalState = new SampleRetrievalState(this);
        this.mgmtRuntime = new MgmtRuntimeState(this);

        startupState = STARTUP_SEQUENCE.STARTUP_COMPLETE;
        this.addShutdownHook(() -> startupExecutor.shutdown());
    }

    public static String getDefaultPBTestFolder() {
        String defaultFolder = System.getenv("ARCHAPPL_MEDIUM_TERM_FOLDER");
        if (defaultFolder != null) {
            return defaultFolder;
        }

        if (File.separatorChar == '\\') {
            return "c://temp";
        } else {
            return "/scratch/LargeDisk/ArchiverStore";
        }
    }

    public static String getDefaultShortTermFolder() {
        String defaultSTFolder = System.getenv("ARCHAPPL_SHORT_TERM_FOLDER");
        if (defaultSTFolder != null) {
            return defaultSTFolder;
        }

        if (File.separatorChar == '\\') {
            return "c://temp";
        } else {
            return "/dev/shm/test";
        }
    }

    @Override
    public void initialize(ServletContext sce) throws ConfigException {
        super.initialize(sce);

        this.retrievalState = new SampleRetrievalState(this);
        if (this.engineContext != null) {
            this.engineContext.setDisconnectCheckTimeoutInSecondsForTestingPurposesOnly(defaultSecondsDisconnect);
        }
    }

	@Override
	public ApplianceInfo getApplianceForPV(String pvName) {
		ApplianceInfo applianceInfo = super.getApplianceForPV(pvName);
		// We should do the following code only for unit tests (and not for the real config service).
        if (applianceInfo == null && pvName.startsWith(ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX)) {
			logger.debug("Setting appliance for unit test pv " + pvName + " to self in unit tests mode.");
			applianceInfo = myApplianceInfo;
		}
		return applianceInfo;
	}

    /**
     * Register the pv to the appliance
     * Note this restarts any ETL JOBs in unit tests, so you'll need to call manualControlForUnitTests(); again
     * if they should be stopped.
     *
     * @param pvName The name of PV.
     * @param applianceInfo ApplianceInfo
     * @throws AlreadyRegisteredException pv already registered.
     */
	@Override
	public void registerPVToAppliance(String pvName, ApplianceInfo applianceInfo) throws AlreadyRegisteredException {
		super.registerPVToAppliance(pvName, applianceInfo);
		if (applianceInfo.getIdentity().equals(myApplianceInfo.getIdentity())) {
			logger.info("Adding pv " + pvName + " to this appliance's pvs and to ETL");
			this.pvsForThisAppliance.add(pvName);
            if (this.getETLLookup() != null) {
            	this.getETLLookup().addETLJobsForUnitTests(pvName, this.getTypeInfoForPV(pvName));
            }
        }
    }


    @Override
    public InputStream getPolicyText() throws IOException {
        if (webInfClassesFolder != null) {
            String policyURL = ConfigServiceForTests.class.getClassLoader().getResource("policies.py").getPath().toString();
            return new FileInputStream(policyURL);
        }
        return super.getPolicyText();
    }

    @Override
    public PVTypeInfo getTypeInfoForPV(String pvName) {
        PVTypeInfo ret = super.getTypeInfoForPV(pvName);
        if (ret != null) return ret;

        // For the unit tests, we have a naming convention that identifies the DBR type etc based on the name of the
        // PV...
        if (pvName.startsWith(ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX)) {
            pvName = PVNames.stripFieldNameFromPVName(pvName);
            logger.info("Unit test typeinfo for pv " + pvName);
            ArchDBRTypes namingConventionType = samplePV2DBRtypemap.get(pvName);
            if (namingConventionType == null) {
                logger.warn(
                        pvName + " does not follow the testing convention. Defaulting the dbrtype to a scalar double.");
                namingConventionType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
            }
            return getPvTypeInfo(pvName, namingConventionType);
        }

        return null;
    }

    private PVTypeInfo getPvTypeInfo(String pvName, ArchDBRTypes namingConventionType) {
        PVTypeInfo typeInfo = new PVTypeInfo(pvName, namingConventionType, !namingConventionType.isWaveForm(), 1);
        typeInfo.setUpperDisplayLimit(1.0);
        typeInfo.setLowerDisplayLimit(-1.0);
        typeInfo.setHasReducedDataSet(true);
        typeInfo.setComputedEventRate(1.0f);
        typeInfo.setComputedStorageRate(12.0f);
        typeInfo.setUserSpecifiedEventRate(1.0f);
        typeInfo.setApplianceIdentity(this.myIdentity);
        typeInfo.addArchiveField("HIHI");
        typeInfo.addArchiveField("LOLO");
        return typeInfo;
    }

    /**
     * Get the root folder for the PB storage plugin
     * @return
     */
    public String getPBRootFolder() {
        return rootFolder;
    }

    /**
     * This should only be called in the unit tests....
     * @param rootFolder
     */
    public void setPBRootFolder(String rootFolder) {
        this.rootFolder = rootFolder;
    }


	@Override
	public String getWebInfFolder() {
		if (this.webInfClassesFolder != null) {
			return this.webInfClassesFolder.getAbsolutePath();
		}

        return super.getWebInfFolder();
    }

    @Override
    public int getInitialDelayBeforeStartingArchiveRequestWorkflow() {
        // Of course, for testing, we kick off the archive PV workflow right away.
        return 10;
    }
}
