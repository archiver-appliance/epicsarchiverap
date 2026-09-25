package org.epics.archiverappliance.config;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.config.persistence.InMemoryPersistence;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.etl.common.PBThreeTierETLPVLookup;
import org.epics.archiverappliance.mgmt.MgmtRuntimeState;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.servlet.ServletContext;

public class ConfigServiceForTests extends DefaultConfigService {
    public static final String TESTAPPLIANCE0 = "appliance0";
    public static final int DEFAULT_MGMT_PORT = 17665;
    /**
     * Tomcat is launched listening to this port when running the unit tests
     */
    public static final int RETRIEVAL_TEST_PORT = DEFAULT_MGMT_PORT;
    /**
     * All unit test PV names are expected to begin with this.
     * This name is supposed to be something that we will not encounter in the field.
     */
    public static final String ARCH_UNIT_TEST_PVNAME_PREFIX = "--ArchUnitTest";

    public static final String HTTP_LOCALHOST = "http://localhost:";
    public static final String DATA_RETRIEVAL_URL = HTTP_LOCALHOST + DEFAULT_MGMT_PORT + "/retrieval";
    public static final String MGMT_BPL = "/mgmt/bpl";
    public static final String MGMT_URL = HTTP_LOCALHOST + DEFAULT_MGMT_PORT + MGMT_BPL;
    public static final String MGMT_UI_URL = HTTP_LOCALHOST + DEFAULT_MGMT_PORT + "/mgmt/ui";
    public static final String MGMT_INDEX_URL = HTTP_LOCALHOST + DEFAULT_MGMT_PORT + "/mgmt/ui/index.html";
    public static final String ENGINE_BPL = "/engine/bpl";
    public static final String ENGINE_URL = HTTP_LOCALHOST + DEFAULT_MGMT_PORT + ENGINE_BPL;
    public static final String RETRIEVAL_BPL = "/retrieval/bpl";
    public static final String RETRIEVAL_URL = HTTP_LOCALHOST + DEFAULT_MGMT_PORT + RETRIEVAL_BPL;
    public static final String RETRIEVAL_DATA = "/retrieval/data/getData.raw";
    public static final String RAW_RETRIEVAL_URL = HTTP_LOCALHOST + DEFAULT_MGMT_PORT + RETRIEVAL_DATA;
    public static final String GETDATAATTIME_DATA = "/retrieval/data/getDataAtTime";
    public static final String GETDATAATTIME_URL = HTTP_LOCALHOST + DEFAULT_MGMT_PORT + GETDATAATTIME_DATA;
    public static final String ETL_BPL = "/etl/bpl";
    public static final String ETL_URL = HTTP_LOCALHOST + DEFAULT_MGMT_PORT + ETL_BPL;
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
     * A single JVM-wide Hazelcast instance backing the config-service maps for every
     * {@link ConfigServiceForTests}, so the shared test JVM holds one member, not one per class.
     */
    private static volatile HazelcastInstance testHzInstance;

    /**
     * Lazily build the shared test Hazelcast instance: an isolated single member with all
     * discovery/join mechanisms disabled.
     */
    private static synchronized HazelcastInstance getTestHazelcastInstance() {
        if (testHzInstance == null) {
            Config config = new Config();
            config.setClusterName("archappl-tests");
            config.setProperty("hazelcast.phone.home.enabled", "false");
            config.getMetricsConfig().setEnabled(false);
            var join = config.getNetworkConfig().getJoin();
            join.getAutoDetectionConfig().setEnabled(false);
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(false);
            testHzInstance = Hazelcast.newHazelcastInstance(config);
        }
        return testHzInstance;
    }

    /**
     * Test-driver instances to shut down when the creating test class finishes; drained by
     * {@code ConfigServiceForTestsCleanupListener}.
     */
    private static final Set<ConfigServiceForTests> liveTestInstances = ConcurrentHashMap.newKeySet();

    /** Uniquifies this instance's map names on the shared Hazelcast member. */
    private static final AtomicLong instanceCounter = new AtomicLong();

    private final long instanceId = instanceCounter.incrementAndGet();

    /** Shut down and forget every tracked test-driver instance. */
    public static void shutdownTrackedInstances() {
        for (ConfigServiceForTests cs : liveTestInstances) {
            liveTestInstances.remove(cs);
            try {
                cs.shutdownNow();
            } catch (Throwable t) {
                logger.warn("Exception shutting down tracked test ConfigService", t);
            }
        }
    }

    /**
     * Exempt this instance from automatic teardown; for static helpers reused across test classes.
     */
    public void keepAliveAcrossTests() {
        liveTestInstances.remove(this);
    }

    private final List<IMap<?, ?>> perInstanceMaps = new ArrayList<>();

    private <K, V> IMap<K, V> perInstanceMap(HazelcastInstance hzinstance, String name) {
        IMap<K, V> map = hzinstance.getMap(name + "-" + instanceId);
        perInstanceMaps.add(map);
        return map;
    }

    @Override
    public void shutdownNow() {
        super.shutdownNow();
        // Release this instance's maps on the shared member so they don't accumulate over the suite.
        perInstanceMaps.forEach(IMap::destroy);
        perInstanceMaps.clear();
    }

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

        // These must be Hazelcast IMaps (the config service runs Hz predicate queries on them),
        // but a fresh member per construction leaks; reuse the one JVM-wide instance. Map names
        // are per-instance: several instances coexist (test drivers, PVCaPut helpers, ...) and
        // must not see or clear each other's state. Destroyed in shutdownNow().
        HazelcastInstance hzinstance = getTestHazelcastInstance();
        pv2appliancemapping = perInstanceMap(hzinstance, "pv2appliancemapping");
        namedFlags = perInstanceMap(hzinstance, "namedflags");
        typeInfos = perInstanceMap(hzinstance, TYPEINFO);
        archivePVRequests = perInstanceMap(hzinstance, "archivePVRequests");
        channelArchiverDataServers = perInstanceMap(hzinstance, "channelArchiverDataServers");
        clusterInet2ApplianceIdentity = perInstanceMap(hzinstance, CLUSTER_INET_2_APPLIANCE_IDENTITY);
        appliancesConfigLoaded = perInstanceMap(hzinstance, "appliancesConfigLoaded");
        aliasNamesToRealNames = perInstanceMap(hzinstance, "aliasNamesToRealNames");
        pv2ChannelArchiverDataServer = perInstanceMap(hzinstance, "pv2ChannelArchiverDataServer");

        appliances = new HashMap<String, ApplianceInfo>();

        myApplianceInfo = new ApplianceInfo(
                TESTAPPLIANCE0, MGMT_URL, ENGINE_URL, RETRIEVAL_URL, ETL_URL, "localhost:16670", DATA_RETRIEVAL_URL);
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
        ArchiveChannel.configureMetaDataPeriod(archapplproperties);

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

        // Tracked for teardown by the cleanup listener; shared helpers opt out via keepAliveAcrossTests().
        liveTestInstances.add(this);
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
    public void registerPVToAppliance(String pvName, ApplianceInfo applianceInfo, PVRegistrationType registrationType)
            throws AlreadyRegisteredException {
        // When we register a PV to an appliance in the unit tests, we often forget to set the appliance identity
        PVTypeInfo typeInfo = this.getTypeInfoForPV(pvName);
        if (typeInfo != null) {
            if (typeInfo.getApplianceIdentity() == null
                    || !typeInfo.getApplianceIdentity().equals(applianceInfo.getIdentity())) {
                typeInfo.setApplianceIdentity(applianceInfo.getIdentity());
                typeInfos.put(pvName, typeInfo);
            }
        }
        super.registerPVToAppliance(pvName, applianceInfo, registrationType);
        Set<String> queryResult = this.getPVsForAppliance(applianceInfo);
        if (!queryResult.contains(pvName)) {
            throw new RuntimeException(
                    "After registering PV " + pvName + ", getPVsForAppliance does not contains this PV");
        }
        if (applianceInfo.getIdentity().equals(myApplianceInfo.getIdentity())) {
            logger.info("Adding pv " + pvName + " to this appliance's pvs and to ETL");
            if (this.getETLLookup() != null) {
                this.getETLLookup().addETLJobsForUnitTests(pvName, this.getTypeInfoForPV(pvName));
            }
        }
    }

    @Override
    public InputStream getPolicyText() throws IOException {
        if (webInfClassesFolder != null) {
            String policyURL = ConfigServiceForTests.class
                    .getClassLoader()
                    .getResource("policies.py")
                    .getPath();
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
            pvName = PVNames.channelNamePVName(pvName);
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
