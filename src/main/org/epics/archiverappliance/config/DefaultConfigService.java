/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.topic.ITopic;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;

import edu.stanford.slac.archiverappliance.PB.data.PBTypeSystem;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.ProcessMetrics;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfoEvent.ChangeType;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.config.persistence.MySQLPersistence;
import org.epics.archiverappliance.config.pubsub.PubSubEvent;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.etl.common.PBThreeTierETLPVLookup;
import org.epics.archiverappliance.mgmt.MgmtPostStartup;
import org.epics.archiverappliance.mgmt.MgmtRuntimeState;
import org.epics.archiverappliance.mgmt.NonMgmtPostStartup;
import org.epics.archiverappliance.mgmt.bpl.cahdlers.NamesHandler;
import org.epics.archiverappliance.mgmt.policy.ExecutePolicy;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.retrieval.RetrievalState;
import org.epics.archiverappliance.retrieval.channelarchiver.XMLRPCClient;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.URIUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.ServletContext;

/**
 * This is the default config service for the archiver appliance.
 * There is a subclass that is used in the junit tests.
 *
 * @author mshankar
 *
 */
public class DefaultConfigService implements ConfigService {
    public static final String LOGGING_TYPE = "log4j2";
    public static final String ARCHAPPL_NAME = "archappl";
    public static final String LOCAL_HOST_ADDRESS = "127.0.0.1";
    public static final String TYPEINFO = "typeinfo";
    public static final String CLUSTER_INET_2_APPLIANCE_IDENTITY = "clusterInet2ApplianceIdentity";
    private static final Logger logger = LogManager.getLogger(DefaultConfigService.class.getName());
    private static final Logger configlogger = LogManager.getLogger("config." + DefaultConfigService.class.getName());
    private static final Logger clusterLogger = LogManager.getLogger("cluster." + DefaultConfigService.class.getName());

    static {
        System.getProperties().setProperty("log4j1.compatibility", "true");
    }

    public static final String SITE_FOR_UNIT_TESTS_NAME = "org.epics.archiverappliance.config.site";
    public static final String SITE_FOR_UNIT_TESTS_VALUE = "tests";

    /**
     * Add a property in archappl.properties under this key to identify the class that implements the PVNameToKeyMapping for this installation.
     */
    public static final String ARCHAPPL_PVNAME_TO_KEY_MAPPING_CLASSNAME =
            "org.epics.archiverappliance.config.DefaultConfigService.PVName2KeyMappingClassName";

    // Configuration state begins here.
    protected String myIdentity;
    protected ApplianceInfo myApplianceInfo = null;
    protected Map<String, ApplianceInfo> appliances = null;
    // Persisted state begins here.
    protected Map<String, PVTypeInfo> typeInfos = null;
    protected Map<String, UserSpecifiedSamplingParams> archivePVRequests = null;
    protected Map<String, String> channelArchiverDataServers = null;
    protected Map<String, String> aliasNamesToRealNames = null;
    // These are not persisted but derived from other info
    protected Map<String, ApplianceInfo> pv2appliancemapping = null;
    protected Map<String, String> clusterInet2ApplianceIdentity = null;
    protected Map<String, Boolean> appliancesConfigLoaded = null;
    protected Map<String, List<ChannelArchiverDataServerPVInfo>> pv2ChannelArchiverDataServer = null;
    protected ITopic<PubSubEvent> pubSub = null;
    protected Map<String, Boolean> namedFlags = null;
    // Configuration state ends here.

    // Runtime state begins here
    protected LinkedList<Runnable> shutdownHooks = new LinkedList<>();
    protected PBThreeTierETLPVLookup etlPVLookup = null;
    protected RetrievalState retrievalState = null;
    protected MgmtRuntimeState mgmtRuntime = null;
    protected EngineContext engineContext = null;
    protected ConcurrentSkipListSet<String> appliancesInCluster = new ConcurrentSkipListSet<String>();
    // Runtime state ends here

    // This is an optimization; we cache a copy of PVs that are registered for this appliance.
    protected ConcurrentSkipListSet<String> pvsForThisAppliance = null;
    // Maintain a TRIE index for the pvNames in this appliance.
    protected ConcurrentHashMap<String, ConcurrentSkipListSet<String>> parts2PVNamesForThisAppliance =
            new ConcurrentHashMap<String, ConcurrentSkipListSet<String>>();
    protected ConcurrentSkipListSet<String> pausedPVsForThisAppliance = null;
    protected ApplianceAggregateInfo applianceAggregateInfo = new ApplianceAggregateInfo();
    protected EventBus eventBus = new AsyncEventBus(Executors.newSingleThreadExecutor(r -> new Thread(r, "Event bus")));
    protected Properties archapplproperties = new Properties();
    protected PVNameToKeyMapping pvName2KeyConverter = null;
    protected ConfigPersistence persistanceLayer;
    protected ConcurrentHashMap<String, LoadingCache<String, Boolean>> failoverPVs =
            new ConcurrentHashMap<String, LoadingCache<String, Boolean>>();

    // State local to DefaultConfigService.
    protected WAR_FILE warFile = WAR_FILE.MGMT;
    protected STARTUP_SEQUENCE startupState = STARTUP_SEQUENCE.ZEROTH_STATE;
    protected ScheduledExecutorService startupExecutor = null;
    protected ProcessMetrics processMetrics = new ProcessMetrics();
    private final HashSet<String> runTimeFields = new HashSet<String>();
    // Use a Guava cache to store one and only one ExecutePolicy object that expires after some inactivity.
    // The side effect is that it may take this many minutes to update the policy that is cached.
    private final LoadingCache<String, ExecutePolicy> theExecutionPolicy = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .removalListener((RemovalListener<String, ExecutePolicy>)
                    arg -> arg.getValue().close())
            .build(new CacheLoader<String, ExecutePolicy>() {
                public ExecutePolicy load(String key) throws IOException {
                    logger.info("Updating the cached execute policy");
                    return new ExecutePolicy(DefaultConfigService.this);
                }
            });

    private ServletContext servletContext;

    private final long appserverStartEpochSeconds = TimeUtils.getCurrentEpochSeconds();

    private HazelcastInstance hzinstance;

    protected DefaultConfigService() {
        // Only the unit tests config service uses this constructor.
    }

    @Override
    public void initialize(ServletContext sce) throws ConfigException {
        this.servletContext = sce;
        String contextPath = sce.getContextPath();
        logger.info("DefaultConfigService was created with a servlet context " + contextPath);

        try {
            String pathToVersionTxt = sce.getRealPath("ui/comm/version.txt");
            logger.debug(() -> "The full path to the version.txt is " + pathToVersionTxt);
            List<String> lines = Files.readAllLines(Paths.get(pathToVersionTxt), StandardCharsets.UTF_8);
            for (String line : lines) {
                configlogger.info(line);
            }
        } catch (Throwable t) {
            logger.fatal("Unable to determine appliance version", t);
        }

        try {
            // We first try Java system properties for this appliance's identity
            // If a property is not defined, then we check the environment.
            // This gives us the ability to cater to unit tests as well as running using buildAndDeploy scripts without
            // touching the server.xml file.
            // Probably not the most standard way but suited to this need.
            // Finally, we use the local machine's hostname as the myidentity.
            myIdentity = System.getProperty(ARCHAPPL_MYIDENTITY);
            if (myIdentity == null) {
                myIdentity = System.getenv(ARCHAPPL_MYIDENTITY);
                if (myIdentity != null) {
                    logger.info("Obtained my identity from environment variable " + myIdentity);
                } else {
                    logger.info("Using the local machine's hostname " + myIdentity + " as my identity");
                    myIdentity = InetAddress.getLocalHost().getCanonicalHostName();
                }
                if (myIdentity == null) {
                    throw new ConfigException("Unable to determine identity of this appliance");
                }
            } else {
                logger.info("Obtained my identity from Java system properties " + myIdentity);
            }

            logger.info("My identity is " + myIdentity);
        } catch (Exception ex) {
            String msg = "Cannot determine this appliance's identity using either the environment variable "
                    + ARCHAPPL_MYIDENTITY + " or the java system property " + ARCHAPPL_MYIDENTITY;
            configlogger.fatal(msg);
            throw new ConfigException(msg, ex);
        }
        // Appliances should be local and come straight from persistence.
        try {
            appliances = AppliancesList.loadAppliancesXML(servletContext);
        } catch (Exception ex) {
            throw new ConfigException("Exception loading appliances.xml", ex);
        }

        myApplianceInfo = appliances.get(myIdentity);
        if (myApplianceInfo == null)
            throw new ConfigException("Unable to determine applianceinfo using identity " + myIdentity);
        configlogger.info("My identity is " + myApplianceInfo.getIdentity() + " and my mgmt URL is "
                + myApplianceInfo.getMgmtURL());

        try {
            String archApplPropertiesFileName = System.getProperty(ARCHAPPL_PROPERTIES_FILENAME);
            if (archApplPropertiesFileName == null) {
                archApplPropertiesFileName = System.getenv(ARCHAPPL_PROPERTIES_FILENAME);
            }
            if (archApplPropertiesFileName == null) {
                archApplPropertiesFileName = new URL(this.getClass()
                                .getClassLoader()
                                .getResource(DEFAULT_ARCHAPPL_PROPERTIES_FILENAME)
                                .toString())
                        .getPath();
                configlogger.info(
                        "Loading archappl.properties from the webapp classpath " + archApplPropertiesFileName);
            } else {
                configlogger.info("Loading archappl.properties using the environment/JVM property from "
                        + archApplPropertiesFileName);
            }
            try (InputStream is = new FileInputStream(archApplPropertiesFileName)) {
                archapplproperties.load(is);
                configlogger.info(
                        "Done loading installation specific properties file from " + archApplPropertiesFileName);
            } catch (Exception ex) {
                throw new ConfigException(
                        "Exception loading installation specific properties file " + archApplPropertiesFileName, ex);
            }
        } catch (ConfigException cex) {
            throw cex;
        } catch (Exception ex) {
            configlogger.fatal("Exception loading the appliance properties file", ex);
        }

        String pvName2KeyMappingClass =
                this.getInstallationProperties().getProperty(ARCHAPPL_PVNAME_TO_KEY_MAPPING_CLASSNAME);
        if (pvName2KeyMappingClass == null || pvName2KeyMappingClass.isEmpty()) {
            logger.info("Using the default key mapping class");
            pvName2KeyConverter = new ConvertPVNameToKey();
            pvName2KeyConverter.initialize(this);
        } else {
            try {
                logger.info("Using " + pvName2KeyMappingClass + " as the name to key mapping class");
                pvName2KeyConverter = (PVNameToKeyMapping)
                        Class.forName(pvName2KeyMappingClass).getConstructor().newInstance();
                pvName2KeyConverter.initialize(this);
            } catch (Exception ex) {
                logger.fatal("Cannot initialize pv name to key mapping class " + pvName2KeyMappingClass, ex);
                throw new ConfigException(
                        "Cannot initialize pv name to key mapping class " + pvName2KeyMappingClass, ex);
            }
        }

        String runtimeFieldsListStr =
                this.getInstallationProperties().getProperty("org.epics.archiverappliance.config.RuntimeKeys");
        if (runtimeFieldsListStr != null && !runtimeFieldsListStr.isEmpty()) {
            logger.debug(() -> "Got runtime fields from the properties file " + runtimeFieldsListStr);
            String[] runTimeFieldsArr = runtimeFieldsListStr.split(",");
            for (String rf : runTimeFieldsArr) {
                this.runTimeFields.add(rf.trim());
            }
        }



        switch (contextPath) {
            case "/mgmt":
                warFile = WAR_FILE.MGMT;
                this.mgmtRuntime = new MgmtRuntimeState(this);
                break;
            case "/engine":
                warFile = WAR_FILE.ENGINE;
                this.engineContext = new EngineContext(this);
                break;
            case "/retrieval":
                warFile = WAR_FILE.RETRIEVAL;
                this.retrievalState = new RetrievalState(this);
                break;
            case "/etl":
                this.etlPVLookup = new PBThreeTierETLPVLookup(this);
                warFile = WAR_FILE.ETL;
                break;
            default:
                logger.error("We seem to have introduced a new component into the system " + contextPath);
        }

        // To make sure we are not starting multiple appliance with the same identity, we make sure that the hostnames
        // match
        if (this.warFile == WAR_FILE.MGMT) {
            try {
                String machineHostName = InetAddress.getLocalHost().getCanonicalHostName();
                String[] myAddrParts = myApplianceInfo.getClusterInetPort().split(":");
                String myHostNameFromInfo = myAddrParts[0];
                if (myHostNameFromInfo.equals("localhost")) {
                    logger.debug(
                            "Using localhost for the cluster inet port. If you are indeed running a cluster, the cluster members will not join the cluster.");
                } else if (myHostNameFromInfo.equals(machineHostName)) {
                    logger.debug(
                            "Hostname from config and hostname from InetAddress match exactly; we are correctly configured "
                                    + machineHostName);
                } else if (InetAddressValidator.getInstance().isValid(myHostNameFromInfo)) {
                    logger.debug(() -> "Using ipAddress for cluster config " + myHostNameFromInfo);
                } else {
                    String msg = "The hostname from appliances.xml is " + myHostNameFromInfo
                            + " and from a call to InetAddress.getLocalHost().getCanonicalHostName() (typially FQDN) is "
                            + machineHostName
                            + ". These are not identical. They are probably equivalent but to prevent multiple appliances binding to the same identity we enforce this equality.";
                    configlogger.fatal(msg);
                    throw new ConfigException(msg);
                }
            } catch (UnknownHostException ex) {
                configlogger.error(
                        "Got an UnknownHostException when trying to determine the hostname. This happens when DNS is not set correctly on this machine (for example, when using VM's. See the documentation for InetAddress.getLocalHost().getCanonicalHostName()");
            }
        }

        startupExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r);
            t.setName("Startup executor");
            return t;
        });

        this.addShutdownHook(() -> {
            logger.info("Shutting down startup scheduled executor...");
            startupExecutor.shutdown();
        });

        this.startupState = STARTUP_SEQUENCE.READY_TO_JOIN_APPLIANCE;
        if (this.warFile == WAR_FILE.MGMT) {
            logger.info("Scheduling webappReady's for the mgmt webapp ");
            MgmtPostStartup mgmtPostStartup = new MgmtPostStartup(this);
            ScheduledFuture<?> postStartupFuture =
                    startupExecutor.scheduleAtFixedRate(mgmtPostStartup, 10, 20, TimeUnit.SECONDS);
            mgmtPostStartup.setCancellingFuture(postStartupFuture);
        } else {
            logger.info("Scheduling webappReady's for the non-mgmt webapp " + this.warFile.toString());
            NonMgmtPostStartup nonMgmtPostStartup = new NonMgmtPostStartup(this, this.warFile.toString());
            ScheduledFuture<?> postStartupFuture =
                    startupExecutor.scheduleAtFixedRate(nonMgmtPostStartup, 10, 20, TimeUnit.SECONDS);
            nonMgmtPostStartup.setCancellingFuture(postStartupFuture);
        }

        // Measure some JMX metrics once a minute
        startupExecutor.scheduleAtFixedRate(() -> processMetrics.takeMeasurement(), 60, 60, TimeUnit.SECONDS);
    }

    /* (non-Javadoc)
     * @see org.epics.archiverappliance.config.ConfigService#postStartup()
     */
    @Override
    public void postStartup() throws ConfigException {
        if (this.startupState != STARTUP_SEQUENCE.READY_TO_JOIN_APPLIANCE) {
            configlogger.info("Webapp is not in correct state for postStartup "
                    + this.getWarFile().toString() + ". It is in " + this.startupState.toString());
            return;
        }

        this.startupState = STARTUP_SEQUENCE.POST_STARTUP_RUNNING;
        configlogger.info("Post startup for " + this.getWarFile().toString());

        // Set the thread count to control how may threads this library spawns.
        Properties hzThreadCounts = new Properties();
        if (System.getenv().containsKey("ARCHAPPL_ALL_APPS_ON_ONE_JVM")) {
            logger.info("Reducing the generic clustering thread counts.");
            hzThreadCounts.put("hazelcast.clientengine.thread.count", "2");
            hzThreadCounts.put("hazelcast.operation.generic.thread.count", "2");
            hzThreadCounts.put("hazelcast.operation.thread.count", "2");
        }

        if (this.warFile == WAR_FILE.MGMT) {
            // The management webapps are the head honchos in the cluster. We set them up differently

            configlogger.debug(() -> "Initializing the MGMT webapp's clustering");
            // If we have a hazelcast.xml in the servlet classpath, the XmlConfigBuilder picks that up.
            // If not we use the default config found in hazelcast.jar
            // We then alter this config to suit our purposes.
            Config config = new XmlConfigBuilder().build();
            try {
                if (this.getClass().getResource("hazelcast.xml") == null) {
                    logger.info("We override the default cluster config by disabling multicast discovery etc.");
                    // We do not use multicast as it is not supported on all networks.
                    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
                    // We use TCPIP to discover the members in the cluster.
                    // This is part of the config that comes from appliance.xml
                    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
                    // Clear any tcpip config that comes from the default config
                    // This gets rid of the localhost in the default that prevents clusters from forming..
                    // If we need localhost, we'll add it back later.
                    config.getNetworkConfig().getJoin().getTcpIpConfig().clear();
                    // Enable interfaces; we seem to need this after 2.4 for clients to work correctly in a multi-homed
                    // environment.
                    // We'll add the actual interface later below
                    config.getNetworkConfig().getInterfaces().setEnabled(true);
                    config.getNetworkConfig().getInterfaces().clear();

                    // We don't really use the authentication provided by the tool; however, we set it to some default
                    config.setClusterName(ARCHAPPL_NAME);

                    // Backup count is 1 by default; we set it explicitly however...
                    config.getMapConfig("default").setBackupCount(1);
                } else {
                    logger.debug(
                            "There is a hazelcast.xml in the classpath; skipping default configuration in the code.");
                }
            } catch (Exception ex) {
                throw new ConfigException("Exception configuring cluster", ex);
            }

            config.setInstanceName(myIdentity);

            if (!hzThreadCounts.isEmpty()) {
                logger.info("Reducing the generic clustering thread counts.");
                config.getProperties().putAll(hzThreadCounts);
            }

            config.setProperty( "hazelcast.logging.type", "log4j2" );

            try {
                String[] myAddrParts = myApplianceInfo.getClusterInetPort().split(":");
                String myHostName = myAddrParts[0];
                InetAddress myInetAddr = InetAddress.getByName(myHostName);
                if (!myHostName.equals("localhost") && myInetAddr.isLoopbackAddress()) {
                    logger.info("Address for this appliance -- " + myInetAddr
                            + " is a loopback address. Changing this to 127.0.0.1 to clustering happy");
                    myInetAddr = InetAddress.getByName(LOCAL_HOST_ADDRESS);
                }
                int myClusterPort = Integer.parseInt(myAddrParts[1]);

                logger.debug(() -> "We do not let the port auto increment for the MGMT webap");
                config.getNetworkConfig().setPortAutoIncrement(false);

                config.getNetworkConfig().setPort(myClusterPort);
                config.getNetworkConfig().getInterfaces().addInterface(myInetAddr.getHostAddress());
                configlogger.info("Setting my cluster port base to " + myClusterPort + " and using interface "
                        + myInetAddr.getHostAddress());

                for (ApplianceInfo applInfo : appliances.values()) {
                    if (applInfo.getIdentity().equals(myIdentity) && this.warFile == WAR_FILE.MGMT) {
                        logger.debug(() -> "Not adding myself to the discovery process when I am the mgmt webapp");
                    } else {
                        String[] addressparts = applInfo.getClusterInetPort().split(":");
                        String inetaddrpart = addressparts[0];
                        try {
                            InetAddress inetaddr = InetAddress.getByName(inetaddrpart);
                            if (!inetaddrpart.equals("localhost") && inetaddr.isLoopbackAddress()) {
                                logger.info("Address for appliance " + applInfo.getIdentity() + " -  " + inetaddr
                                        + " is a loopback address. Changing this to 127.0.0.1 to clustering happy");
                                inetaddr = InetAddress.getByName(LOCAL_HOST_ADDRESS);
                            }
                            int clusterPort = Integer.parseInt(addressparts[1]);
                            logger.info("Adding " + applInfo.getIdentity()
                                    + " from appliances.xml to the cluster discovery using cluster inetport "
                                    + inetaddr.toString() + ":" + clusterPort);
                            config.getNetworkConfig()
                                    .getJoin()
                                    .getTcpIpConfig()
                                    .addMember(inetaddr.getHostAddress() + ":" + clusterPort);
                        } catch (UnknownHostException ex) {
                            configlogger.info("Cannnot resolve the IP address for appliance " + inetaddrpart
                                    + ". Skipping adding this appliance to the cliuster.");
                        }
                    }
                }
                hzinstance = Hazelcast.newHazelcastInstance(config);
            } catch (Exception ex) {
                throw new ConfigException("Exception adding member to cluster", ex);
            }
        } else {
            // All other webapps are "native" clients.
            try {
                logger.debug(() -> "Initializing a non-mgmt webapp's clustering");
                /*
                 * Loads the client config using the following resolution mechanism:
                 *   1. first it checks if a system property 'hazelcast.client.config' is set. If it exist and it begins with 'classpath:', then a classpath resource is loaded. Else it will assume it is a file reference
                 *   2. it checks if a hazelcast-client.xml is available in the working dir
                 *   3. it checks if a hazelcast-client.xml is available on the classpath
                 *   4. it loads the hazelcast-client-default.xml
                 */
                ClientConfig clientConfig = new XmlClientConfigBuilder().build();
                clientConfig.setClusterName(ARCHAPPL_NAME);
                clientConfig.setInstanceName(myIdentity+"_"+this.warFile);
                clientConfig.setProperty( "hazelcast.logging.type", "log4j2" );

                // Non mgmt client can only connect to their MGMT webapp.
                String[] myAddrParts = myApplianceInfo.getClusterInetPort().split(":");
                String myHostName = myAddrParts[0];
                InetAddress myInetAddr = InetAddress.getByName(myHostName);
                if (!myHostName.equals("localhost") && myInetAddr.isLoopbackAddress()) {
                    logger.info("Address for this appliance -- " + myInetAddr
                            + " is a loopback address. Changing this to 127.0.0.1 to clustering happy");
                    myInetAddr = InetAddress.getByName(LOCAL_HOST_ADDRESS);
                }
                int myClusterPort = Integer.parseInt(myAddrParts[1]);

                logger.debug(this.warFile + " connecting as a native client to " + myInetAddr.getHostAddress()
                        + ":" + myClusterPort);
                clientConfig.getNetworkConfig().addAddress(myInetAddr.getHostAddress() + ":" + myClusterPort);

                if (!hzThreadCounts.isEmpty()) {
                    logger.info("Reducing the generic clustering thread counts.");
                    clientConfig.getProperties().putAll(hzThreadCounts);
                }
                logger.info("client network config conn timeout: "
                        + clientConfig.getNetworkConfig().getConnectionTimeout());
                logger.info("client network config addresses: "
                        + clientConfig.getNetworkConfig().getAddresses().stream()
                                .map(Object::toString)
                                .collect(Collectors.joining(",")));
                logger.info("client network config is redo: "
                        + clientConfig.getNetworkConfig().isRedoOperation());
                logger.info("client config properties: "
                        + clientConfig.getProperties().toString());
                hzinstance = HazelcastClient.newHazelcastClient(clientConfig);
            } catch (Exception ex) {
                throw new ConfigException("Exception adding client to cluster", ex);
            }
        }

        pv2appliancemapping = hzinstance.getMap("pv2appliancemapping");
        namedFlags = hzinstance.getMap("namedflags");
        typeInfos = hzinstance.getMap(TYPEINFO);
        archivePVRequests = hzinstance.getMap("archivePVRequests");
        channelArchiverDataServers = hzinstance.getMap("channelArchiverDataServers");
        clusterInet2ApplianceIdentity = hzinstance.getMap(CLUSTER_INET_2_APPLIANCE_IDENTITY);
        appliancesConfigLoaded = hzinstance.getMap("appliancesConfigLoaded");
        aliasNamesToRealNames = hzinstance.getMap("aliasNamesToRealNames");
        pv2ChannelArchiverDataServer = hzinstance.getMap("pv2ChannelArchiverDataServer");
        pubSub = hzinstance.getTopic("pubSub");

        final HazelcastInstance shutdownHzInstance = hzinstance;
        shutdownHooks.add(0, () -> {
            logger.debug(() -> "Shutting down clustering instance in webapp " + warFile.toString());
            shutdownHzInstance.shutdown();
        });

        if (this.warFile == WAR_FILE.MGMT) {
            Cluster cluster = hzinstance.getCluster();
            String localInetPort = getMemberKey(cluster.getLocalMember());
            clusterInet2ApplianceIdentity.put(localInetPort, myIdentity);
            logger.debug(() -> "Adding myself " + myIdentity + " as having inetport " + localInetPort);
            hzinstance
                    .getMap(CLUSTER_INET_2_APPLIANCE_IDENTITY)
                    .addEntryListener(
                            (EntryAddedListener<Object, Object>) event -> {
                                String appliden = (String) event.getValue();
                                appliancesInCluster.add(appliden);
                                logger.info("Adding appliance " + appliden
                                        + " to the list of active appliances as inetport "
                                        + event.getKey());
                            },
                            true);
            hzinstance
                    .getMap(CLUSTER_INET_2_APPLIANCE_IDENTITY)
                    .addEntryListener(
                            (EntryRemovedListener<Object, Object>) event -> {
                                String appliden = (String) event.getValue();
                                appliancesInCluster.remove(appliden);
                                logger.info("Removing appliance " + appliden
                                        + " from the list of active appliancesas inetport "
                                        + event.getKey());
                            },
                            true);

            logger.debug(
                    () -> "Establishing a cluster membership listener to detect when appliances drop off the cluster");
            cluster.addMembershipListener(new MembershipListener() {
                public void memberAdded(MembershipEvent membersipEvent) {
                    Member member = membersipEvent.getMember();
                    String inetPort = getMemberKey(member);
                    if (clusterInet2ApplianceIdentity.containsKey(inetPort)) {
                        String appliden = clusterInet2ApplianceIdentity.get(inetPort);
                        appliancesInCluster.add(appliden);
                        configlogger.info("Adding newly started appliance " + appliden
                                + " to the list of active appliances for inetport " + inetPort);
                    } else {
                        logger.debug(() -> "Skipping adding appliance using inetport " + inetPort
                                + " to the list of active instances as we do not have a mapping to its identity");
                    }
                }

                public void memberRemoved(MembershipEvent membersipEvent) {
                    Member member = membersipEvent.getMember();
                    String inetPort = getMemberKey(member);
                    if (clusterInet2ApplianceIdentity.containsKey(inetPort)) {
                        String appliden = clusterInet2ApplianceIdentity.get(inetPort);
                        appliancesInCluster.remove(appliden);
                        configlogger.info("Removing appliance " + appliden + " from the list of active appliances");
                    } else {
                        configlogger.debug(() -> "Received member removed event for " + inetPort);
                    }
                }
            });

            logger.debug(
                    "Adding the current members in the cluster after establishing the cluster membership listener");
            for (Member member : cluster.getMembers()) {
                String mbrInetPort = getMemberKey(member);
                logger.debug(() -> "Found member " + mbrInetPort);
                if (clusterInet2ApplianceIdentity.containsKey(mbrInetPort)) {
                    String appliden = clusterInet2ApplianceIdentity.get(mbrInetPort);
                    appliancesInCluster.add(appliden);
                    logger.info("Adding appliance " + appliden + " to the list of active appliances for inetport "
                            + mbrInetPort);
                } else {
                    logger.debug(() -> "Skipping adding appliance using inetport " + mbrInetPort
                            + " to the list of active instances as we do not have a mapping to its identity");
                }
            }
            logger.info("Established subscription(s) for appliance availability");

            if (this.getInstallationProperties().containsKey(ARCHAPPL_NAMEDFLAGS_PROPERTIES_FILE_PROPERTY)) {
                String namedFlagsFileName =
                        (String) this.getInstallationProperties().get(ARCHAPPL_NAMEDFLAGS_PROPERTIES_FILE_PROPERTY);
                configlogger.info("Loading named flags from file " + namedFlagsFileName);
                File namedFlagsFile = new File(namedFlagsFileName);
                if (!namedFlagsFile.exists()) {
                    configlogger.error(
                            "File containing named flags " + namedFlagsFileName + " specified but not present");
                } else {
                    Properties namedFlagsFromFile = new Properties();
                    try (FileInputStream is = new FileInputStream(namedFlagsFile)) {
                        namedFlagsFromFile.load(is);
                        for (Object namedFlagFromFile : namedFlagsFromFile.keySet()) {
                            try {
                                String namedFlagFromFileStr = (String) namedFlagFromFile;
                                Boolean namedFlagFromFileValue =
                                        Boolean.parseBoolean((String) namedFlagsFromFile.get(namedFlagFromFileStr));
                                logger.debug(
                                        "Setting named flag " + namedFlagFromFileStr + " to " + namedFlagFromFileValue);
                                this.namedFlags.put(namedFlagFromFileStr, namedFlagFromFileValue);
                            } catch (Exception ex) {
                                logger.error("Exception loading named flag from file" + namedFlagsFileName, ex);
                            }
                        }
                    } catch (Exception ex) {
                        configlogger.error("Exception loading named flags from " + namedFlagsFileName, ex);
                    }
                }
            }
        }

        if (this.warFile == WAR_FILE.ENGINE) {
            // It can take a while for the engine to start up.
            // We probably want to do this in the background so that the appliance as a whole starts up quickly and we
            // get retrieval up and running quickly.
            this.startupExecutor.schedule(
                    () -> {
                        try {
                            logger.debug(() -> "Starting up the engine's channels on startup.");
                            archivePVSonStartup();
                            logger.debug(() -> "Done starting up the engine's channels in startup.");
                        } catch (Throwable t) {
                            configlogger.fatal("Exception starting up the engine channels on startup", t);
                        }
                    },
                    1,
                    TimeUnit.SECONDS);
        } else if (this.warFile == WAR_FILE.ETL) {
            this.etlPVLookup.postStartup();
        } else if (this.warFile == WAR_FILE.MGMT) {
            pvsForThisAppliance = new ConcurrentSkipListSet<String>();
            pausedPVsForThisAppliance = new ConcurrentSkipListSet<String>();

            initializePersistenceLayer();

            loadTypeInfosFromPersistence();

            loadAliasesFromPersistence();

            loadArchiveRequestsFromPersistence();

            loadExternalServersFromPersistence();

            appliancesConfigLoaded.put(myIdentity, Boolean.TRUE);

            registerForNewExternalServers(hzinstance.getMap("channelArchiverDataServers"));

            // Cache the aggregate of all the PVs that are registered to this appliance.
            logger.debug(() -> "Building a local aggregate of PV infos that are registered to this appliance");
            try {
                for (String pvName : getPVsForThisAppliance()) {
                    if (!pvsForThisAppliance.contains(pvName)) {
                        applianceAggregateInfo.addInfoForPV(pvName, this.getTypeInfoForPV(pvName), this);
                    }
                }
            } catch (Exception ex) {
                logger.error("Exception building data for capacity planning", ex);
            }

        } else if (this.warFile == WAR_FILE.RETRIEVAL) {
            initializeFailoverServerCache();
        }

        // Register for changes to the typeinfo map.
        logger.info("Registering for changes to typeinfos");
        hzinstance
                .getMap(TYPEINFO)
                .addEntryListener(
                        (EntryAddedListener<Object, Object>) entryEvent -> {
                            logger.debug(() -> "Received entryAdded for pvTypeInfo");
                            PVTypeInfo typeInfo = (PVTypeInfo) entryEvent.getValue();
                            String pvName = typeInfo.getPvName();
                            eventBus.post(new PVTypeInfoEvent(pvName, typeInfo, ChangeType.TYPEINFO_ADDED));
                            if (persistanceLayer != null) {
                                try {
                                    persistanceLayer.putTypeInfo(pvName, typeInfo);
                                } catch (Exception ex) {
                                    logger.error("Exception persisting pvTypeInfo for pv " + pvName, ex);
                                }
                            }
                        },
                        true);
        hzinstance
                .getMap(TYPEINFO)
                .addEntryListener(
                        (EntryRemovedListener<Object, Object>) entryEvent -> {
                            PVTypeInfo typeInfo = (PVTypeInfo) entryEvent.getOldValue();
                            String pvName = typeInfo.getPvName();
                            logger.info("Received entryRemoved for pvTypeInfo " + pvName);
                            eventBus.post(new PVTypeInfoEvent(pvName, typeInfo, ChangeType.TYPEINFO_DELETED));
                            if (persistanceLayer != null) {
                                try {
                                    persistanceLayer.deleteTypeInfo(pvName);
                                } catch (Exception ex) {
                                    logger.error("Exception deleting pvTypeInfo for pv " + pvName, ex);
                                }
                            }
                        },
                        true);
        hzinstance
                .getMap(TYPEINFO)
                .addEntryListener(
                        (EntryUpdatedListener<Object, Object>) entryEvent -> {
                            PVTypeInfo typeInfo = (PVTypeInfo) entryEvent.getValue();
                            String pvName = typeInfo.getPvName();
                            eventBus.post(new PVTypeInfoEvent(pvName, typeInfo, ChangeType.TYPEINFO_MODIFIED));
                            logger.debug(() -> "Received entryUpdated for pvTypeInfo");
                            if (persistanceLayer != null) {
                                try {
                                    persistanceLayer.putTypeInfo(pvName, typeInfo);
                                } catch (Exception ex) {
                                    logger.error("Exception persisting pvTypeInfo for pv " + pvName, ex);
                                }
                            }
                        },
                        true);

        eventBus.register(this);

        pubSub.addMessageListener(pubSubEventMsg -> {
            PubSubEvent pubSubEvent = pubSubEventMsg.getMessageObject();
            if (pubSubEvent.getDestination() != null) {
                if (pubSubEvent.getDestination().equals("ALL")
                        || (pubSubEvent.getDestination().startsWith(myIdentity)
                                && pubSubEvent
                                        .getDestination()
                                        .endsWith(DefaultConfigService.this.warFile.toString()))) {
                    // We publish messages from hazelcast into this VM only if the intened WAR file is us.
                    logger.debug(() -> "Publishing event into this JVM " + pubSubEvent.generateEventDescription());
                    // In this case, we set the source as being the cluster to prevent republishing back into the
                    // cluster.
                    pubSubEvent.markSourceAsCluster();
                    eventBus.post(pubSubEvent);
                } else {
                    logger.debug(
                            () -> "Skipping publishing event into this JVM " + pubSubEvent.generateEventDescription()
                                    + " as destination is not me " + DefaultConfigService.this.warFile.toString());
                }
            } else {
                logger.debug(() -> "Skipping publishing event with null destination");
            }
        });

        logger.info("Done registering for changes to typeinfos");

        this.startupState = STARTUP_SEQUENCE.STARTUP_COMPLETE;
        configlogger.info("Start complete for webapp " + this.warFile);
    }

    @Override
    public STARTUP_SEQUENCE getStartupState() {
        return this.startupState;
    }

    @Subscribe
    public void updatePVSForThisAppliance(PVTypeInfoEvent event) {
        if (logger.isDebugEnabled()) logger.debug(() -> "Received pvTypeInfo change event for pv " + event.getPvName());
        PVTypeInfo typeInfo = event.getTypeInfo();
        String pvName = typeInfo.getPvName();
        if (typeInfo.getApplianceIdentity().equals(myApplianceInfo.getIdentity())) {
            if (event.getChangeType() == ChangeType.TYPEINFO_DELETED) {
                if (pvsForThisAppliance != null) {
                    if (pvsForThisAppliance.contains(pvName)) {
                        logger.debug(
                                "Removing pv " + pvName + " from the locally cached copy of pvs for this appliance");
                        pvsForThisAppliance.remove(pvName);
                        pausedPVsForThisAppliance.remove(pvName);
                        // For now, we do not anticipate many PVs being deleted from the cache to worry about keeping
                        // applianceAggregateInfo upto date...
                        // This may change later...
                        String[] parts = this.pvName2KeyConverter.breakIntoParts(pvName);
                        for (String part : parts) {
                            parts2PVNamesForThisAppliance.get(part).remove(pvName);
                        }
                    }
                }
            } else {
                if (pvsForThisAppliance != null) {
                    if (!pvsForThisAppliance.contains(pvName)) {
                        logger.debug(
                                () -> "Adding pv " + pvName + " to the locally cached copy of pvs for this appliance");
                        pvsForThisAppliance.add(pvName);
                        if (typeInfo.isPaused()) {
                            pausedPVsForThisAppliance.add(typeInfo.getPvName());
                        }
                        String[] parts = this.pvName2KeyConverter.breakIntoParts(pvName);
                        for (String part : parts) {
                            if (!parts2PVNamesForThisAppliance.containsKey(part)) {
                                parts2PVNamesForThisAppliance.put(part, new ConcurrentSkipListSet<String>());
                            }
                            parts2PVNamesForThisAppliance.get(part).add(pvName);
                        }
                        applianceAggregateInfo.addInfoForPV(pvName, typeInfo, this);
                    } else {
                        if (typeInfo.isPaused()) {
                            pausedPVsForThisAppliance.add(typeInfo.getPvName());
                        } else {
                            pausedPVsForThisAppliance.remove(typeInfo.getPvName());
                        }
                    }
                }
            }
        }
    }

    @Subscribe
    public void publishEventIntoCluster(PubSubEvent pubSubEvent) {
        if (pubSubEvent.isSourceCluster()) {
            logger.debug(() -> "Skipping publishing events from the cluster back into the cluster "
                    + pubSubEvent.generateEventDescription());
            return;
        }

        if (pubSubEvent.getDestination().startsWith(myIdentity)
                && pubSubEvent.getDestination().endsWith(this.warFile.toString())) {
            logger.debug(this.warFile + " - Skipping publishing event " + pubSubEvent.generateEventDescription()
                    + " meant for myself " + this.warFile.toString());
        } else {
            pubSubEvent.setSource(myIdentity);
            logger.debug(this.warFile + " - Publishing event from local event bus onto cluster "
                    + pubSubEvent.generateEventDescription());
            pubSub.publish(pubSubEvent);
        }
    }

    /**
     * Get the PVs that belong to this appliance and start archiving them
     * Needless to day, this gets done only in the engine.
     */
    private void archivePVSonStartup() {
        configlogger.debug(() -> "Start archiving PVs from persistence.");
        // To prevent broadcast storms, we pause for pausePerGroup seconds for every pausePerGroup PVs
        int currentPVCount = 0;
        int pausePerGroupPVCount = Integer.parseInt(this.getInstallationProperties().getProperty("org.epics.archiverappliance.engine.archivePVSonStartup.pausePerGroupPVCount", "2000"));
        int pausePerGroupPauseTimeInSeconds = Integer.parseInt(this.getInstallationProperties().getProperty("org.epics.archiverappliance.engine.archivePVSonStartup.pausePerGroupPauseTimeInSeconds", "2000"));
        boolean determineLastKnownEventFromStores = Boolean.parseBoolean(this.getInstallationProperties().getProperty("org.epics.archiverappliance.engine.archivePVSonStartup.determineLastKnownEventFromStores", "true"));

        for (String pvName : this.getPVsForThisAppliance()) {
            try {
                PVTypeInfo typeInfo = typeInfos.get(pvName);
                if (typeInfo == null) {
                    logger.error("On restart, cannot find typeinfo for pv " + pvName + ". Not archiving");
                    continue;
                }

                if (typeInfo.isPaused()) {
                    logger.debug(() -> "Skipping archiving paused PV " + pvName + " on startup");
                    this.engineContext.incrementPausedPVCount();
                    continue;
                }

                ArchDBRTypes dbrType = typeInfo.getDBRType();
                float samplingPeriod = typeInfo.getSamplingPeriod();
                SamplingMethod samplingMethod = typeInfo.getSamplingMethod();
                StoragePlugin firstDest = StoragePluginURLParser.parseStoragePlugin(typeInfo.getDataStores()[0], this);

                Instant lastKnownTimestamp = null;
                if(determineLastKnownEventFromStores) {
                    lastKnownTimestamp = typeInfo.determineLastKnownEventFromStores(this);
                    if(logger.isDebugEnabled()) {
                        logger.debug("Last known timestamp from ETL stores is for pv {} is {} ", pvName,
                            TimeUtils.convertToHumanReadableString(lastKnownTimestamp));
                    }
                }

                ArchiveEngine.archivePV(
                        pvName,
                        samplingPeriod,
                        samplingMethod,
                        firstDest,
                        this,
                        dbrType,
                        lastKnownTimestamp,
                        typeInfo.getControllingPV(),
                        typeInfo.getArchiveFields(),
                        typeInfo.getHostName(),
                        typeInfo.isUsePVAccess(),
                        typeInfo.isUseDBEProperties());
                currentPVCount++;
                if (currentPVCount % pausePerGroupPVCount == 0) {
                    logger.debug(
                            () -> "Sleeping for " + pausePerGroupPauseTimeInSeconds + " to prevent CA search storms");
                    Thread.sleep(pausePerGroupPauseTimeInSeconds * 1000);
                }
            } catch (Throwable t) {
                logger.error("Exception starting up archiving of PV " + pvName + ". Moving on to the next pv.", t);
            }
        }
        configlogger.debug("Started " + currentPVCount + " PVs from persistence.");
    }

    @Override
    public boolean isStartupComplete() {
        return startupState == STARTUP_SEQUENCE.STARTUP_COMPLETE;
    }

    @Override
    public Properties getInstallationProperties() {
        return archapplproperties;
    }

    @Override
    public Collection<ApplianceInfo> getAppliancesInCluster() {
        ArrayList<ApplianceInfo> sortedAppliances = new ArrayList<ApplianceInfo>();
        for (ApplianceInfo info : appliances.values()) {
            if (this.getWarFile() == WAR_FILE.MGMT) {
                if (appliancesInCluster.contains(info.getIdentity())) {
                    sortedAppliances.add(info);
                } else {
                    logger.debug(() -> "Skipping appliance that is in the persistence but not in the cluster"
                            + info.getIdentity());
                }
            } else {
                // For non-mgmt apps, we add all the appliances into this call.
                // This is because, the non-mgmt webapps are Hz clients; they do not receive the instance change events.
                // So, appliancesInCluster is always empty.
                sortedAppliances.add(info);
            }
        }

        sortedAppliances.sort(Comparator.comparing(ApplianceInfo::getIdentity));
        return sortedAppliances;
    }

    @Override
    public boolean hasClusterFinishedInitialization() {
        logger.info("Appliances that have loaded their PVs" + String.join(",", appliancesConfigLoaded.keySet()));
        return appliancesConfigLoaded.keySet().containsAll(appliances.keySet());
    }

    @Override
    public ApplianceInfo getMyApplianceInfo() {
        return myApplianceInfo;
    }

    @Override
    public ApplianceInfo getAppliance(String identity) {
        return appliances.get(identity);
    }

    @Override
    public Collection<String> getAllPVs() {
        List<PVApplianceCombo> sortedCombos = getSortedPVApplianceCombo();
        ArrayList<String> allPVs = new ArrayList<String>();
        for (PVApplianceCombo combo : sortedCombos) {
            allPVs.add(combo.pvName);
        }
        return allPVs;
    }

    @Override
    public ApplianceInfo getApplianceForPV(String pvName) {
        ApplianceInfo applianceInfo = pv2appliancemapping.get(pvName);
        if (applianceInfo == null && this.persistanceLayer != null) {
            try {
                PVTypeInfo typeInfo = this.persistanceLayer.getTypeInfo(pvName);
                if (typeInfo != null) {
                    applianceInfo = this.getAppliance(typeInfo.getApplianceIdentity());
                }
            } catch (IOException ex) {
                logger.error("Exception lookin up appliance for pv in persistence", ex);
            }
        }
        return applianceInfo;
    }

    @Override
    public Iterable<String> getPVsForAppliance(ApplianceInfo info) {
        String identity = info.getIdentity();
        List<PVApplianceCombo> sortedCombos = getSortedPVApplianceCombo();
        ArrayList<String> pvsForAppliance = new ArrayList<String>();
        for (PVApplianceCombo combo : sortedCombos) {
            if (combo.applianceIdentity.equals(identity)) {
                pvsForAppliance.add(combo.pvName);
            }
        }
        return pvsForAppliance;
    }

    @Override
    public Set<String> getPVsForThisAppliance() {
        if (pvsForThisAppliance != null) {
            logger.debug(() -> "Returning the locally cached copy of the pvs for this appliance");
            return Collections.unmodifiableSet(pvsForThisAppliance);
        } else {
            logger.debug(() -> "Generating the list of PVs for this appliance from pv2appliancemapping");
            ConcurrentSkipListSet<String> retval = new ConcurrentSkipListSet<String>();
            for (Map.Entry<String, ApplianceInfo> entry : pv2appliancemapping.entrySet()) {
                if(entry.getValue().getIdentity().equals(this.myIdentity)) {
                    retval.add(entry.getKey());
                }
            }
            return Collections.unmodifiableSet(retval);
        }
    }

    @Override
	public <T> Collection<T> projectPVTypeInfos(Set<String> pvNames, Projection<Map.Entry<String, PVTypeInfo>, T> projection) {
        IMap<String, PVTypeInfo> hztypeinfos = hzinstance.getMap(TYPEINFO);
        EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        Predicate<String, PVTypeInfo> predicate = e.key().in(pvNames.toArray(new String[0]));
        Collection<T> projectedTypeInfos = hztypeinfos.project(projection, predicate);
        return projectedTypeInfos;
    }

    @Override
    public boolean isBeingArchivedOnThisAppliance(String pvName) {
        boolean isField = PVNames.isFieldOrFieldModifier(pvName);
        String plainPVName = PVNames.channelNamePVName(pvName);
        String fieldName = PVNames.getFieldName(pvName);
        if (isField) {
            // If this is a field, we have two possibilities.
            // Either the plainPVname is being archived and the field is an extra field
            // Or the whole pv (with the field) is being archived.
            if (this.pvsForThisAppliance.contains(pvName)
                    || (this.pvsForThisAppliance.contains(plainPVName)
                            && Arrays.asList(this.getTypeInfoForPV(plainPVName).getArchiveFields())
                                    .contains(fieldName))) {
                return true;
            }

        } else {
            if (this.pvsForThisAppliance.contains(plainPVName)) {
                return true;
            }
        }

        if (this.aliasNamesToRealNames.containsKey(pvName)
                && this.pvsForThisAppliance.contains(this.aliasNamesToRealNames.get(pvName))) {
            return true;
        }

        if (this.aliasNamesToRealNames.containsKey(plainPVName)) {
            plainPVName = this.aliasNamesToRealNames.get(plainPVName);

            if (isField) {
                return this.pvsForThisAppliance.contains(PVNames.transferField(pvName, plainPVName))
                        || (this.pvsForThisAppliance.contains(plainPVName)
                                && Arrays.asList(this.getTypeInfoForPV(plainPVName)
                                                .getArchiveFields())
                                        .contains(fieldName));

            } else {
                return this.pvsForThisAppliance.contains(plainPVName);
            }
        }

        return false;
    }

    @Override
    public Set<String> getPVsForApplianceMatchingRegex(String nameToMatch) {
        logger.debug(() -> "Finding matching names for " + nameToMatch);
        LinkedList<String> fixedStringParts = new LinkedList<String>();
        String[] parts = this.pvName2KeyConverter.breakIntoParts(nameToMatch);
        Pattern fixedStringParttern = Pattern.compile("[a-zA-Z_0-9-]+");
        for (String part : parts) {
            if (fixedStringParttern.matcher(part).matches()) {
                logger.debug(() -> "Fixed string part " + part);
                fixedStringParts.add(part);
            } else {
                logger.debug(() -> "Regex string part " + part);
            }
        }

        if (!fixedStringParts.isEmpty()) {
            HashSet<String> ret = new HashSet<String>();
            HashSet<String> namesSubset = new HashSet<String>();
            // This reverse is probably specific to SLAC's namespace rules but it does make a big difference.
            // Perhaps we can use a more intelligent way of choosing the specific path thru the trie.
            Collections.reverse(fixedStringParts);
            for (String fixedStringPart : fixedStringParts) {
                ConcurrentSkipListSet<String> pvNamesForPart = parts2PVNamesForThisAppliance.get(fixedStringPart);
                if (pvNamesForPart != null) {
                    if (namesSubset.isEmpty()) {
                        namesSubset.addAll(pvNamesForPart);
                    } else {
                        namesSubset.retainAll(pvNamesForPart);
                    }
                }
            }
            logger.debug(() -> "Using fixed string path matching against names " + namesSubset.size());
            Pattern pattern = Pattern.compile(nameToMatch);
            for (String pvName : namesSubset) {
                if (pattern.matcher(pvName).matches()) {
                    ret.add(pvName);
                }
            }
            return ret;
        } else {
            // The use pattern did not have any fixed elements at all.
            // In this case we do brute force matching; should take longer.
            // This is also not optimal but probably don't want yet another list of PV's
            Pattern pattern = Pattern.compile(nameToMatch);
            HashSet<String> allNames = new HashSet<String>();
            HashSet<String> ret = new HashSet<String>();
            logger.debug(() -> "Using brute force pattern matching against names");
            for (ConcurrentSkipListSet<String> pvNamesForPart : parts2PVNamesForThisAppliance.values()) {
                allNames.addAll(pvNamesForPart);
            }
            for (String pvName : allNames) {
                if (pattern.matcher(pvName).matches()) {
                    ret.add(pvName);
                }
            }
            return ret;
        }
    }

    @Override
    public ApplianceAggregateInfo getAggregatedApplianceInfo(ApplianceInfo applianceInfo) throws IOException {
        if (applianceInfo.getIdentity().equals(myApplianceInfo.getIdentity()) && this.warFile == WAR_FILE.MGMT) {
            logger.debug(() -> "Returning local copy of appliance info for " + applianceInfo.getIdentity());
            return applianceAggregateInfo;
        } else {
            try {
                JSONObject aggregateInfo = GetUrlContent.getURLContentAsJSONObject(
                        applianceInfo.getMgmtURL() + "/getAggregatedApplianceInfo", false);
                JSONDecoder<ApplianceAggregateInfo> jsonDecoder = JSONDecoder.getDecoder(ApplianceAggregateInfo.class);
                ApplianceAggregateInfo retval = new ApplianceAggregateInfo();
                jsonDecoder.decode(aggregateInfo, retval);
                return retval;
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }

    @Override
    public void registerPVToAppliance(String pvName, ApplianceInfo applianceInfo) throws AlreadyRegisteredException {
        ApplianceInfo info = pv2appliancemapping.get(pvName);
        if (info != null) throw new AlreadyRegisteredException(info);
        pv2appliancemapping.put(pvName, applianceInfo);
    }

    @Override
    public PVTypeInfo getTypeInfoForPV(String pvName) {
        if (typeInfos.containsKey(pvName)) {
            logger.debug(() -> "Retrieving typeinfo from cache for pv " + pvName);
            return typeInfos.get(pvName);
        }

        return null;
    }

    @Override
    public void updateTypeInfoForPV(String pvName, PVTypeInfo typeInfo) {
        logger.debug(() -> "Updating typeinfo for " + pvName);
        if (!typeInfo.keyAlreadyGenerated()) {
            // This call should also typically set the chunk key in the type info.
            this.pvName2KeyConverter.convertPVNameToKey(pvName);
        }

        typeInfos.put(pvName, typeInfo);
    }

    @Override
    public void removePVFromCluster(String pvName) {
        logger.info("Removing PV from cluster.." + pvName);
        pv2appliancemapping.remove(pvName);
        pvsForThisAppliance.remove(pvName);
        typeInfos.remove(pvName);
        pausedPVsForThisAppliance.remove(pvName);
        String[] parts = this.pvName2KeyConverter.breakIntoParts(pvName);
        for (String part : parts) {
            ConcurrentSkipListSet<String> pvNamesForPart = parts2PVNamesForThisAppliance.get(part);
            if (pvNamesForPart != null) {
                pvNamesForPart.remove(pvName);
            }
        }
    }

    private static class PVApplianceCombo implements Comparable<PVApplianceCombo> {
        String applianceIdentity;
        String pvName;

        public PVApplianceCombo(String applianceIdentity, String pvName) {
            this.applianceIdentity = applianceIdentity;
            this.pvName = pvName;
        }

        @Override
        public int compareTo(PVApplianceCombo other) {
            if (this.applianceIdentity.equals(other.applianceIdentity)) {
                return this.pvName.compareTo(other.pvName);
            } else {
                return this.applianceIdentity.compareTo(other.applianceIdentity);
            }
        }
    }

    private List<PVApplianceCombo> getSortedPVApplianceCombo() {
        ArrayList<PVApplianceCombo> sortedCombos = new ArrayList<PVApplianceCombo>();
        for (Map.Entry<String, ApplianceInfo> entry : pv2appliancemapping.entrySet()) {
            sortedCombos.add(new PVApplianceCombo(entry.getValue().getIdentity(), entry.getKey()));
        }
        Collections.sort(sortedCombos);
        return sortedCombos;
    }

    @Override
    public void addToArchiveRequests(String pvName, UserSpecifiedSamplingParams userSpecifiedSamplingParams) {
        archivePVRequests.put(pvName, userSpecifiedSamplingParams);
        try {
            persistanceLayer.putArchivePVRequest(pvName, userSpecifiedSamplingParams);
        } catch (IOException ex) {
            logger.error("Exception adding request to persistence", ex);
        }
    }

    @Override
    public void updateArchiveRequest(String pvName, UserSpecifiedSamplingParams userSpecifiedSamplingParams) {
        try {
            if (persistanceLayer.getArchivePVRequest(pvName) != null) {
                archivePVRequests.put(pvName, userSpecifiedSamplingParams);
                persistanceLayer.putArchivePVRequest(pvName, userSpecifiedSamplingParams);
            } else {
                logger.error(
                        "Do not have user specified params for pv " + pvName + " in this appliance. Not updating.");
            }
        } catch (IOException ex) {
            logger.error("Exception updating request in persistence", ex);
        }
    }

    @Override
    public Set<String> getArchiveRequestsCurrentlyInWorkflow() {
        return new HashSet<String>(archivePVRequests.keySet());
    }

    @Override
    public boolean doesPVHaveArchiveRequestInWorkflow(String pvname) {
        return archivePVRequests.containsKey(pvname);
    }

    @Override
    public UserSpecifiedSamplingParams getUserSpecifiedSamplingParams(String pvName) {
        return archivePVRequests.get(pvName);
    }

    @Override
    public void archiveRequestWorkflowCompleted(String pvName) {
        archivePVRequests.remove(pvName);
        try {
            persistanceLayer.removeArchivePVRequest(pvName);
        } catch (IOException ex) {
            logger.error("Exception removing request from persistence", ex);
        }
    }

    @Override
    public int getInitialDelayBeforeStartingArchiveRequestWorkflow() {
        int initialDelayInSeconds = Integer.parseInt(this.getInstallationProperties().getProperty("org.epics.archiverappliance.mgmt.MgmtRuntimeState.initialDelayBeforeStartingArchiveRequests", "10"));
        return initialDelayInSeconds;
    }

    @Override
    public void addAlias(String aliasName, String realName) {
        aliasNamesToRealNames.put(aliasName, realName);
        try {
            persistanceLayer.putAliasNamesToRealName(aliasName, realName);
        } catch (IOException ex) {
            logger.error("Exception adding alias name to persistence " + aliasName, ex);
        }

        // Add aliases into the trie
        String[] parts = this.pvName2KeyConverter.breakIntoParts(aliasName);
        for (String part : parts) {
            if (!parts2PVNamesForThisAppliance.containsKey(part)) {
                parts2PVNamesForThisAppliance.put(part, new ConcurrentSkipListSet<String>());
            }
            parts2PVNamesForThisAppliance.get(part).add(aliasName);
        }
    }

    @Override
    public void removeAlias(String aliasName, String realName) {
        aliasNamesToRealNames.remove(aliasName);
        try {
            persistanceLayer.removeAliasName(aliasName, realName);
        } catch (IOException ex) {
            logger.error("Exception removing alias name from persistence " + aliasName, ex);
        }

        // Remove the aliasname from the trie
        String[] parts = this.pvName2KeyConverter.breakIntoParts(aliasName);
        for (String part : parts) {
            parts2PVNamesForThisAppliance.get(part).remove(aliasName);
        }
    }

    @Override
    public String getRealNameForAlias(String aliasName) {
        return aliasNamesToRealNames.get(aliasName);
    }

    @Override
    public List<String> getAliasesForRealName(String realName) { 
        try {
            return persistanceLayer.getAliasNamesForRealName(realName);
        } catch (IOException ex) {
            logger.error("Exception retrieving aliasnames for real name " + realName, ex);
            return new LinkedList<String>();
        }
    }

    @Override
    public List<String> getAllAliases() {
        return new ArrayList<String>(aliasNamesToRealNames.keySet());
    }

    private final String[] extraFields = new String[] {"MDEL", "ADEL", "SCAN", "RTYP"};

    @Override
    public String[] getExtraFields() {
        return extraFields;
    }

    @Override
    public Set<String> getRuntimeFields() {
        return runTimeFields;
    }

    @Override
    public PBThreeTierETLPVLookup getETLLookup() {
        return etlPVLookup;
    }

    @Override
    public RetrievalState getRetrievalRuntimeState() {
        return retrievalState;
    }

    @Override
    public boolean isShuttingDown() {
        return startupExecutor.isShutdown();
    }

    @Override
    public void addShutdownHook(Runnable runnable) {
        shutdownHooks.add(runnable);
    }

    private void runShutDownHooksAndCleanup() {
        LinkedList<Runnable> shutDnHooks = new LinkedList<Runnable>(this.shutdownHooks);
        Collections.reverse(shutDnHooks);
        logger.debug(() -> "Running shutdown hooks in webapp " + this.warFile);
        for (Runnable shutdownHook : shutDnHooks) {
            try {
                shutdownHook.run();
            } catch (Throwable t) {
                logger.warn("Exception shutting down service using shutdown hook " + shutdownHook.toString(), t);
            }
        }
        logger.debug(() -> "Done running shutdown hooks in webapp " + this.warFile);
    }

    @Override
    public void shutdownNow() {
        this.runShutDownHooksAndCleanup();
    }

    @Override
    public Map<String, String> getExternalArchiverDataServers() {
        return channelArchiverDataServers;
    }

    @Override
    public void addExternalArchiverDataServer(String serverURL, String archivesCSV) throws IOException {
        String[] archives = archivesCSV.split(",");
        boolean loadCAPVs = false;
        if (!this.getExternalArchiverDataServers().containsKey(serverURL)) {
            this.getExternalArchiverDataServers().put(serverURL, archivesCSV);
            loadCAPVs = true;
        } else {
            logger.info(serverURL + " already exists in the map. So, skipping loading PVs from the external server.");
        }

        // We always add to persistence; whether this is from the UI or from the other appliances in the cluster.
        if (this.persistanceLayer != null) {
            persistanceLayer.putExternalDataServer(serverURL, archivesCSV);
        }

        try {
            // We load PVs from the external server only if this is the first server starting up...
            if (loadCAPVs) {
                for (String archive : archives) {
                    loadExternalArchiverPVs(serverURL, archive);
                }
            }
        } catch (Exception ex) {
            logger.error("Exception adding Channel Archiver archives " + serverURL + " - " + archivesCSV, ex);
            throw new IOException(ex);
        }
    }

    @Override
    public void removeExternalArchiverDataServer(String serverURL, String archivesCSV) throws IOException {
        this.getExternalArchiverDataServers().remove(serverURL);
        // We always add to persistence; whether this is from the UI or from the other appliances in the cluster.
        if (this.persistanceLayer != null) {
            logger.info("Removing the channel archiver server " + serverURL + " from the persistent store.");
            persistanceLayer.removeExternalDataServer(serverURL, archivesCSV);
        }
    }

    /**
     * Given a external Archiver data server URL and an archive;
     * If this is a ChannelArchiver (archives != pbraw);
     * this adds the PVs in the Channel Archiver so that they can be proxied.
     * @param serverURL
     * @param archive
     * @throws IOException
     * @throws SAXException
     */
    private void loadExternalArchiverPVs(String serverURL, String archive) throws IOException, SAXException {
        if (archive.equals("pbraw")) {
            logger.debug(
                    "We do not load PV names from external EPICS archiver appliances. These can number in the multiple millions and the respone on retrieval is fast enough anyways");
            return;
        }

        ChannelArchiverDataServerInfo serverInfo = new ChannelArchiverDataServerInfo(serverURL, archive);
        NamesHandler handler = new NamesHandler();
        logger.debug(
                () -> "Getting list of PV's from Channel Archiver Server at " + serverURL + " using index " + archive);
        XMLRPCClient.archiverNames(serverURL, archive, handler);
        HashMap<String, List<ChannelArchiverDataServerPVInfo>> tempPVNames =
                new HashMap<String, List<ChannelArchiverDataServerPVInfo>>();
        long totalPVsProxied = 0;
        for (NamesHandler.ChannelDescription pvChannelDesc : handler.getChannels()) {
            String pvName = PVNames.normalizeChannelName(pvChannelDesc.getName());
            if (this.pv2ChannelArchiverDataServer.containsKey(pvName)) {
                List<ChannelArchiverDataServerPVInfo> alreadyExistingServers =
                        this.pv2ChannelArchiverDataServer.get(pvName);
                logger.debug(() -> "Adding new server to already existing ChannelArchiver server for " + pvName);
                addExternalCAServerToExistingList(alreadyExistingServers, serverInfo, pvChannelDesc);
                tempPVNames.put(pvName, alreadyExistingServers);
            } else if (tempPVNames.containsKey(pvName)) {
                List<ChannelArchiverDataServerPVInfo> alreadyExistingServers = tempPVNames.get(pvName);
                logger.debug(
                        "Adding new server to already existing ChannelArchiver server (in tempspace) for " + pvName);
                addExternalCAServerToExistingList(alreadyExistingServers, serverInfo, pvChannelDesc);
                tempPVNames.put(pvName, alreadyExistingServers);
            } else {
                List<ChannelArchiverDataServerPVInfo> caServersForPV = new ArrayList<ChannelArchiverDataServerPVInfo>();
                caServersForPV.add(new ChannelArchiverDataServerPVInfo(
                        serverInfo, pvChannelDesc.getStartSec(), pvChannelDesc.getEndSec()));
                tempPVNames.put(pvName, caServersForPV);
            }

            if (tempPVNames.size() > 1000) {
                this.pv2ChannelArchiverDataServer.putAll(tempPVNames);
                totalPVsProxied += tempPVNames.size();
                tempPVNames.clear();
            }
        }
        if (!tempPVNames.isEmpty()) {
            this.pv2ChannelArchiverDataServer.putAll(tempPVNames);
            totalPVsProxied += tempPVNames.size();
            tempPVNames.clear();
        }
        if (logger.isDebugEnabled())
            logger.debug("Proxied a total of " + totalPVsProxied + " from server " + serverURL + " using archive "
                    + archive);
    }

    private void initializeFailoverServerCache() {
        Map<String, String> existingCAServers = this.getExternalArchiverDataServers();
        for (String serverURL : existingCAServers.keySet()) {
            String archiveType = existingCAServers.get(serverURL);
            if (archiveType.equals("pbraw")) {
                logger.debug(() -> "Checking to see if " + serverURL + " is used for failover");
                try {
                    URI serverURI = new URI(serverURL);
                    HashMap<String, String> queryNVPairs = URIUtils.parseQueryString(serverURI);
                    if (!queryNVPairs.isEmpty() && queryNVPairs.containsKey("mergeDuringRetrieval")) {
                        configlogger.info("Merging data from " + serverURL + " during data retrieval");
                        failoverPVs.put(
                                serverURL,
                                CacheBuilder.newBuilder()
                                        .expireAfterWrite(86400, TimeUnit.SECONDS)
                                        .build(new CacheLoader<>() {
                                            public Boolean load(String pvName) {
                                                String areWeURL = serverURL.split("\\?")[0] + "/"
                                                        + "bpl/areWeArchivingPV?pv=" + pvName;
                                                logger.debug(() -> "Checking to see if " + serverURL
                                                        + " is archiving PV " + pvName + " using " + areWeURL);
                                                try {
                                                    JSONObject areWeResp =
                                                            GetUrlContent.getURLContentAsJSONObject(areWeURL);
                                                    return Boolean.valueOf((String) areWeResp.get("status"));
                                                } catch (Exception ex) {
                                                    logger.error(
                                                            "Exception checking to see if " + serverURL
                                                                    + " is archiving PV " + pvName,
                                                            ex);
                                                }
                                                return false;
                                            }
                                        }));
                    }
                } catch (Exception ex) {
                    logger.error("Exception parsing external server URL " + serverURL, ex);
                }
            }
        }
    }

    private static void addExternalCAServerToExistingList(
            List<ChannelArchiverDataServerPVInfo> alreadyExistingServers,
            ChannelArchiverDataServerInfo serverInfo,
            NamesHandler.ChannelDescription pvChannelDesc) {
        List<ChannelArchiverDataServerPVInfo> copyOfAlreadyExistingServers = new LinkedList<>();
        for (ChannelArchiverDataServerPVInfo alreadyExistingServer : alreadyExistingServers) {
            if (alreadyExistingServer.getServerInfo().equals(serverInfo)) {
                logger.debug(() -> "Removing a channel archiver server that already exists " + alreadyExistingServer);
            } else {
                copyOfAlreadyExistingServers.add(alreadyExistingServer);
            }
        }

        int beforeCount = alreadyExistingServers.size();
        alreadyExistingServers.clear();
        alreadyExistingServers.addAll(copyOfAlreadyExistingServers);

        // Readd the CA server - this should take into account any updated start times, end times and so on.
        alreadyExistingServers.add(new ChannelArchiverDataServerPVInfo(
                serverInfo, pvChannelDesc.getStartSec(), pvChannelDesc.getEndSec()));

        int afterCount = alreadyExistingServers.size();
        logger.debug(() -> "We had " + beforeCount + " and now we have " + afterCount
                + " when adding external ChannelArchiver server");

        // Sort the servers by ascending time stamps before adding it back.
        ChannelArchiverDataServerPVInfo.sortServersBasedOnStartAndEndSecs(alreadyExistingServers);
    }

    @Override
    public List<ChannelArchiverDataServerPVInfo> getChannelArchiverDataServers(String pvName) {
        String normalizedPVName = PVNames.normalizeChannelName(pvName);
        logger.debug(() -> "Looking for CA sever for pv " + normalizedPVName);
        return pv2ChannelArchiverDataServer.get(normalizedPVName);
    }

    @Override
    public PolicyConfig computePolicyForPV(String pvName, MetaInfo metaInfo, UserSpecifiedSamplingParams userSpecParams)
            throws IOException {
        try (InputStream is = this.getPolicyText()) {
            logger.debug(() -> "Computing policy for pvName");
            HashMap<String, Object> pvInfo = new HashMap<String, Object>();
            pvInfo.put("dbrtype", metaInfo.getArchDBRTypes().toString());
            pvInfo.put("elementCount", metaInfo.getCount());
            pvInfo.put("eventRate", metaInfo.getEventRate());
            pvInfo.put("eventCount", metaInfo.getEventCount());
            pvInfo.put("storageRate", metaInfo.getStorageRate());
            pvInfo.put("aliasName", metaInfo.getAliasName());
            if (userSpecParams != null && userSpecParams.getPolicyName() != null) {
                logger.debug(() -> "Passing user override of policy " + userSpecParams.getPolicyName()
                        + " as the dict entry policyName");
                pvInfo.put("policyName", userSpecParams.getPolicyName());
            }
            if (userSpecParams.getControllingPV() != null) {
                pvInfo.put("controlPV", userSpecParams.getControllingPV());
            }

            HashMap<String, String> otherMetaInfo = metaInfo.getOtherMetaInfo();
            for (String otherMetaInfoKey : this.getExtraFields()) {
                if (otherMetaInfo.containsKey(otherMetaInfoKey)) {
                    if (otherMetaInfoKey.equals("ADEL") || otherMetaInfoKey.equals("MDEL")) {
                        try {
                            pvInfo.put(otherMetaInfoKey, Double.parseDouble(otherMetaInfo.get(otherMetaInfoKey)));
                        } catch (Exception ex) {
                            logger.error("Exception adding MDEL and ADEL to the info", ex);
                        }
                    } else {
                        pvInfo.put(otherMetaInfoKey, otherMetaInfo.get(otherMetaInfoKey));
                    }
                }
            }

            if (logger.isDebugEnabled()) {
                StringBuilder buf = new StringBuilder();
                buf.append("Before computing policy for");
                buf.append(pvName);
                buf.append(" pvInfo is \n");
                for (String key : pvInfo.keySet()) {
                    buf.append(key);
                    buf.append("=");
                    buf.append(pvInfo.get(key));
                    buf.append("\n");
                }
                logger.debug(buf.toString());
            }

            try {
                // We only have one policy in the cache...
                ExecutePolicy executePolicy = theExecutionPolicy.get("ThePolicy");
                return executePolicy.computePolicyForPV(pvName, pvInfo);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                logger.error("Exception executing policy for pv " + pvName, cause);
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                } else {
                    throw new IOException(cause);
                }
            }
        }
    }

    @Override
    public HashMap<String, String> getPoliciesInInstallation() throws IOException {
        try (ExecutePolicy executePolicy = new ExecutePolicy(this)) {
            return executePolicy.getPolicyList();
        }
    }

    @Override
    public List<String> getFieldsArchivedAsPartOfStream() throws IOException {
        try {
            ExecutePolicy executePolicy = theExecutionPolicy.get("ThePolicy");
            return executePolicy.getFieldsArchivedAsPartOfStream();
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public TypeSystem getArchiverTypeSystem() {
        return new PBTypeSystem();
    }

    private boolean finishedLoggingPolicyLocation = false;

    @Override
    public InputStream getPolicyText() throws IOException {
        String policiesPyFile = System.getProperty(ARCHAPPL_POLICIES);
        if (policiesPyFile == null) {
            policiesPyFile = System.getenv(ARCHAPPL_POLICIES);
            if (policiesPyFile != null) {
                if (!finishedLoggingPolicyLocation) {
                    configlogger.info("Obtained policies location from environment " + policiesPyFile);
                    finishedLoggingPolicyLocation = true;
                }
                return new FileInputStream(policiesPyFile);
            } else {
                logger.info("Looking for /WEB-INF/classes/policies.py in classpath");
                if (servletContext != null) {
                    if (!finishedLoggingPolicyLocation) {
                        configlogger.info("Using policies file /WEB-INF/classes/policies.py found in classpath");
                        finishedLoggingPolicyLocation = true;
                    }
                    return servletContext.getResourceAsStream("/WEB-INF/classes/policies.py");
                } else {
                    throw new IOException(
                            "Cannot determine location of policies file as both servlet context and webInfClassesFolder are null");
                }
            }
        } else {
            if (!finishedLoggingPolicyLocation) {
                configlogger.info("Obtained policies location from system property " + policiesPyFile);
                finishedLoggingPolicyLocation = true;
            }
            return new FileInputStream(policiesPyFile);
        }
    }

    @Override
    public EngineContext getEngineContext() {
        return engineContext;
    }

    @Override
    public MgmtRuntimeState getMgmtRuntimeState() {
        return mgmtRuntime;
    }

    @Override
    public WAR_FILE getWarFile() {
        return warFile;
    }

    /**
     * Return a string representation of the member.
     * @param member
     * @return
     */
    private String getMemberKey(Member member) {
        // We use deprecated versions of the methods as the non-deprecated versions do not work as of 2.0x?
        return member.getSocketAddress().toString();
    }

    @Override
    public EventBus getEventBus() {
        return eventBus;
    }

    @Override
    public PVNameToKeyMapping getPVNameToKeyConverter() {
        return pvName2KeyConverter;
    }

    /**
     * Load typeInfos into the cluster hashmaps from the persistence layer on startup.
     * To avoid overwhelming the cluster, we batch the loads
     */
    private void loadTypeInfosFromPersistence() {
        try {
            configlogger.info("Loading PVTypeInfo from persistence");
            List<String> upgradedPVs = new LinkedList<String>();
            List<PVTypeInfo> pvTypeInfos = persistanceLayer.getAllTypeInfosForAppliance(myIdentity);
            HashMap<String, PVTypeInfo> newTypeInfos = new HashMap<String, PVTypeInfo>();
            HashMap<String, ApplianceInfo> newPVMappings = new HashMap<String, ApplianceInfo>();
            int objectCount = 0;
            int batch = 0;
            int clusterPVCount = 0;
            for (PVTypeInfo typeInfo : pvTypeInfos) {
                if (typeInfo.getApplianceIdentity().equals(myIdentity)) {
                    // Here's where we put schema update logic
                    upgradeTypeInfo(typeInfo, upgradedPVs);

                    String pvName = typeInfo.getPvName();
                    newTypeInfos.put(pvName, typeInfo);
                    newPVMappings.put(pvName, appliances.get(typeInfo.getApplianceIdentity()));
                    pvsForThisAppliance.add(pvName);
                    if (typeInfo.isPaused()) {
                        pausedPVsForThisAppliance.add(pvName);
                    }
                    String[] parts = this.pvName2KeyConverter.breakIntoParts(pvName);
                    for (String part : parts) {
                        if (!parts2PVNamesForThisAppliance.containsKey(part)) {
                            parts2PVNamesForThisAppliance.put(part, new ConcurrentSkipListSet<String>());
                        }
                        parts2PVNamesForThisAppliance.get(part).add(pvName);
                    }

                    objectCount++;
                }
                // Add in batch sizes of 1000 or so...
                if (objectCount > 1000) {
                    this.typeInfos.putAll(newTypeInfos);
                    this.pv2appliancemapping.putAll(newPVMappings);
                    for (String pvName : newTypeInfos.keySet()) {
                        applianceAggregateInfo.addInfoForPV(pvName, newTypeInfos.get(pvName), this);
                    }
                    clusterPVCount += newTypeInfos.size();
                    newTypeInfos = new HashMap<String, PVTypeInfo>();
                    newPVMappings = new HashMap<String, ApplianceInfo>();
                    objectCount = 0;
                    logger.debug("Adding next batch of PVs " + batch++);
                }
            }

            if (!newTypeInfos.isEmpty()) {
                logger.debug(() -> "Adding final batch of PVs from persistence");
                this.typeInfos.putAll(newTypeInfos);
                this.pv2appliancemapping.putAll(newPVMappings);
                for (String pvName : newTypeInfos.keySet()) {
                    applianceAggregateInfo.addInfoForPV(pvName, newTypeInfos.get(pvName), this);
                }
                clusterPVCount += newTypeInfos.size();
            }

            configlogger.info("Done loading " + clusterPVCount + " PVs from persistence into cluster");

            for (String upgradedPVName : upgradedPVs) {
                logger.debug(() -> "PV " + upgradedPVName + "'s schema was upgraded");
                persistanceLayer.putTypeInfo(upgradedPVName, getTypeInfoForPV(upgradedPVName));
                logger.debug(() -> "Done persisting upgraded PV's " + upgradedPVName + "'s typeInfo");
            }
        } catch (Exception ex) {
            configlogger.error("Exception loading PVs from persistence", ex);
        }
    }

    /**
     * The occasional upgrade to PVTypeInfo schema is handed here.
     * @param typeInfo - Typeinfo to be upgraded
     * @param upgradedPVs - Add the pvName here if we actually did an upgrade to the typeInfo.
     */
    private void upgradeTypeInfo(PVTypeInfo typeInfo, List<String> upgradedPVs) {
        // We added the chunkKey to typeInfo to permanently remember the key mapping to accomodate slowly changing key
        // mappings.
        // This could be a possibility after talking to SPEAR folks.
        if (!typeInfo.keyAlreadyGenerated()) {
            typeInfo.setChunkKey(this.pvName2KeyConverter.convertPVNameToKey(typeInfo.getPvName()));
            upgradedPVs.add(typeInfo.getPvName());
        }
    }

    /**
     * Load alias mappings from persistence on startup in batches
     */
    private void loadAliasesFromPersistence() {
        try {
            configlogger.info("Loading aliases from persistence");
            List<String> pvNamesFromPersistence = persistanceLayer.getAliasNamesToRealNamesKeys();
            HashMap<String, String> newAliases = new HashMap<>();
            int clusterPVCount = 0;
            for (String pvNameFromPersistence : pvNamesFromPersistence) {
                String realName = persistanceLayer.getAliasNamesToRealName(pvNameFromPersistence);
                if (this.pvsForThisAppliance.contains(realName)) {
                    newAliases.put(pvNameFromPersistence, realName);
                    // Add the alias into the trie
                    String[] parts = this.pvName2KeyConverter.breakIntoParts(pvNameFromPersistence);
                    for (String part : parts) {
                        if (!parts2PVNamesForThisAppliance.containsKey(part)) {
                            parts2PVNamesForThisAppliance.put(part, new ConcurrentSkipListSet<>());
                        }
                        parts2PVNamesForThisAppliance.get(part).add(pvNameFromPersistence);
                    }
                }
            }

            if (!newAliases.isEmpty()) {
                logger.debug(() -> "Adding final batch of aliases from persistence");
                this.aliasNamesToRealNames.putAll(newAliases);
                clusterPVCount += newAliases.size();
            }

            configlogger.info("Done loading " + clusterPVCount + " aliases from persistence into cluster ");
        } catch (Exception ex) {
            configlogger.error("Exception loading aliases from persistence", ex);
        }
    }

    /**
     * Load any pending archive requests that have not been fulfilled yet on startup
     * Also, start their workflows..
     */
    private void loadArchiveRequestsFromPersistence() {
        try {
            configlogger.info("Loading archive requests from persistence");
            List<String> pvNamesFromPersistence = persistanceLayer.getArchivePVRequestsKeys();
            HashMap<String, UserSpecifiedSamplingParams> newArchiveRequests = new HashMap<>();
            int clusterPVCount = 0;
            for (String pvNameFromPersistence : pvNamesFromPersistence) {
                UserSpecifiedSamplingParams userSpecifiedParams =
                        persistanceLayer.getArchivePVRequest(pvNameFromPersistence);
                // We should not need to add an appliance check here.. However, if after production deployment, we
                // determine we need to do so; this is the right place.
                newArchiveRequests.put(pvNameFromPersistence, userSpecifiedParams);
                // Add in batch sizes of 1000 or so...
            }

            if (!newArchiveRequests.isEmpty()) {
                logger.debug(() -> "Adding final batch of archive pv requests from persistence");
                this.archivePVRequests.putAll(newArchiveRequests);
                clusterPVCount += newArchiveRequests.size();
            }

            configlogger.info("Done loading " + clusterPVCount + " archive pv requests from persistence into cluster ");
        } catch (Exception ex) {
            configlogger.error("Exception loading archive pv requests from persistence", ex);
        }
    }

    /**
     * Initialize the persistenceLayer using environment/system property.
     * By default, initialize the MySQLPersistence
     * @throws ConfigException
     */
    private void initializePersistenceLayer() throws ConfigException {
        String persistenceFromEnv = System.getenv(ARCHAPPL_PERSISTENCE_LAYER);
        if (persistenceFromEnv == null || persistenceFromEnv.isEmpty()) {
            persistenceFromEnv = System.getProperty(ARCHAPPL_PERSISTENCE_LAYER);
        }
        if (persistenceFromEnv == null || persistenceFromEnv.isEmpty()) {
            logger.info("Using MYSQL for persistence; we expect to find a JNDI connection pool called jdbc/archappl");
            persistanceLayer = new MySQLPersistence();
        } else {
            try {
                logger.info("Using persistence provided by class " + persistenceFromEnv);
                persistanceLayer = (ConfigPersistence) getClass()
                        .getClassLoader()
                        .loadClass(persistenceFromEnv)
                        .getConstructor()
                        .newInstance();
            } catch (Exception ex) {
                throw new ConfigException("Exception initializing persistence layer using " + persistenceFromEnv, ex);
            }
        }
    }

    public ProcessMetrics getProcessMetrics() {
        return processMetrics;
    }

    @Override
    public String getWebInfFolder() {
        return servletContext.getRealPath("WEB-INF/");
    }

    private void loadExternalServersFromPersistence() {
        try {
            configlogger.info("Loading external servers from persistence");
            List<String> externalServerKeys = persistanceLayer.getExternalDataServersKeys();
            for (String serverUrl : externalServerKeys) {
                String archivesCSV = persistanceLayer.getExternalDataServer(serverUrl);
                if (this.getExternalArchiverDataServers().containsKey(serverUrl)) {
                    configlogger.info("Skipping adding " + serverUrl
                            + " on this appliance as another appliance has already added it");
                } else {
                    this.getExternalArchiverDataServers().put(serverUrl, archivesCSV);
                    String[] archives = archivesCSV.split(",");

                    this.startupExecutor.schedule(
                            () -> {
                                try {
                                    for (String archive : archives) {
                                        loadExternalArchiverPVs(serverUrl, archive);
                                    }
                                } catch (Exception ex) {
                                    logger.error(
                                            "Exception adding Channel Archiver archives " + serverUrl + " - "
                                                    + archivesCSV,
                                            ex);
                                }
                            },
                            15,
                            TimeUnit.SECONDS);
                }
            }
            configlogger.info("Done loading external servers from persistence ");
        } catch (Exception ex) {
            configlogger.error("Exception loading external servers from persistence", ex);
        }
    }

    private void registerForNewExternalServers(IMap<Object, Object> dataServerMap) {
        dataServerMap.addEntryListener(
                (EntryAddedListener<Object, Object>) arg0 -> {
                    String url = (String) arg0.getKey();
                    String archivesCSV = (String) arg0.getValue();
                    try {
                        addExternalArchiverDataServer(url, archivesCSV);
                    } catch (Exception ex) {
                        logger.error("Exception syncing external data server " + url + archivesCSV, ex);
                    }
                },
                true);
        dataServerMap.addEntryListener(
                (EntryRemovedListener<Object, Object>) arg0 -> {
                    String url = (String) arg0.getKey();
                    String archivesCSV = (String) arg0.getValue();
                    try {
                        removeExternalArchiverDataServer(url, archivesCSV);
                    } catch (Exception ex) {
                        logger.error("Exception syncing external data server " + url + archivesCSV, ex);
                    }
                },
                true);
    }

    @Override
    public Set<String> getPausedPVsInThisAppliance() {
        if (pausedPVsForThisAppliance != null) {
            logger.debug(() -> "Returning the locally cached copy of the paused pvs for this appliance");
            return pausedPVsForThisAppliance;
        } else {
            logger.debug(() -> "Fetching the list of paused PVs for this appliance from the mgmt app");
            JSONArray pvs = GetUrlContent.getURLContentAsJSONArray(
                    myApplianceInfo.getMgmtURL() + "/getPausedPVsForThisAppliance");
            HashSet<String> retval = new HashSet<String>();
            for (Object pv : pvs) {
                retval.add((String) pv);
            }
            return retval;
        }
    }

    @Override
    public void refreshPVDataFromChannelArchiverDataServers() {
        Map<String, String> existingCAServers = this.getExternalArchiverDataServers();
        for (String serverURL : existingCAServers.keySet()) {
            String archivesCSV = existingCAServers.get(serverURL);
            String[] archives = archivesCSV.split(",");

            try {
                for (String archive : archives) {
                    loadExternalArchiverPVs(serverURL, archive);
                }
            } catch (Throwable ex) {
                logger.error("Exception adding Channel Archiver archives " + serverURL + " - " + archivesCSV, ex);
            }
        }
    }

    @Override
    public String getFailoverApplianceURL(String pvName) {
        for (String serverURL : failoverPVs.keySet()) {
            try {
                if (Boolean.TRUE.equals(failoverPVs.get(serverURL).get(pvName))) {
                    return serverURL;
                }
            } catch (ExecutionException e) {
                logger.error("Exception checking for failover for PV " + pvName + " on server " + serverURL);
            }
        }
        return null;
    }

    @Override
    public Set<String> getFailoverServerURLs() {
        return failoverPVs.keySet();
    }

    @Override
    public void resetFailoverCaches() {
        failoverPVs.clear();
        initializeFailoverServerCache();
    }

    @Override
    public boolean getNamedFlag(String name) {
        if (name == null) {
            return false;
        }
        if (name.equalsIgnoreCase("false")) {
            return false;
        }
        if (name.equalsIgnoreCase("true")) {
            return true;
        }
        if (namedFlags.containsKey(name)) {
            return namedFlags.get(name);
        }
        // If we don't know about this named flag, we return false;
        return false;
    }

    @Override
    public void setNamedFlag(String name, boolean value) {
        namedFlags.put(name, value);
    }

    @Override
    public Set<String> getNamedFlagNames() {
        return namedFlags.keySet();
    }

    @Override
    public long getTimeOfAppserverStartup() {
        return this.appserverStartEpochSeconds;
    }

    @Override
    public void getAllExpandedNames(Consumer<String> func) {
        Collection<String> allPVs = this.getAllPVs();
        // Add fields and the VAL field
        for (String pvName : allPVs) {
            func.accept(pvName);
            if (!PVNames.isFieldOrFieldModifier(pvName)) {
                func.accept(pvName + ".VAL");
                PVTypeInfo typeInfo = this.getTypeInfoForPV(pvName);
                if (typeInfo != null) {
                    for (String fieldName : typeInfo.getArchiveFields()) {
                        func.accept(pvName + "." + fieldName);
                    }
                }
            }
        }
        List<String> allAliases = this.getAllAliases();
        for (String pvName : allAliases) {
            func.accept(pvName);
            if (!PVNames.isFieldOrFieldModifier(pvName)) {
                func.accept(pvName + ".VAL");
                PVTypeInfo typeInfo = this.getTypeInfoForPV(pvName);
                if (typeInfo != null) {
                    for (String fieldName : typeInfo.getArchiveFields()) {
                        func.accept(pvName + "." + fieldName);
                    }
                }
            }
        }
        for (String pvName : this.getArchiveRequestsCurrentlyInWorkflow()) {
            func.accept(pvName);
            if (!PVNames.isFieldOrFieldModifier(pvName)) {
                func.accept(pvName + ".VAL");
            }
        }
    }
}
