/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.NoDataException;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.mergededup.TimeSpanLimitEventStream;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.etl.ConversionFunction;
import org.epics.archiverappliance.etl.ETLBulkStream;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.StorageMetrics;
import org.epics.archiverappliance.etl.StorageMetricsContext;
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.ui.URIUtils;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBPathNameUtility.StartEndTimeFromName;

/**
 * The plain PB storage plugin stores data in a chunk per PV per partition in sequential form.
 * No index is maintained, simple search algorithms are used to locate events.
 * This plugin has these configuration parameters.
 * <dl>
 * <dt>name</dt><dd>This serves to identify this plugin; mandatory</dd>
 * <dt>rootFolder</dt><dd>This serves as the rootFolder that is prepended to the path generated for a PV+chunk ; mandatory. 
 * One can use environment variables here; for example, <code>pb://localhost?name=STS&amp;rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}&amp;partitionGranularity=PARTITION_HOUR</code> where the value for ${ARCHAPPL_SHORT_TERM_FOLDER} is picked up from the environment/system properties.
 * </dd>
 * <dt>partitionGranularity</dt><dd>Defines the time partition granularity for this plugin. For example, if the granularity <code>PARTITION_HOUR</code>, then a new chunk is created for each hour of data. The partitions are clean; that is, they contain data only for that partition. It is possible to predict which chunk contains data for a particular instant in time and which chunks contain data for a particular time period. This is a mandatory field.</dd>
 * <dt>compress</dt><dd>This is an optional field that defines the compression mode. 
 * The support for zip compression is experimental. 
 * If the zip compression is used, the <code>rootfolder</code> is prepended with <code>{@link org.epics.archiverappliance.utils.nio.ArchPaths#ZIP_PREFIX ZIP_PREFIX}</code>.
 * If this is absent in the <code>rootfolder</code>, the initialization code automatically adds it in.
 * </dd>
 * <dt>hold &amp; gather</dt><dd><code>hold</code> and <code>gather</code> are optional fields that work together to implement high/low watermarks for data transfer.
 * By default, both <code>hold</code> and <code>gather</code> are 0 which leads to data being transferred out of this plugin as soon as the partition boundary is reached.
 * You can <code>hold</code> a certain number of partitions in this store (perhaps because this store is a high performing one). 
 * In this case, ETL does not start until the first event in this store is older than <code>hold</code> partitions.
 * Once ETL begins, you can transfer <code>gather</code> partitions at a time.
 * For example, <code>hold=5&amp;gather=3</code> lets you keep at least <code>5-3=2</code> partitions in this store. ETL kicks in once the oldest event is older than than <code>5</code> partitions and data is moved <code>3</code> partitions at a time. 
 * </dd>
 * <dt>pp</dt><dd>An optional parameter, this contains a list of {@link org.epics.archiverappliance.retrieval.postprocessors.PostProcessor post processing operators} that are computed and cached during ETL.
 * During retrieval, if an exact match is found, then the data from the cached copy is used (greatly improving retrieval performance). 
 * Otherwise, the post processor is applied and the data is computed at runtime.
 * To specify multiple post processors, use standard URL syntax like so <code>pp=rms&amp;pp=mean_3600</code>
 * </dd>
 * <dt>consolidateOnShutdown</dt><dd>This lets you control if ETL should push data to the subsequent store on appserver shutdown. This is useful if you are using a RAMDisk for the short term store.</dd>
 * <dt>reducedata</dt><dd>An optional parameter; use this parameter to reduce the data as you move it into this store. You can use any of the <a href="http://slacmshankar.github.io/epicsarchiver_docs/userguide.html#post_processing">post processors</a> that can be used with the <code>pp</code> argument.
 * For example, if you define the LTS as <code>pb://localhost?name=LTS&amp;rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&amp;partitionGranularity=PARTITION_YEAR&amp;reducedata=firstSample_3600</code>, then when moving data into this store, ETL will apply the <code>firstSample_3600</code> operator on the raw data to reduce the data and store only the reduced data.
 * The difference between this parameter and the <code>pp</code> parameter is that in the <code>reducedata</code> case, only the reduced data is stored. The raw data is thrown away.
 * If you specify both the <code>pp</code> and the <code>reducedata</code>, you may get unpredictable results because the raw data is necessary to precompute the caches. 
 * </dd>
 * <dt>etlIntoStoreIf</dt><dd>An optional parameter; use this parameter to control if ETL should move data into this store. 
 * If the named flag specified by this parameter is false, this plugin will behave like the blackhole plugin (and you will lose data).
 * Note that named flags are false by default; so the default behavior if you specify this flag and forget to the set the named flag is to lose data.
 * If you don't set this flag at all; then this plugin behaves normally and will accept all the ETL data coming in.
 * For example, if you add a <code>etlIntoStoreIf=testFlag</code>; then data will be moved into this store only if the value of the named flag <code>testFlag</code> is true.
 * </dd>
 * <dt>etlOutofStoreIf</dt><dd>An optional parameter; use this parameter to control if ETL should move data out of this store. 
 * If the named flag specified by this parameter is false, this plugin will behave like a bag of holding and accumulate all the data it can.
 * Note that named flags are false by default; so the default behavior if you specify this flag and forget to the set the named flag is to collect data till you run out of space.
 * If you don't set this flag at all; then this plugin behaves normally and will move data out as before.
 * For example, if you add a <code>etlOutofStoreIf=testFlag</code>; then data will be moved ouf of this store only if the value of the named flag <code>testFlag</code> is true.
 * </dd>
 * </dl>
 * @author mshankar
 *
 */
public class PlainPBStoragePlugin implements StoragePlugin, ETLSource, ETLDest, StorageMetrics {
	private static Logger logger = LogManager.getLogger(PlainPBStoragePlugin.class.getName());

	public static final String PB_EXTENSION = ".pb";
	public static final String APPEND_EXTENSION = ".pbappend";

	private String rootFolder = "/tmp";
	private String name;
	private ConfigService configService;
	private PVNameToKeyMapping pv2key;
	/**
	 * Support for ZIP_PER_PV is still experimental.
	 * @author mshankar
	 */
	public enum CompressionMode {
		NONE,
		ZIP_PER_PV
	}
	

	private String desc = "Plain PB plugin";
	// By default, we partition based on a year's boundary.
	PartitionGranularity partitionGranularity = PartitionGranularity.PARTITION_YEAR;
	/**
	 * Should we backup the affected partitions before letting ETL touch that partition.
	 * This has some performance implications as we will be copying the file on each run
	 */
	private boolean backupFilesBeforeETL = false;

	private CompressionMode compressionMode = CompressionMode.NONE;
	
	private List<String> postProcessorUserArgs = null;
	private String reducedataPostProcessor = null;
	
	private ConcurrentHashMap<String, AppendDataStateData> appendDataStates = new ConcurrentHashMap<String, AppendDataStateData>();
	
	private int holdETLForPartions = 0;
	private int gatherETLinPartitions = 0;
	private boolean consolidateOnShutdown = false;
	/**
	 * Most of the time; this will be null.
	 * However; if specified; we should use the value of the named flag identified by this variable to control if this plugin behaves like a black hole plugin or not.
	 */
	private String etlIntoStoreIf;
	private String etlOutofStoreIf;

	
	public List<Callable<EventStream>> getDataForPV(BasicContext context, String pvName, Timestamp startTime, Timestamp endTime) throws IOException {
		DefaultRawPostProcessor postProcessor = new DefaultRawPostProcessor();
		return getDataForPV(context, pvName, startTime, endTime, postProcessor);
	}

	/* 
	 *  (non-Javadoc)
	 * @see org.epics.archiverappliance.Reader#getDataForPV(java.lang.String, java.sql.Timestamp, java.sql.Timestamp, boolean)
	 */
	@Override
	public List<Callable<EventStream>> getDataForPV(BasicContext context, String pvName, Timestamp startTime, Timestamp endTime, PostProcessor postProcessor) throws IOException {
		try {
			Path[] paths = null;
			String extension = "." + postProcessor.getExtension();
			boolean userWantsRawData = extension.equals(PB_EXTENSION);
			boolean askingForProcessedDataButAbsentInCache = false;
			// We assume that if things are cached then all of the caches are available.
			// There's probably a more accurate but slightly slower way to do this where we check if each partition has its cached data and if not return a wrapped version.
			// For now, we assume that ETL is doing its job. 
			// If this is not the case, we should switch to the more accurate algorithm.
			if(userWantsRawData) {
				logger.debug("User wants raw data.");
				paths = PlainPBPathNameUtility.getPathsWithData(context.getPaths(), rootFolder, pvName, startTime, endTime, PB_EXTENSION, partitionGranularity, this.compressionMode, this.pv2key);
			} else {
				paths = PlainPBPathNameUtility.getPathsWithData(context.getPaths(), rootFolder, pvName, startTime, endTime, extension, partitionGranularity, this.compressionMode, this.pv2key);
				if(paths == null || paths.length == 0) {
					logger.debug("Did not find any cached entries for " + pvName + " for post processor " + extension + ". Defaulting to using the raw streams and computing the data at runtime.");
					askingForProcessedDataButAbsentInCache = true;
					paths = PlainPBPathNameUtility.getPathsWithData(context.getPaths(), rootFolder, pvName, startTime, endTime, PB_EXTENSION, partitionGranularity, this.compressionMode, this.pv2key);
				} else {
					logger.debug("Found " + paths.length + " cached entries for " + pvName + " for post processor " + extension);
				}
			}
			logger.debug(desc + " Found " + (paths != null ? paths.length : 0) + " matching files for pv " + pvName + " in store " + this.getName());
			boolean useSearchForPositions = (this.compressionMode == CompressionMode.NONE);
			boolean doNotuseSearchForPositions = !useSearchForPositions;
			
			ArrayList<Callable<EventStream>> ret = new ArrayList<Callable<EventStream>>();
			// Regardless of what we find, we add the last event from the partition before the start time
			// This takes care of several multi-year bugs and hopefully does not introduce more.
			// The mergededup consumer will digest this using its buffers and serve data appropriately.
			Callable<EventStream> lastEventOfPreviousStream = getLastEventOfPreviousPartitionBeforeTimeAsStream(context, pvName, startTime, postProcessor, askingForProcessedDataButAbsentInCache);
			if(lastEventOfPreviousStream != null) ret.add(lastEventOfPreviousStream);

			if(paths != null && paths.length == 1) {
				PBFileInfo fileInfo = new PBFileInfo(paths[0]); 
				ArchDBRTypes dbrtype = fileInfo.getType();
				if(fileInfo.getLastEventEpochSeconds() <= TimeUtils.convertToEpochSeconds(startTime)) { 
					logger.debug("All we can get from this store is the last known event at " + TimeUtils.convertToHumanReadableString(fileInfo.getLastEventEpochSeconds()));
					ret.add(CallableEventStream.makeOneEventCallable(fileInfo.getLastEvent(), new RemotableEventStreamDesc(dbrtype, pvName, fileInfo.getDataYear()), postProcessor, askingForProcessedDataButAbsentInCache));
				} else { 
					ret.add(CallableEventStream.makeOneStreamCallable(new FileBackedPBEventStream(pvName, paths[0], dbrtype, startTime, endTime, doNotuseSearchForPositions), postProcessor, askingForProcessedDataButAbsentInCache));
				}
			} else if(paths != null && paths.length > 1) {
				PBFileInfo fileInfo = new PBFileInfo(paths[0]); 
				ArchDBRTypes dbrtype = fileInfo.getType();
				int pathsCount = paths.length;
				for(int pathid = 0; pathid < pathsCount; pathid++) {
					if(pathid == 0) {
						ret.add(CallableEventStream.makeOneStreamCallable(new FileBackedPBEventStream(pvName, paths[pathid], dbrtype, startTime, endTime, doNotuseSearchForPositions), postProcessor, askingForProcessedDataButAbsentInCache));						
					} else if(pathid == pathsCount -1 ) {
						ret.add(CallableEventStream.makeOneStreamCallable(new FileBackedPBEventStream(pvName, paths[pathid], dbrtype, startTime, endTime, doNotuseSearchForPositions), postProcessor, askingForProcessedDataButAbsentInCache));
					} else {
						ret.add(CallableEventStream.makeOneStreamCallable(new FileBackedPBEventStream(pvName, paths[pathid], dbrtype), postProcessor, askingForProcessedDataButAbsentInCache));
					}
				}
			} else {
				logger.debug("Ret should have only the last event of the previous partition for pv " + pvName);
			}
			return ret;
		} catch (NoDataException nex) {
			logger.warn(desc + ": did not find any data for " + pvName + " returning null", nex);
			return null;
		} catch (Exception ex) {
			throw new IOException("Exception retrieving data from " + desc + " for pv " + pvName, ex);
		} finally {
			
		}
	}

	private Callable<EventStream> getLastEventOfPreviousPartitionBeforeTimeAsStream(BasicContext context, String pvName, Timestamp startTime, PostProcessor postProcessor, boolean askingForProcessedDataButAbsentInCache) throws Exception, IOException {
		Path mostRecentPath = PlainPBPathNameUtility.getPreviousPartitionBeforeTime(context.getPaths(), rootFolder, pvName, startTime, PB_EXTENSION, partitionGranularity, this.compressionMode, this.pv2key);
		if(mostRecentPath != null) {
			// Should we use these two here?
			// boolean useSearchForPositions = (this.compressionMode == CompressionMode.NONE);
			// boolean doNotuseSearchForPositions = !useSearchForPositions;
			logger.debug("Last known event for PV comes from " + mostRecentPath.toString());
			PBFileInfo fileInfo = new PBFileInfo(mostRecentPath); 
			ArchDBRTypes dbrtype = fileInfo.getType();
			RemotableEventStreamDesc lastKnownEventDesc = new RemotableEventStreamDesc(dbrtype, pvName, fileInfo.getDataYear());
			lastKnownEventDesc.setSource("Last known event from " + this.getName() + " from " + mostRecentPath.getFileName());
			return CallableEventStream.makeOneEventCallable(fileInfo.getLastEvent(), lastKnownEventDesc, postProcessor, askingForProcessedDataButAbsentInCache);
		}
		
		logger.debug(desc + ": did not even find the most recent file with data for " + pvName + " returning null.");
		return null;
	}
	

	private AppendDataStateData getAppendDataState(BasicContext context, String pvName) throws IOException {
		if(appendDataStates.containsKey(pvName)) {
			return appendDataStates.get(pvName);
		} else {
			logger.debug("Creating new append data state for pv " + pvName);
			AppendDataStateData state = new AppendDataStateData(this.partitionGranularity, this.rootFolder, this.desc, getLastKnownTimestampForAppend(context, pvName), this.compressionMode, this.pv2key);
			appendDataStates.put(pvName, state);
			return state;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see org.epics.archiverbenchmarks.Writer#appendData(java.lang.String, org.epics.archiverbenchmarks.EventStream)
	 * Append the data to the end of the file.
	 * For now we are assuming that the caller is doing the partitioning by year.
	 */
	@Override
	public boolean appendData(BasicContext context, String pvName, EventStream stream) throws IOException {
		AppendDataStateData state = getAppendDataState(context, pvName);
		state.partitionBoundaryAwareAppendData(context, pvName, stream, PB_EXTENSION, null);
		return true;
	}

	/* (non-Javadoc)
	 * Append the data to the end of the ETL append data file.
	 */
	@Override
	public boolean appendToETLAppendData(String pvName, EventStream stream, ETLContext context) throws IOException {
		
		if(this.etlIntoStoreIf != null) { 
			boolean namedFlagValue = this.configService.getNamedFlag(this.etlIntoStoreIf);
			if(!namedFlagValue) { 
				logger.info("Skipping ETL append data for " + pvName + " as named flag " + this.etlIntoStoreIf + " is false.");
				return true;
			}
		}
		
		AppendDataStateData state = getAppendDataState(context, pvName);
		
		if(this.reducedataPostProcessor != null) {
			try { 
				PostProcessor postProcessor = PostProcessors.findPostProcessor(this.reducedataPostProcessor);
				postProcessor.initialize(reducedataPostProcessor, pvName);
				stream = CallableEventStream.makeOneStreamCallable(stream, postProcessor, true).call();
				logger.debug("Wrapped stream with post processor " + this.reducedataPostProcessor + " for pv " + pvName);
				if(postProcessor instanceof PostProcessorWithConsolidatedEventStream) {
					stream = ((PostProcessorWithConsolidatedEventStream) postProcessor).getConsolidatedEventStream();
					logger.debug("Using consolidated event stream for pv " + pvName);
				}
			} catch (Exception ex) { 
				logger.error("Exception moving reduced data for pv " + pvName + " to store " + this.getName() + " using operator " + this.reducedataPostProcessor, ex);
				return false;
			}
		}
		
		boolean bulkInserted = false;
		if(stream instanceof ETLBulkStream) {
			ETLBulkStream bulkStream = (ETLBulkStream) stream;
			if(backupFilesBeforeETL) {
				bulkInserted = state.bulkAppend(pvName, context, bulkStream, APPEND_EXTENSION, PB_EXTENSION);
			} else { 
				bulkInserted = state.bulkAppend(pvName, context, bulkStream, PB_EXTENSION, null);				
			}
		}
		
		if(!bulkInserted) {
			if(backupFilesBeforeETL) {
				state.partitionBoundaryAwareAppendData(context, pvName, stream, APPEND_EXTENSION, PB_EXTENSION);			
			} else {
				state.partitionBoundaryAwareAppendData(context, pvName, stream, PB_EXTENSION, null);			
			}
		}
		return true;
	}

	@Override
	public String getDescription() {
		return desc;
	}
	

	@Override
	public void initialize(String configURL, ConfigService configService) throws IOException {
		this.configService = configService;
		this.pv2key = this.configService.getPVNameToKeyConverter();
		assert(pv2key != null);
		
		try {
			URI srcURI = new URI(configURL);
			HashMap<String, String> queryNVPairs = URIUtils.parseQueryString(srcURI);

			if(queryNVPairs.containsKey("name")) {
				name = queryNVPairs.get("name");
			} else {
				throw new IOException("Cannot initialize the plugin; this plugin implements the storage metrics API which needs an identity");
			}

			String rootFolderStr = null;
			if(queryNVPairs.containsKey("rootFolder")) {
				rootFolderStr = queryNVPairs.get("rootFolder");
			} else {
				throw new IOException("Cannot initialize the plugin; this needs both the rootFolder and the partitionGranularity to be specified");
			}

			if(queryNVPairs.containsKey("partitionGranularity")) {
				this.setPartitionGranularity(PartitionGranularity.valueOf(queryNVPairs.get("partitionGranularity")));
			} else {
				throw new IOException("Cannot initialize the plugin; this needs both the rootFolder and the partitionGranularity to be specified");
			}
			
			if(queryNVPairs.containsKey("hold")) {
				this.setHoldETLForPartions(Integer.parseInt(queryNVPairs.get("hold")));
			}
			if(queryNVPairs.containsKey("gather")) {
				this.setGatherETLinPartitions(Integer.parseInt(queryNVPairs.get("gather")));
			}
			
			if(queryNVPairs.containsKey("compress")) {
				compressionMode = CompressionMode.valueOf(queryNVPairs.get("compress"));
				if(compressionMode != CompressionMode.NONE) {
					if(!rootFolderStr.startsWith(ArchPaths.ZIP_PREFIX)) {
						String rootFolderWithPath = ArchPaths.ZIP_PREFIX + rootFolderStr;
						logger.debug("Automatically adding url scheme for compression to rootfolder " + rootFolderWithPath);
						rootFolderStr = rootFolderWithPath;
					}
				}
			}
			
			setRootFolder(rootFolderStr);
			
			this.postProcessorUserArgs = URIUtils.getMultiValuedParamFromQueryString(srcURI, "pp");
			
			if(queryNVPairs.containsKey("reducedata")) { 
				reducedataPostProcessor = queryNVPairs.get("reducedata");
			}
			
			if(queryNVPairs.containsKey("consolidateOnShutdown")) {
				this.consolidateOnShutdown = Boolean.parseBoolean(queryNVPairs.get("consolidateOnShutdown"));
			}

			if(queryNVPairs.containsKey("etlIntoStoreIf")) { 
				this.etlIntoStoreIf = queryNVPairs.get("etlIntoStoreIf");
			}

			if(queryNVPairs.containsKey("etlOutofStoreIf")) { 
				this.etlOutofStoreIf = queryNVPairs.get("etlOutofStoreIf");
			}

			this.setDesc("PlainPBStorage plugin  - " + name + " with rootFolder " + rootFolder + " and granularity " + partitionGranularity);
		} catch(URISyntaxException ex) {
			throw new IOException(ex);
		}
	}
	
	/**
	 * Return a URL representation of this plugin suitable for parsing by StoragePluginURLParser
	 * @return ret A URL representation
	 */
	public String getURLRepresentation() {
		try {
			StringBuilder buf = new StringBuilder();
			buf.append("pb://localhost?name=");
			buf.append(URLEncoder.encode(name, "UTF-8"));
			buf.append("&rootFolder=");
			buf.append(URLEncoder.encode(rootFolder, "UTF-8"));
			buf.append("&partitionGranularity=");
			buf.append(partitionGranularity.toString());
			if(this.holdETLForPartions != 0) {
				buf.append("&hold=");
				buf.append(Integer.toString(holdETLForPartions));
			}
			if(this.gatherETLinPartitions != 0) {
				buf.append("&gather=");
				buf.append(Integer.toString(gatherETLinPartitions));
			}
			
			if(this.consolidateOnShutdown) {
				buf.append("&consolidateOnShutdown=");
				buf.append(Boolean.toString(consolidateOnShutdown));
			}

			
			if(this.compressionMode != CompressionMode.NONE) {
				buf.append("&compress=");
				buf.append(compressionMode.toString());
			}
			
			if(this.postProcessorUserArgs != null && !this.postProcessorUserArgs.isEmpty()) {
				for(String postProcessorUserArg : postProcessorUserArgs) {
					buf.append("&pp=");
					buf.append(postProcessorUserArg);
				}
			}
			
			if(this.reducedataPostProcessor != null) { 
				buf.append("&reducedata=");
				buf.append(reducedataPostProcessor);
			}
			
			if(this.etlIntoStoreIf != null) { 
				buf.append("&etlIntoStoreIf=");
				buf.append(this.etlIntoStoreIf);
			}

			if(this.etlOutofStoreIf != null) { 
				buf.append("&etlOutofStoreIf=");
				buf.append(this.etlOutofStoreIf);
			}
			
			String ret =  buf.toString();
			logger.debug("URL representation " + ret);
			return ret;
		} catch(Exception ex) {
			logger.error("Exception generating URL representation of plugin", ex);
			return null;
		}
	}
	
	private static void loadPBclasses() {
		try {
			EPICSEvent.ScalarDouble.newBuilder()
			.setSecondsintoyear(0)
			.setNano(0)
			.setVal(0)
			.setSeverity(0)
			.setStatus(0)
			.build().toByteArray();
		} catch(Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
	}

	public void setRootFolder(String rootFolder) throws IOException {
		this.rootFolder = rootFolder;
		logger.debug("Setting root folder to " + rootFolder);
		try(ArchPaths paths = new ArchPaths()) {
			if(this.compressionMode == CompressionMode.NONE) {
				Path path = paths.get(this.rootFolder);
				if(!Files.exists(path)) {
					logger.warn(desc + ": The root folder specified does not exist - " + rootFolder + ". Creating it");
					Files.createDirectories(path);
					return;
				}

				if(!Files.isDirectory(path)) {
					logger.error(desc + ": The root folder specified is not a directory - " + rootFolder);
					return;
				}
			} else {
				Path path = paths.get(this.rootFolder.replace(ArchPaths.ZIP_PREFIX, "/"));
				if(!Files.exists(path)) {
					logger.warn(desc + ": The root folder specified does not exist - " + rootFolder + " Creating it");
					Files.createDirectories(path);
					return;
				}

				if(!Files.isDirectory(path)) {
					logger.error(desc + ": The root folder specified is not a directory - " + rootFolder);
					return;
				}
				
			}
		}
		
		loadPBclasses();
		return;
	}
	
	public void setDesc(String newDesc) {
		this.desc = newDesc;
	}

	public String getRootFolder() {
		return rootFolder;
	}

	public String getDesc() {
		return desc;
	}

	@Override
	public PartitionGranularity getPartitionGranularity() {
		return partitionGranularity;
	}
	
	public void setPartitionGranularity(PartitionGranularity partitionGranularity) {
		this.partitionGranularity = partitionGranularity;
	}


	@Override
	public List<ETLInfo> getETLStreams(String pvName, Timestamp currentTime, ETLContext context) throws IOException {

		if(etlOutofStoreIf != null) { 
			boolean namedFlagValue = this.configService.getNamedFlag(etlOutofStoreIf);
			if(!namedFlagValue) { 
				logger.info("Skipping getting ETL Streams for " + pvName + " as named flag " + this. etlOutofStoreIf + " is false.");
				return new LinkedList<ETLInfo>();
			}
		}

		Path[] paths = PlainPBPathNameUtility.getPathsBeforeCurrentPartition(context.getPaths(), rootFolder, pvName, currentTime, PB_EXTENSION, partitionGranularity, this.compressionMode, this.pv2key);
		if(paths == null || paths.length == 0) { 
			if(logger.isInfoEnabled()) { 
				logger.debug("No files for ETL for pv " + pvName + " for time " + TimeUtils.convertToISO8601String(currentTime));
			}
			return null;
		}
		
		if((holdETLForPartions - gatherETLinPartitions) < 0) {
			logger.error("holdETLForPartions - gatherETLinPartitions is invalid for hold=" + holdETLForPartions + " and gather=" + gatherETLinPartitions);
		}
		
		
		long holdInEpochSeconds = TimeUtils.getPreviousPartitionLastSecond(TimeUtils.convertToEpochSeconds(currentTime) - partitionGranularity.getApproxSecondsPerChunk()*holdETLForPartions, partitionGranularity);
		long gatherInEpochSeconds = TimeUtils.getPreviousPartitionLastSecond(TimeUtils.convertToEpochSeconds(currentTime) - partitionGranularity.getApproxSecondsPerChunk()*(holdETLForPartions - (gatherETLinPartitions - 1)), partitionGranularity);
		boolean skipHoldAndGather = (holdETLForPartions == 0) && (gatherETLinPartitions == 0);
		
		ArrayList<ETLInfo> etlreadystreams = new ArrayList<ETLInfo>();
		boolean holdOk = false;
		for(Path path : paths) {
			try {
				if(!Files.exists(path)) { 
					logger.warn("Path " + path + " does not seem to exist for ETL at time " + TimeUtils.convertToISO8601String(currentTime));
					continue;
				}
				
				if(Files.size(path) <= 0) { 
					logger.warn("Path " + path + " is of size zero bytes at time " + TimeUtils.convertToISO8601String(currentTime));
					
					long lastModifiedInMillis = Files.getLastModifiedTime(path).toMillis();
					long currentTimeInMillis = currentTime.getTime();
					if((currentTimeInMillis - lastModifiedInMillis) > ((this.holdETLForPartions+1) * this.getPartitionGranularity().getApproxSecondsPerChunk()*60)) { 
						logger.warn("Zero byte file is older than current ETL time by holdETLForPartions; deleting it " + path.toAbsolutePath().toString());
						try { 
							Files.delete(path);
						} catch(Exception ex) { 
							logger.error("Exception deleting file " + path.toAbsolutePath().toString(), ex);
						}
					}
					
					continue;
				}
				
				PBFileInfo fileinfo = new PBFileInfo(path);
				ETLInfo etlInfo = new ETLInfo(pvName, fileinfo.getType(), path.toAbsolutePath().toString(), partitionGranularity, new FileStreamCreator(pvName, path, fileinfo), fileinfo.getFirstEvent(), Files.size(path));
				if(skipHoldAndGather) {
					logger.debug("Skipping computation of hold and gather");
					etlreadystreams.add(etlInfo);					
				} else {
					if(fileinfo.getFirstEvent() == null) {
						logger.debug("We seem to have an empty file " + path.toAbsolutePath().toString());

						long lastModifiedInMillis = Files.getLastModifiedTime(path).toMillis();
						long currentTimeInMillis = currentTime.getTime();
						if((currentTimeInMillis - lastModifiedInMillis) > ((this.holdETLForPartions+1) * this.getPartitionGranularity().getApproxSecondsPerChunk()*60)) { 
							logger.warn("Empty file is older than current ETL time by holdETLForPartions; deleting it " + path.toAbsolutePath().toString());
							try { 
								Files.delete(path);
							} catch(Exception ex) { 
								logger.error("Exception deleting file " + path.toAbsolutePath().toString(), ex);
							}
						}
						
						continue; 
					}
					if(!holdOk) {
						if(fileinfo.getFirstEventEpochSeconds() <= holdInEpochSeconds) {
							holdOk = true;
						} else {
							logger.debug("Hold not satisfied for first event " + TimeUtils.convertToISO8601String(fileinfo.getFirstEventEpochSeconds()) + " and hold = " + TimeUtils.convertToISO8601String(holdInEpochSeconds));
							return etlreadystreams;
						}
					}
					
					if(fileinfo.getFirstEventEpochSeconds() <= gatherInEpochSeconds) {
						etlreadystreams.add(etlInfo);
					} else {
						logger.debug("Gather not satisfied for first event " + TimeUtils.convertToISO8601String(fileinfo.getFirstEventEpochSeconds()) + " and gather = " + TimeUtils.convertToISO8601String(gatherInEpochSeconds));
					}
					
				}
			} catch(IOException ex) {
				logger.error("Skipping ading " + path.toAbsolutePath().toString() + " to ETL list due to exception. Should we go ahead and mark this file for deletion in this case? ", ex);
			}
		}
		
		return etlreadystreams;
	}

	@Override
	public void markForDeletion(ETLInfo info, ETLContext context) {
		try {
			Path path = context.getPaths().get(info.getKey());
			long size = Files.size(path);
			long sizeFromInfo = info.getSize();
			if(sizeFromInfo == -1) { 
				logger.error("We are missing size information from ETLInfo for " + info.getKey());
				Files.delete(path);
			} else { 
				if(sizeFromInfo == size) { 
					Files.delete(path);
				} else { 
					logger.error("The path " + info.getKey() + " has changed since we generate the ETLInfo. Not deleting it this time around. If this persists, please manually remove the file. Current Size " + size + ". Size from info " + sizeFromInfo);
				}
			}
		} catch(Exception ex) {
			logger.error("Exception deleting " + info.getKey() + ". Please manually remove this file", ex);
		}
	}

	@Override
	public Event getLastKnownEvent(BasicContext context, String pvName) throws IOException {
		try {
			Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, pvName, PB_EXTENSION, partitionGranularity, this.compressionMode, this.pv2key);
			logger.debug(desc + " Found " + (paths != null ? paths.length : 0) + " matching files for pv " + pvName);
			if(paths != null && paths.length > 0) {
				for(int i = paths.length-1; i >=0; i--) {
					if(logger.isDebugEnabled()) logger.debug("Looking for last known event in file " + paths[i].toAbsolutePath().toString());
					try {
						if(Files.size(paths[i]) <= 0) { 
							logger.debug("Ignoring zero byte file " + paths[i].toAbsolutePath().toString());
							continue;
						}
						PBFileInfo fileInfo = new PBFileInfo(paths[i]);
						if(fileInfo.getLastEvent() != null) return fileInfo.getLastEvent();
					} catch(Exception ex) { 
						logger.warn("Exception determing header information from file " + paths[i].toAbsolutePath().toString(), ex);
					}
				}
			}
		} catch(NoSuchFileException ex) {
			// We expect a NoSuchFileException if the file does not exist.
			return null;
		} catch (Exception ex) {
			logger.error("Exception determining last known event for " + pvName, ex);
		}
		
		return null;
	}
	
	@Override
	public Event getFirstKnownEvent(BasicContext context, String pvName) throws IOException {
		try {
			Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, pvName, PB_EXTENSION, partitionGranularity, this.compressionMode, this.pv2key);
			logger.debug(desc + " Found " + (paths != null ? paths.length : 0) + " matching files for pv " + pvName);
			if(paths != null && paths.length > 0) {
				for(int i = 0; i < paths.length; i++) {
					if(logger.isDebugEnabled()) logger.debug("Looking for first known event in file " + paths[i].toAbsolutePath().toString());
					try { 
						PBFileInfo fileInfo = new PBFileInfo(paths[i]);
						if(fileInfo.getFirstEvent() != null) return fileInfo.getFirstEvent();
					} catch(Exception ex) { 
						logger.warn("Exception determing header information from file " + paths[i].toAbsolutePath().toString(), ex);
					}
				}
			}
		} catch(NoSuchFileException ex) {
			// We expect a NoSuchFileException if the file does not exist.
			return null;
		} catch (Exception ex) {
			logger.error("Exception determining first known event for " + pvName, ex);
		}
		
		return null;
	}

	
	/**
	 * Get last known timestamp for append purposes. If last event is not known, we return time(0)
	 * @param pvName The name of PV.
	 * @return Timestamp Last known Timestamp
	 * @throws IOException
	 */
	private Timestamp getLastKnownTimestampForAppend(BasicContext context, String pvName) throws IOException {
		Event event = getLastKnownEvent(context, pvName);
		if(event != null) {
			return event.getEventTimeStamp();
		}
		return new Timestamp(0);
	}

	@Override
	public boolean prepareForNewPartition(String pvName, Event ev, ArchDBRTypes archDBRType, ETLContext context) throws IOException {
		// The functionality in AppendDataState should take care of automatically preparing partitions and the like.
		return true;
	}
	
	@Override
	public boolean commitETLAppendData(String pvName, ETLContext context) throws IOException {
		if(compressionMode == CompressionMode.NONE) {
			if(backupFilesBeforeETL) {
				// Get all append data files for the specified PV name and partition granularity.
				Path[] appendDataPaths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, pvName, APPEND_EXTENSION, partitionGranularity, this.compressionMode, this.pv2key);
				if (appendDataPaths == null) {
					logger.debug("No " + APPEND_EXTENSION + " files found for PV " + pvName);
					return true;
				}
				
				if(logger.isDebugEnabled()) logger.debug(desc + " Found " + (appendDataPaths != null ? appendDataPaths.length : 0) + " matching files for pv " + pvName);

				for(Path srcPath : appendDataPaths) {
					Path destPath = context.getPaths().get(srcPath.toUri().toString().replace(APPEND_EXTENSION, PB_EXTENSION));
					Files.move(srcPath, destPath, REPLACE_EXISTING, ATOMIC_MOVE);
				}
			}
		}
		
		return true;
	}
	
	
	@Override
	public boolean runPostProcessors(String pvName, ArchDBRTypes dbrtype, ETLContext context) throws IOException {
		if(postProcessorUserArgs != null && !postProcessorUserArgs.isEmpty()) {
			for(String postProcessorUserArg : postProcessorUserArgs) {
				PostProcessor postProcessor = PostProcessors.findPostProcessor(postProcessorUserArg);
				if(postProcessor == null) {
					logger.error("Cannot find post processor for " + postProcessorUserArg);
					continue;
				}
				postProcessor.initialize(postProcessorUserArg, pvName);
				String ppExt = "." + postProcessor.getExtension();
				List<PPMissingPaths> missingOrOlderPPPaths = getListOfPathsWithMissingOrOlderPostProcessorData(context, pvName, postProcessor);
				if(missingOrOlderPPPaths != null && !missingOrOlderPPPaths.isEmpty()) {
					for(PPMissingPaths missingOrOlderPath : missingOrOlderPPPaths) {
						if(logger.isDebugEnabled()) logger.debug("Generating pp data for " + missingOrOlderPath.ppsPath.toString() + " from " + missingOrOlderPath.srcPath.toString() + " and pp with extension" + ppExt + ". Size of src before " + Files.size(missingOrOlderPath.srcPath));
						Callable<EventStream> callable = CallableEventStream.makeOneStreamCallable(new FileBackedPBEventStream(pvName, missingOrOlderPath.srcPath, dbrtype), postProcessor, true);
						try(EventStream stream = callable.call()) {
							// The post processor data can be generated at any time in any sequence; so we suspend the initial monotonicity checks for the post processor where we compare with the last known event.
							// Ideally this should be the first event of the source stream minus some buffer.
							Timestamp timezero = TimeUtils.convertFromEpochSeconds(0, 0);
							AppendDataStateData state = new AppendDataStateData(this.partitionGranularity, this.rootFolder, this.desc, timezero, this.compressionMode, this.pv2key);
							int eventsAppended = state.partitionBoundaryAwareAppendData(context, pvName, stream, ppExt, null);
							if(logger.isDebugEnabled()) logger.debug("Done generating pp data for " + missingOrOlderPath.ppsPath.toString() + " from " + missingOrOlderPath.srcPath.toString() + " appending " + eventsAppended + " events. Size of src after " + Files.size(missingOrOlderPath.srcPath));
						} catch(Exception ex) {
							logger.error("Exception appending pp data for pv " + pvName + " for source " + missingOrOlderPath.srcPath.toString() + " for " + postProcessorUserArg);
						}
					}
				} else {
					logger.debug("All paths are current for pv " + pvName + " for pp " + postProcessorUserArg);
				}
			}
		}
		
		return true;
	}


	public boolean isBackupFilesBeforeETL() {
		return backupFilesBeforeETL;
	}

	public void setBackupFilesBeforeETL(boolean backupFilesBeforeETL) {
		this.backupFilesBeforeETL = backupFilesBeforeETL;
	}

	/**
	 * The hold and gather are used to implement a high/low watermark for ETL.
	 * ETL is skipped until the first known event in the partitions available for ETL is earlier than <i>hold</i> partitions.
	 * Once this is true, we then include in the ETL list all partitions whose first event is earlier than <i>hold - gather</i> partitions.
	 * For example, in a PARTITION_DAY, if you want to run ETL once every 7 days, but when you run you want to move 5 days worth of data to the dest, set hold to 7 and gather to 5.
	 * Hold and gather default to a scenario where we aggressively push data to the destination as soon as it is available.    
	 * @return holdETLForPartions &emsp;
	 */
	public int getHoldETLForPartions() {
		return holdETLForPartions;
	}

	public void setHoldETLForPartions(int holdETLForPartions) throws IOException {
		if((holdETLForPartions - gatherETLinPartitions) < 0) throw new IOException("holdETLForPartions - gatherETLinPartitions is invalid for hold=" + holdETLForPartions + " and gather=" + gatherETLinPartitions);
		this.holdETLForPartions = holdETLForPartions;
	}

	public int getGatherETLinPartitions() {
		return gatherETLinPartitions;
	}

	public void setGatherETLinPartitions(int gatherETLinPartitions) throws IOException {
		if((holdETLForPartions - gatherETLinPartitions) < 0) throw new IOException("holdETLForPartions - gatherETLinPartitions is invalid for hold=" + holdETLForPartions + " and gather=" + gatherETLinPartitions);
		this.gatherETLinPartitions = gatherETLinPartitions;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public long getTotalSpace(StorageMetricsContext storageMetricsContext) throws IOException {
		return storageMetricsContext.getFileStore(this.getRootFolder()).getTotalSpace();
	}

	@Override
	public long getUsableSpace(StorageMetricsContext storageMetricsContext) throws IOException  {
		return storageMetricsContext.getFileStore(this.getRootFolder()).getUsableSpace();
	}


	@Override
	public long spaceConsumedByPV(String pvName) throws IOException {
		// Using a blank extension should fetch everything?
		Path[] rawPaths = PlainPBPathNameUtility.getAllPathsForPV(new ArchPaths(), rootFolder, pvName, "", partitionGranularity, this.compressionMode, this.pv2key);
		long spaceConsumed = 0;
		if(rawPaths != null) {
			for(Path f : rawPaths) {
				spaceConsumed = spaceConsumed + f.toFile().length();
			}
		}
		
		return spaceConsumed;
	}

	public void setName(String name) {
		this.name = name;
	}

	public CompressionMode getCompressionMode() {
		return compressionMode;
	}
	
	private class PPMissingPaths {
		Path srcPath;
		Path ppsPath;

		PPMissingPaths(Path srcPath, Path ppsPath) {
			this.srcPath = srcPath;
			this.ppsPath = ppsPath;
		}
	}
	
	private List<PPMissingPaths> getListOfPathsWithMissingOrOlderPostProcessorData(BasicContext context, String pvName, PostProcessor postProcessor) throws IOException {
		String ppExt = "." + postProcessor.getExtension();
		logger.debug("Looking for missing " + ppExt + " paths based on the list of " + PB_EXTENSION + " paths");
		Path[] rawPaths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), this.rootFolder, pvName, PB_EXTENSION, this.partitionGranularity, this.compressionMode, this.pv2key);
		Path[] ppPaths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), this.rootFolder, pvName, ppExt, this.partitionGranularity, this.compressionMode, this.pv2key);
		
		HashMap<String, Path> ppPathsMap = new HashMap<String, Path>();
		for(Path ppPath : ppPaths) {
			ppPathsMap.put(ppPath.toUri().toString(), ppPath);
		}
		
		LinkedList<PPMissingPaths> ret = new LinkedList<PPMissingPaths>();
		for(Path rawPath : rawPaths) {
			String expectedPPPath = rawPath.toUri().toString().replace(PB_EXTENSION, ppExt);
			if(!ppPathsMap.containsKey(expectedPPPath)) {
				if(logger.isDebugEnabled()) logger.debug("Missing pp path " + expectedPPPath);
				ret.add(new PPMissingPaths(rawPath, context.getPaths().get(expectedPPPath)));
			} else {
				if(logger.isDebugEnabled()) logger.debug("pp path " + expectedPPPath + " already present");
				Path actualPPPath = ppPathsMap.get(expectedPPPath);
				FileTime rawPathTime = Files.getLastModifiedTime(rawPath);
				FileTime ppPathTime = Files.getLastModifiedTime(actualPPPath);
				if(logger.isDebugEnabled()) logger.debug("Modification time of src " + rawPathTime.toString() + " and of pp file " + ppPathTime.toString());
				if(rawPathTime.compareTo(ppPathTime) > 0) {
					logger.debug("Raw file is newer than PP file for " + expectedPPPath);
					ret.add(new PPMissingPaths(rawPath, context.getPaths().get(expectedPPPath)));
				}
			}
		}
		
		return ret;
	}

	@Override
	public boolean consolidateOnShutdown() {
		return consolidateOnShutdown;
	}
	
	private List<String> getPPExtensions() { 
		LinkedList<String> ret = new LinkedList<String>();
		for(String postProcessorUserArg : postProcessorUserArgs) {
			PostProcessor postProcessor = PostProcessors.findPostProcessor(postProcessorUserArg);
			String ppExt = "." + postProcessor.getExtension();
			ret.add(ppExt);
		} 
		return ret;
	}
	
	
	/* (non-Javadoc)
	 * @see org.epics.archiverappliance.StoragePlugin#renamePV(java.lang.String, java.lang.String, org.epics.archiverappliance.config.PVTypeInfo, org.epics.archiverappliance.config.PVTypeInfo)
	 * 
	 * We need to do these things here
	 * <ol>
	 * <li>Move the files to the new location as determined from the new name.</li>
	 * <li>Change the name in the header/MessageInfo. Since the header is the first line in the PB file, we have to copy over to the new location.</li>
	 * Note this applies to the PB data and also any preprocessor data that is stored.
	 * </ol>
	 */
	@Override
	public void renamePV(BasicContext context, String oldName, String newName) throws IOException {
		// Copy data for the main pb file.
		{ 
			Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, oldName, PB_EXTENSION, partitionGranularity, this.compressionMode, this.pv2key);
			if(paths != null && paths.length > 0) {
				for(Path path : paths) { 
					logger.debug("Copying over data from " + path.toString() + " to new pv " + newName);
					PBFileInfo info = new PBFileInfo(path);
					this.appendData(context, newName, new FileBackedPBEventStream(oldName, path, info.getType()));
				}
			}
		}
		
		// Copy data for the post processors...
		for(String ppExt : getPPExtensions()) { 
			Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, oldName, ppExt, partitionGranularity, this.compressionMode, this.pv2key);
			if(paths != null && paths.length > 0) {
				for(Path path : paths) { 
					logger.debug("Copying over data from " + path.toString() + " to new pv " + newName + " for extension " + ppExt);
					PBFileInfo info = new PBFileInfo(path);
					AppendDataStateData state = getAppendDataState(context, newName);
					state.partitionBoundaryAwareAppendData(context, newName, new FileBackedPBEventStream(oldName, path, info.getType()), ppExt, null);
				}
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see org.epics.archiverappliance.StoragePlugin#convert(org.epics.archiverappliance.common.BasicContext, org.epics.archiverappliance.etl.ConversionFunction)
	 * 
	 * We find all the paths for this PV and then apply the conversion function for the
	 * 
	 */
	@Override
	public void convert(BasicContext context, String pvName, ConversionFunction conversionFuntion) throws IOException {
		// Convert data for the main pb file.
		Random r = new Random();
		int randomInt = r.nextInt();
		String randSuffix = "_tmp_" + randomInt;
		try {
			{ 
				Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, pvName, PB_EXTENSION, partitionGranularity, this.compressionMode, this.pv2key);
				if(paths != null && paths.length > 0) {
					for(Path path : paths) { 
						logger.info("Converting data in " + path.toString() + " for pv " + pvName);
						StartEndTimeFromName setimes = PlainPBPathNameUtility.determineTimesFromFileName(pvName, path.getFileName().toString(), partitionGranularity, this.pv2key);
						PBFileInfo info = new PBFileInfo(path);
						if(conversionFuntion.shouldConvert(new FileBackedPBEventStream(pvName, path, info.getType()), 
								TimeUtils.convertFromEpochSeconds(setimes.chunkStartEpochSeconds, 0), 
								TimeUtils.convertFromEpochSeconds(setimes.chunkEndEpochSeconds, 0))) {
							try(EventStream convertedStream = new TimeSpanLimitEventStream(
									conversionFuntion.convertStream(
											new FileBackedPBEventStream(pvName, path, info.getType()), 
											TimeUtils.convertFromEpochSeconds(setimes.chunkStartEpochSeconds, 0), 
											TimeUtils.convertFromEpochSeconds(setimes.chunkEndEpochSeconds, 0)), 
									setimes.chunkStartEpochSeconds, setimes.chunkEndEpochSeconds)) {
								AppendDataStateData state = new AppendDataStateData(this.partitionGranularity, this.rootFolder, this.desc, new Timestamp(0), this.compressionMode, this.pv2key);
								state.partitionBoundaryAwareAppendData(context, pvName, convertedStream, PB_EXTENSION + randSuffix, null);
							}
						}
					}
				}
			}
			
			// Convert data for the post processors...
			for(String ppExt : getPPExtensions()) { 
				Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, pvName, ppExt, partitionGranularity, this.compressionMode, this.pv2key);
				if(paths != null && paths.length > 0) {
					for(Path path : paths) { 
						logger.info("Converting data in " + path.toString() + " for pv " + pvName + " for extension " + ppExt);
						StartEndTimeFromName setimes = PlainPBPathNameUtility.determineTimesFromFileName(pvName, path.getFileName().toString(), partitionGranularity, this.pv2key);
						PBFileInfo info = new PBFileInfo(path);
						if(conversionFuntion.shouldConvert(new FileBackedPBEventStream(pvName, path, info.getType()),
											TimeUtils.convertFromEpochSeconds(setimes.chunkStartEpochSeconds, 0), 
											TimeUtils.convertFromEpochSeconds(setimes.chunkEndEpochSeconds, 0))) {
							try(EventStream convertedStream = new TimeSpanLimitEventStream(
									conversionFuntion.convertStream(
											new FileBackedPBEventStream(pvName, path, info.getType()),
											TimeUtils.convertFromEpochSeconds(setimes.chunkStartEpochSeconds, 0), 
											TimeUtils.convertFromEpochSeconds(setimes.chunkEndEpochSeconds, 0)), 
									setimes.chunkStartEpochSeconds, setimes.chunkEndEpochSeconds)) {
								AppendDataStateData state = new AppendDataStateData(this.partitionGranularity, this.rootFolder, this.desc, new Timestamp(0), this.compressionMode, this.pv2key);
								state.partitionBoundaryAwareAppendData(context, pvName, convertedStream, ppExt + randSuffix, null);
							}
						}
					}
				}
			}
			
			// Switch the files for the main pb file
			{ 
				Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, pvName, PB_EXTENSION + randSuffix, partitionGranularity, this.compressionMode, this.pv2key);
				if(paths != null && paths.length > 0) {
					for(Path path : paths) { 
						Path destPath = context.getPaths().get(path.toString().replace(randSuffix, ""));
						logger.info("Moving path " + path + " to " + destPath);
						Files.move(path, destPath, StandardCopyOption.ATOMIC_MOVE);
					}
				}
			}
			
			// Switch the files for the post processors...
			{ 
				for(String ppExt : getPPExtensions()) { 
					Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, pvName, ppExt + randSuffix, partitionGranularity, this.compressionMode, this.pv2key);
					if(paths != null && paths.length > 0) {
						for(Path path : paths) { 
							Path destPath = context.getPaths().get(path.toString().replace(randSuffix, ""));
							logger.info("Moving path " + path + " to " + destPath);
							Files.move(path, destPath, StandardCopyOption.ATOMIC_MOVE);
						}
					}
				}
			}
		} finally {
			// Clean up any tmp files
			Path[] paths = PlainPBPathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, pvName, randSuffix, partitionGranularity, this.compressionMode, this.pv2key);
			if(paths != null && paths.length > 0) {
				for(Path path : paths) { 
					logger.error("Deleting leftover file " + path);
					Files.delete(path);
				}
			}
			
		}		
	}
}
