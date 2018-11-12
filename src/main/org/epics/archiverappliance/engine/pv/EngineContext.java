

/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/

package org.epics.archiverappliance.engine.pv;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.epics.archiverappliance.config.pubsub.PubSubEvent;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.metadata.MetaCompletedListener;
import org.epics.archiverappliance.engine.metadata.MetaGet;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.writer.WriterRunnable;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.pvaccess.client.ChannelProvider;
import org.epics.pvaccess.client.ChannelProviderRegistryFactory;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.cosylab.epics.caj.CAJChannel;
import com.cosylab.epics.caj.CAJContext;
import com.google.common.eventbus.Subscribe;

import gov.aps.jca.Channel;
import gov.aps.jca.Context;
/***
 * the context for the Archiver Engine
 * @author Luofeng Li
 *
 */
public class EngineContext {
	private static final Logger logger = Logger.getLogger(EngineContext.class.getName());
	private static final Logger configlogger = Logger.getLogger("config." + EngineContext.class.getName());

	private static final double MAXIMUM_DISCONNECTED_CHANNEL_PERCENTAGE_BEFORE_STARTING_METACHANNELS = 5.0;
	private static final int METACHANNELS_TO_START_AT_A_TIME = 10000;

	/** writing thread to write samplebuffer to protocol buffer */
	final private WriterRunnable writer;
        /**is the write thread started or not*/
	private boolean isWriteThreadStarted = false;
	/**the thread pool to schedule all the runnable of the engine*/
	private ScheduledThreadPoolExecutor scheduler = null;
	/**the writing period*/
	private double write_period;
	/**the channel list of channels for  all pvs,but  without the channels created for the meta fields*/
	private final ConcurrentHashMap<String, ArchiveChannel> channelList;
	
        /**the command thread for all  pvs*/
	private JCACommandThread[] command_threads = null;
	private Context[] context2CommandThreadId = null;
	private ChannelProvider channelProvider;

	
	/**the total time consumed by the writer*/
	private double totalTimeConsumedByWritter;
	/**the total times of writer executed*/
	private long countOfWrittingByWritter = 0;
	/**the list of pvs controlling other pvs*/
	private ConcurrentHashMap<String, ControllingPV> controlingPVList = new ConcurrentHashMap<String, ControllingPV>();
	
	private ConfigService configService;
	private String myIdentity;
	
	/** A scheduler for all the SCAN PV's in the archiver. */
	private ScheduledThreadPoolExecutor scanScheduler;
	/** A scheduled thread pool executor misc tasks - these tasks can take an unspecified amount of time. */
	private ScheduledThreadPoolExecutor miscTasksScheduler;

	/**
	 * On disconnects, we add tasks that wait for this timeout to convert reconnects into ca searches into pause resumes.
	 * Ideally, Channel Access is supposed to take care of this but occasionally, we do see connections not reconnecting for a long time.
	 * This tries to address that problem. 
	 */
	private int disconnectCheckTimeoutInMinutes = 20;
	
	/**
	 * The disconnectChecker thread runs in this time frame.
	 * Note this controls both the connect/disconnect checks and the metafields connection initiations.
	 */
	private int disconnectCheckerPeriodInMinutes = 20;
	
	private ScheduledFuture<?> disconnectFuture = null;
	
	private double sampleBufferCapacityAdjustment = 1.0;
	

	/***
	 * 
	 * @return the list of pvs controlling other pvs
	 */
	public ConcurrentHashMap<String, ControllingPV> getControlingPVList() {
		return controlingPVList;
	}
        /**
	 * set the time consumed by writer to write the sample buffer once
	 * @param secondsConsumedByWritter  the time in second consumed by writer to write the sample buffer once
	 *  
	 */
	public void setSecondsConsumedByWritter(double secondsConsumedByWritter) {
		countOfWrittingByWritter++;
		totalTimeConsumedByWritter = totalTimeConsumedByWritter
				+ secondsConsumedByWritter;
	}
        /**
	 * 
	 * @return the average time in second consumed by writer
	 */
        public double getAverageSecondsConsumedByWritter() {
		if (countOfWrittingByWritter == 0)
			return 0;
		return totalTimeConsumedByWritter / (double) countOfWrittingByWritter;
	}

        /**
	 * This EngineContext should always be singleton
	 * @param configService the config service to initialize the engine context
	 */
	public EngineContext(final ConfigService configService) {
		String commandThreadCountVarName = "org.epics.archiverappliance.engine.epics.commandThreadCount";
		String commandThreadCountStr = configService.getInstallationProperties().getProperty(commandThreadCountVarName, "10");
		configlogger.info("Creating " + commandThreadCountStr + " command threads as specified by " + commandThreadCountVarName + " in archappl.properties");
		int commandThreadCount = Integer.parseInt(commandThreadCountStr);
		command_threads = new JCACommandThread[commandThreadCount];
		for(int threadNum = 0; threadNum < command_threads.length; threadNum++) { 
			command_threads[threadNum] = new JCACommandThread(configService);
			command_threads[threadNum].start();			
		}
		
		writer = new WriterRunnable(configService);
		channelList = new ConcurrentHashMap<String, ArchiveChannel>();
		logger.debug("Registering EngineContext for events");
		this.configService = configService;
		this.myIdentity = configService.getMyApplianceInfo().getIdentity();
		this.configService.getEventBus().register(this);
		
		String scanThreadCountName = "org.epics.archiverappliance.engine.epics.scanThreadCount";
		String scanThreadCountStr = configService.getInstallationProperties().getProperty(scanThreadCountName, "1");
		configlogger.info("Creating " + scanThreadCountStr + " scan threads as specified by " + scanThreadCountName + " in archappl.properties");
		int scanThreadCount = Integer.parseInt(scanThreadCountStr);

		
		// Start the scan thread
		scanScheduler = new ScheduledThreadPoolExecutor(scanThreadCount, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread ret = new Thread(r, "The SCAN scheduler.");
				return ret;
			}
		});


		configService.addShutdownHook(new Runnable() {

			@Override
			public void run() {

				logger.info("the archive engine will shutdown");
				try {

					if (scheduler != null) {
						scheduler.shutdown();
					}
					
					scanScheduler.shutdown();
					scanScheduler = null;
					
					Iterator<Entry<String, ArchiveChannel>> itChannel = channelList.entrySet().iterator();
					while (itChannel.hasNext()) {
						Entry<String, ArchiveChannel> channelentry = (Entry<String, ArchiveChannel>) itChannel.next();
						ArchiveChannel channeltemp = channelentry.getValue();
						channeltemp.shutdownMetaChannels();
						channeltemp.stop();
					}
					
					writer.flushBuffer();
					channelList.clear();
					
					// stop the controlling pv

					for (String pvName : controlingPVList.keySet()) {
						controlingPVList.get(pvName).stop();
					}

					controlingPVList.clear();
					
			        if (channelProvider != null) {
		                org.epics.pvaccess.ClientFactory.stop();
			        }


					scheduler = null;
					isWriteThreadStarted = false;
					for(int threadNum = 0; threadNum < command_threads.length; threadNum++) { 
						command_threads[threadNum].shutdown();
					}

				} catch (Exception e) {
					logger.error(
							"Exception when execuing ShutdownHook inconfigservice",
							e);
				}

				logger.info("the archive engine has been shutdown");

			}

		});
		
		if(configService.getInstallationProperties() != null) { 
			try {
				String disConnStr = configService.getInstallationProperties().getProperty("org.epics.archiverappliance.engine.util.EngineContext.disconnectCheckTimeoutInMinutes", "10");
				if(disConnStr != null) { 
					this.disconnectCheckTimeoutInMinutes = Integer.parseInt(disConnStr);
					logger.debug("Setting disconnectCheckTimeoutInMinutes to " + this.disconnectCheckTimeoutInMinutes);
				}
			} catch(Throwable t) { 
				logger.error("Exception initializing disconnectCheckTimeoutInMinutes", t);
			}
		}
		
		startMiscTasksScheduler(configService);
		
		boolean allContextsHaveBeenInitialized = false;
		for(int loopcount = 0; loopcount < 60 && !allContextsHaveBeenInitialized; loopcount++) {
			allContextsHaveBeenInitialized = true;
			for(int threadNum = 0; threadNum < command_threads.length; threadNum++) {
				Context context = this.command_threads[threadNum].getContext();
				if(context == null) {
					try {
						logger.debug("Waiting for all contexts to be initialized " + threadNum);
						allContextsHaveBeenInitialized = false;
						Thread.sleep(1000);
						break;
					} catch(Exception ex) { 
						// Ifnore
					}
				}
			}
		}

		context2CommandThreadId = new Context[command_threads.length];
		for(int threadNum = 0; threadNum < command_threads.length; threadNum++) {
			Context context = this.command_threads[threadNum].getContext();
			if(context == null) { 
				// We should have had enough time for all the contexts to have initialized...
				logger.error("JCA Context not initialized for thread" + threadNum + ". If you see this, we should a sleep() ahead of this message.");
			} else { 
				this.context2CommandThreadId[threadNum] = context;
			}
		}
		
		this.iniV4ChannelProvidert();
		
		this.sampleBufferCapacityAdjustment = Double.parseDouble(configService.getInstallationProperties().getProperty("org.epics.archiverappliance.config.PVTypeInfo.sampleBufferCapacityAdjustment", "1.0"));
		logger.debug("Buffer capacity adjustment is " + this.sampleBufferCapacityAdjustment);
	}

	/**
	 * Start up the scheduler for misc tasks. 
	 * @param configService
	 */
	private void startMiscTasksScheduler(final ConfigService configService) {
		miscTasksScheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread ret = new Thread(r, "Engine scheduler for misc tasks.");
				return ret;
			}
		});

		configService.addShutdownHook(new Runnable() {
			@Override
			public void run() {
				logger.info("Shutting down the engine scheduler for misc tasks.");
				miscTasksScheduler.shutdown();
			}
		});

		// Add an assertion in case we accidentally set this to 0 from the props file.
		assert(disconnectCheckerPeriodInMinutes > 0);
		disconnectFuture = miscTasksScheduler.scheduleAtFixedRate(new DisconnectChecker(configService), disconnectCheckerPeriodInMinutes, disconnectCheckerPeriodInMinutes, TimeUnit.MINUTES);
		
		// Add a task to update the metadata fields for each PV
		// We start this at a well known time; this code was previously suspected of a small memory leak.
		// Need to make sure this leak is no more.
		long currentEpochSeconds = TimeUtils.getCurrentEpochSeconds();
		// Start the metadata updates tomorrow afternoon; doesn't really matter what time; minimze impact with ETL etc
		long tomorrowAfternoon = ((currentEpochSeconds/(24*60*60)) + 1)*24*60*60 + 22*60*60;
		logger.info("Starting the metadata updater from " + TimeUtils.convertToHumanReadableString(tomorrowAfternoon));
		miscTasksScheduler.scheduleAtFixedRate(new MetadataUpdater(), tomorrowAfternoon-currentEpochSeconds, 24*60*60, TimeUnit.SECONDS);
	}
	
	public JCACommandThread getJCACommandThread(int jcaCommandThreadId) {
		return this.command_threads[jcaCommandThreadId];
	}


	/**
	 * Use this to assign JCA command threads to PV's
	 * @param pvName The name of PV
	 * @param iocHostName Note this can and will often be null.
	 * @return threadId  &emsp;
	 */
	public int assignJCACommandThread(String pvName, String iocHostName) { 
		String pvNameOnly = pvName.split("\\.")[0];
		ArchiveChannel channel = this.channelList.get(pvNameOnly);
		if(channel != null) {
			// Note this is expected for metachannels but not for main channels.
			if(pvName.equals(pvNameOnly)) { 
				logger.debug("We seem to have a channel already for " + pvName + ". Returning its JCA Command thread id.");
			}
			return channel.getJCACommandThreadID();			
		}
		int threadId =  Math.abs(pvNameOnly.hashCode()) % command_threads.length;
		return threadId;
	}
	
	
	public boolean doesContextMatchThread(Context context, int jcaCommandThreadId) { 
		Context contextForThreadId = this.context2CommandThreadId[jcaCommandThreadId];
		if(contextForThreadId != null) { 
			return contextForThreadId == context;
		} else { 
			logger.error("Null context for thread id " + jcaCommandThreadId);
			// We should never get here; but in the case we do failing in this assertion is less harmful than spewing the message with logs...
			return true;
		}
	}
	
/**
 * 
 * @return the channel list of pvs, without the pvs for meta fields
 */
	public ConcurrentHashMap<String, ArchiveChannel> getChannelList() {
		return channelList;
	}

/**
 * 
 * @return  the scheduler for the whole engine
 */
	public ScheduledThreadPoolExecutor getScheduler() {
		if (scheduler == null)
			scheduler = (ScheduledThreadPoolExecutor) Executors
					.newScheduledThreadPool(1, new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                                Thread ret = new Thread(r, "Scheduler for the whole engine");
                                return ret;
                        }
					}
							);
		return scheduler;

	}
	
	
	/**
	 * Get the scheduler used for SCAN PV's
	 * @return scanScheduler  &emsp;
	 */
	public ScheduledThreadPoolExecutor getScanScheduler() { 
		return scanScheduler;
	}
	
/**
 * 
 * @return the WriterRunnable for the engines
 */
	public WriterRunnable getWriteThead() {
		return writer;

	}

/**
 * start the write thread of the engine and this is actually called by the first pv when creating channel
 * @param configservice  configservice used by this writer
 */ 
	public void startWriteThread(ConfigService configservice) {
		int defaultWritePeriod = PVTypeInfo.getSecondsToBuffer(configservice);
		double actualWrite_period=writer.setWritingPeriod(defaultWritePeriod);
		this.write_period = actualWrite_period;
		if (scheduler == null) { 
			scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
		}
		scheduler.scheduleAtFixedRate(writer, 0, (long) (this.write_period * 1000), TimeUnit.MILLISECONDS);
		isWriteThreadStarted = true;
	}
/**
 * 
 * @return the writing period in second
 */
	public double getWritePeriod() {
		return write_period;
	}
/**
 * 
 * @return the status of the writing thread. return true, if it is started.Otherwise, return false;
 */
	public boolean isWriteThreadStarted() {

		return isWriteThreadStarted;
	}
	
	@Subscribe public void computeMetaInfo(PubSubEvent pubSubEvent) {
		if(pubSubEvent.getDestination().equals("ALL") 
				|| (pubSubEvent.getDestination().startsWith(myIdentity) && pubSubEvent.getDestination().endsWith(ConfigService.WAR_FILE.ENGINE.toString()))) {
			if(pubSubEvent.getType().equals("ComputeMetaInfo")) {
				String pvName = pubSubEvent.getPvName();
				try { 
					logger.debug("ComputeMetaInfo called for " + pvName);
					String fieldName = PVNames.getFieldName(pvName);
					String[] extraFields = configService.getExtraFields();
					if(fieldName != null && !fieldName.equals("")) {
						logger.debug("We are not asking for extra fields for a field value " + fieldName + " for pv " + pvName);
						extraFields = new String[0];
					}
					UserSpecifiedSamplingParams userSpec = new UserSpecifiedSamplingParams();
					JSONObject jsonObj = (JSONObject) JSONValue.parse(pubSubEvent.getEventData());
					JSONDecoder<UserSpecifiedSamplingParams> decoder = JSONDecoder.getDecoder(UserSpecifiedSamplingParams.class);
					decoder.decode(jsonObj, userSpec);

					ArchiveEngine.getArchiveInfo(pvName, configService, extraFields, userSpec.isUsePVAccess(), new ArchivePVMetaCompletedListener(pvName, configService, myIdentity));
					PubSubEvent confirmationEvent = new PubSubEvent("MetaInfoRequested", pubSubEvent.getSource() + "_" + ConfigService.WAR_FILE.MGMT, pvName);
					configService.getEventBus().post(confirmationEvent);
				} catch(Exception ex) {
					logger.error("Exception requesting metainfo for pv " + pvName, ex);
				}
			} else if(pubSubEvent.getType().equals("StartArchivingPV")) {
				String pvName = pubSubEvent.getPvName();
				try { 
					this.startArchivingPV(pvName);
					PubSubEvent confirmationEvent = new PubSubEvent("StartedArchivingPV", pubSubEvent.getSource() + "_" + ConfigService.WAR_FILE.MGMT, pvName);
					configService.getEventBus().post(confirmationEvent);
				} catch(Exception ex) {
					logger.error("Exception beginnning archiving pv " + pvName, ex);
				}
			} else if(pubSubEvent.getType().equals("AbortComputeMetaInfo")) {
				String pvName = pubSubEvent.getPvName();
				try { 
					logger.warn("AbortComputeMetaInfo called for " + pvName);
					this.abortComputeMetaInfo(pvName);
					// PubSubEvent confirmationEvent = new PubSubEvent("MetaInfoAborted", pubSubEvent.getSource() + "_" + ConfigService.WAR_FILE.MGMT, pvName);
					// configService.getEventBus().post(confirmationEvent);
				} catch(Exception ex) { 
					logger.error("Exception aborting metainfo for PV " + pvName, ex);
				}
			}
		} else {
			logger.debug("Skipping processing event meant for " + pubSubEvent.getDestination());
		}
		
	}
	
	/**
	 * A class that loops thru the archive channels and checks for connectivity.
	 * We start connecting up the metachannels only after a certain percentage of channels have connected up.
	 * @author mshankar
	 *
	 */
	private final class DisconnectChecker implements Runnable {
		private final ConfigService configService;

		private DisconnectChecker(ConfigService configService) {
			this.configService = configService;
		}

		@Override
		public void run() {
			try { 
				// We run thru all the channels - if a channel has not reconnected in disconnectCheckTimeoutInMinutes, we pause and resume the channel.
				if(EngineContext.this.configService.isShuttingDown()) {
					logger.debug("Skipping checking for disconnected channels as the system is shutting down.");
					return;
				}
				logger.debug("Checking for disconnected channels.");
				LinkedList<String> disconnectedPVNames = new LinkedList<String>();
				LinkedList<String> needToStartMetaChannelPVNames = new LinkedList<String>();
				int totalChannels = EngineContext.this.channelList.size();
				long disconnectTimeoutInSeconds = EngineContext.this.disconnectCheckTimeoutInMinutes*60;
				for(ArchiveChannel channel : EngineContext.this.channelList.values()) {
					if(!channel.isConnected()) {
						logger.debug(channel.getName() + " is not connected. See if we have requested for it some time back and have still not connected.");
						if(disconnectTimeoutInSeconds > 0 && channel.getSecondsElapsedSinceSearchRequest() > disconnectTimeoutInSeconds) { 
							disconnectedPVNames.add(channel.getName());
						} else {
							if(disconnectTimeoutInSeconds > 0) { 
								logger.debug(channel.getName() + " is not connected but we still have some time to go before attempting pause/resume " + channel.getSecondsElapsedSinceSearchRequest() + " and disconnectTimeoutInSeconds " + disconnectTimeoutInSeconds);
							} else { 
								logger.debug("The pause/resume on disconnect has been turned off. Not attempting reconnect using pause/resume for PV " + channel.getName());
							}
						}
					} else { 
						// Channel is connected.
						logger.debug(channel.getName() + " is connected. Seeing if we need to start up the meta channels for the fields.");
						if(channel.metaChannelsNeedStartingUp()) { 
							needToStartMetaChannelPVNames.add(channel.getName());
						}
					}
				}

				int disconnectedChannels = disconnectedPVNames.size();

				// Need to start up the metachannels here after we determine that the cluster has started up..
				// To do this we update the connected/disconnected count for this appliance.
				// We fire up the metachannels gradually only after the entire cluster's connected PV count has reached a certain threshold.
				// First we see if the percentage of disconnected channels in this appliance is lower than a threshold
				if(!needToStartMetaChannelPVNames.isEmpty()) {   
					if ((disconnectedChannels*100.0)/totalChannels < MAXIMUM_DISCONNECTED_CHANNEL_PERCENTAGE_BEFORE_STARTING_METACHANNELS) {
						boolean kickOffMetaChannels = true;
						// Then we repeat the same check for the other appliances in this cluster
						for(ApplianceInfo applianceInfo : configService.getAppliancesInCluster()) { 
							if(applianceInfo.getIdentity().equals(configService.getMyApplianceInfo().getIdentity())) { 
								// We do not check for ourself...
							} else { 
								String connectedPVCountURL = applianceInfo.getEngineURL() + "/getConnectedPVCountForAppliance";
								try { 
									JSONObject connectedPVCount = GetUrlContent.getURLContentAsJSONObject(connectedPVCountURL);
									int applianceTotalPVCount = Integer.parseInt((String) connectedPVCount.get("total"));
									int applianceDisconnectedPVCount = Integer.parseInt((String) connectedPVCount.get("disconnected"));
									if ((applianceDisconnectedPVCount*100.0/applianceTotalPVCount) < MAXIMUM_DISCONNECTED_CHANNEL_PERCENTAGE_BEFORE_STARTING_METACHANNELS) { 
										logger.debug("Appliance " + applianceInfo.getIdentity() + " has connected to most of its channels");
									} else { 
										logger.info("Appliance " + applianceInfo.getIdentity() + " has not connected to most of its channels. Skipping starting of meta channels");
										kickOffMetaChannels = false;
										break;
									}
								} catch(Exception ex) { 
									logger.error("Exception checking for disconnected PVs on appliance " + applianceInfo.getIdentity() + " using URL " + connectedPVCountURL, ex);
								}
							}
						}

						if(kickOffMetaChannels && !needToStartMetaChannelPVNames.isEmpty()) { 
							// We can kick off the metachannels. We kick them off a few at a time.
							for (int i = 0; i < METACHANNELS_TO_START_AT_A_TIME; i++) { 
								String channelPVNameToKickOffMetaFields = needToStartMetaChannelPVNames.poll();
								if(channelPVNameToKickOffMetaFields != null) { 
									logger.debug("Starting meta channels for " + channelPVNameToKickOffMetaFields);
									ArchiveChannel channelToKickOffMetaFields = EngineContext.this.channelList.get(channelPVNameToKickOffMetaFields);
									channelToKickOffMetaFields.startUpMetaChannels();
								} else { 
									logger.debug("No more metachannels to start");
									break;
								}
							}
						}
					}
				}
			} catch(Throwable t) { 
				logger.error("Exception doing the pause/resume checks", t);
			}
		}
	}

	static class ArchivePVMetaCompletedListener implements MetaCompletedListener {
		String pvName;
		ConfigService configService;
		String myIdentity;
		ArchivePVMetaCompletedListener(String pvName, ConfigService configService, String myIdentity) {
			this.pvName = pvName;
			this.configService = configService;
			this.myIdentity = myIdentity;
		}
		
		
		@Override
		public void completed(MetaInfo metaInfo) {
			try { 
				logger.debug("Completed computing archive info for pv " + pvName);
				PubSubEvent confirmationEvent = new PubSubEvent("MetaInfoFinished", myIdentity + "_" + ConfigService.WAR_FILE.MGMT, pvName);
				JSONEncoder<MetaInfo> encoder = JSONEncoder.getEncoder(MetaInfo.class);
				JSONObject metaInfoObj = encoder.encode(metaInfo);
				confirmationEvent.setEventData(JSONValue.toJSONString(metaInfoObj));
				configService.getEventBus().post(confirmationEvent);
			} catch(Exception ex) {
				logger.error("Exception sending across metainfo for pv " + pvName, ex);
			}
		}
	}
	
	
	private void startArchivingPV(String pvName) throws Exception {
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.error("Unable to find pvTypeInfo for PV" + pvName + ". This is an error; this method should be called after the pvTypeInfo has been determined and settled in the DHT");
			throw new IOException("Unable to find pvTypeInfo for PV" + pvName);
		}

		ArchDBRTypes dbrType = typeInfo.getDBRType();
		// The first data store in the policy is always the first destination; hence thePolicy.getDataStores()[0]
		StoragePlugin firstDest = StoragePluginURLParser.parseStoragePlugin(typeInfo.getDataStores()[0], configService);
		SamplingMethod samplingMethod = typeInfo.getSamplingMethod();
		float samplingPeriod = typeInfo.getSamplingPeriod();
		int secondsToBuffer = PVTypeInfo.getSecondsToBuffer(configService);
		Timestamp lastKnownTimeStamp = typeInfo.determineLastKnownEventFromStores(configService);
		String controllingPV = typeInfo.getControllingPV();
		String[] archiveFields = typeInfo.getArchiveFields();
		
		logger.info("Archiving PV " + pvName + "using " + samplingMethod.toString() + " with a sampling period of "+ samplingPeriod + "(s)");
		ArchiveEngine.archivePV(pvName, samplingPeriod, samplingMethod, secondsToBuffer, firstDest, configService, dbrType, lastKnownTimeStamp, controllingPV, archiveFields, typeInfo.getHostName(), typeInfo.isUsePVAccess(), typeInfo.isUseDBEProperties()); 
	}
	
	
	public boolean abortComputeMetaInfo(String pvName) { 
		return MetaGet.abortMetaGet(pvName);
	}

	/**
	 * @param newDisconnectCheckTimeoutMins
	 * This is to be used only for unit testing purposes...
	 * There are no guarantees that using this on a running server will be benign.
	 */
	public void setDisconnectCheckTimeoutInMinutesForTestingPurposesOnly(int newDisconnectCheckTimeoutMins) { 
		logger.error("Changing the disconnect timer - this should be done only in the unit tests.");
		disconnectFuture.cancel(false);
		this.disconnectCheckTimeoutInMinutes = newDisconnectCheckTimeoutMins;
		this.disconnectCheckerPeriodInMinutes = newDisconnectCheckTimeoutMins;
		if(this.miscTasksScheduler != null) { 
			logger.info("Shutting down the engine scheduler for misc tasks.");
			miscTasksScheduler.shutdown();
			this.miscTasksScheduler = null;
		}
		this.startMiscTasksScheduler(configService);
	}
	
	
	/**
	 * Go thru all the contexts and return channels whose names match this
	 * This is to be used for for testing purposes only.
	 * This may not work in running servers; so, please avoid use outside unit tests.
	 */
	public class CommandThreadChannel { 
		JCACommandThread commandThread;
		Channel channel;
		public CommandThreadChannel(JCACommandThread commandThread, Channel channel) {
			this.commandThread = commandThread;
			this.channel = channel;
		}
		public JCACommandThread getCommandThread() {
			return commandThread;
		}
		public Channel getChannel() {
			return channel;
		}
	}
	
	
	public List<CommandThreadChannel> getAllChannelsForPV(String pvName) {
		LinkedList<CommandThreadChannel> retval = new LinkedList<CommandThreadChannel>();
		String pvNameOnly = pvName.split("\\.")[0];
		for(JCACommandThread command_thread : this.command_threads) { 
			Context context = command_thread.getContext();
			for(Channel channel : context.getChannels()) { 
				String channelNameOnly = channel.getName().split("\\.")[0];
				if(channelNameOnly.equals(pvNameOnly)) { 
					retval.add(new CommandThreadChannel(command_thread, channel));
				}
			}
		}
		return retval;
	}

	/**
	 * Per FRIB/PSI, we have a configuration knob to increase/decrease the sample buffer size used by the engine for all PV's.
	 * This comes from archappl.properties and is a double - by default 1.0 which means we leave the buffer size computation as is.
	 * If you want to increase buffer size globally to 150% of what is normally computed, set this to 1.5  
	 * @return sampleBufferCapacityAdjustment  &emsp;
	 */
	public double getSampleBufferCapacityAdjustment() {
		return sampleBufferCapacityAdjustment;
	}
	
	
	/**
	 * Use EPICS_V3_PV's updateTotalMetaInfo to update the metadata once every 24 hours.  
	 * @author mshankar
	 *
	 */
	private class MetadataUpdater implements Runnable { 
		public void run() {
			logger.info("Starting the daily update of metadata information");
			int pvCount = 0;
			for(ArchiveChannel channel : EngineContext.this.channelList.values()) {
				if(channel.isConnected()) {
					logger.debug("Updating metadata for " + channel.getName());
					channel.updateMetadataOnceADay(EngineContext.this);
					pvCount++;
					// 100,000 PVs should complete in 100,000/(100*(1000/250)) seconds approx 5 minutes
					if(pvCount %100 == 0) { 
						try {Thread.sleep(250); } catch(Throwable t) {}
					}
				}
			}
			logger.info("Completed scheduling the daily update of metadata information");
		}
	}
	
	
	/**
	 * Get the total channel count as CAJ sees it.
	 * @return totalCAJChannelCount  &emsp;
	 */
	public int getCAJChannelCount() { 
		int totalCAJChannelCount = 0;
		for(int threadNum = 0; threadNum < command_threads.length; threadNum++) {
			Context context = this.command_threads[threadNum].getContext();
			totalCAJChannelCount += context.getChannels().length;
		}
		return totalCAJChannelCount;
	}
	
	
    private void iniV4ChannelProvidert() {
        if (channelProvider == null) {
                org.epics.pvaccess.ClientFactory.start();
                logger.info("Registered the pvAccess client factory.");
                channelProvider = ChannelProviderRegistryFactory.getChannelProviderRegistry().getProvider(org.epics.pvaccess.ClientFactory.PROVIDER_NAME);

                for(String providerName : ChannelProviderRegistryFactory.getChannelProviderRegistry().getProviderNames()) {
                        logger.debug("PVAccess Channel provider " + providerName);
                }
        }
    }
	public ChannelProvider getChannelProvider() {
		return channelProvider;
	}
	
	public ScheduledThreadPoolExecutor getMiscTasksScheduler() {
		return miscTasksScheduler;
	}
	
	/**
	 * Get the number of tasks pending in the main scheduler. 
	 * This is the one that powers the write thread.
	 * @return
	 */
	public int getMainSchedulerPendingTasks() {
		if(scheduler != null) { 
			return scheduler.getQueue().size();
		} else {
			return -1;
		}
	}
	
	
	/**
	 * Return some details on the CAJ contexts for the metrics page.
	 * @return
	 */
	public List<Map<String, String>> getCAJContextDetails() {
		List<Map<String, String>> ret = new LinkedList<Map<String, String>>();

		int channelsWithPendingSearchRequests = 0;
		int totalChannels = 0;

		for(Context context : this.context2CommandThreadId) {
			if(context instanceof CAJContext) {
				CAJContext cajContext = (CAJContext) context;
				for(Channel channel : cajContext.getChannels()) {
					CAJChannel cajChannel = (CAJChannel) channel;
					totalChannels++;
					if(cajChannel.getTimerId() != null) channelsWithPendingSearchRequests++;
				}
			}
		}

		Map<String, String> obj = new LinkedHashMap<String, String>();
		obj.put("name", "Channels with pending search requests");
		obj.put("value", channelsWithPendingSearchRequests + " of " + totalChannels);
		obj.put("source", "engine");
		ret.add(obj);
		return ret;
	}
}
