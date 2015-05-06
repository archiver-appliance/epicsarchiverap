/*******************************************************************************

 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University

 * as Operator of the SLAC National Accelerator Laboratory.

 * Copyright (c) 2011 Brookhaven National Laboratory.

 * EPICS archiver appliance is distributed subject to a Software License Agreement found

 * in file LICENSE that is included with this distribution.

 *******************************************************************************/

package org.epics.archiverappliance.engine;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.engine.metadata.MetaCompletedListener;
import org.epics.archiverappliance.engine.metadata.MetaGet;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.model.DeltaArchiveChannel;
import org.epics.archiverappliance.engine.model.Enablement;
import org.epics.archiverappliance.engine.model.MonitoredArchiveChannel;
import org.epics.archiverappliance.engine.model.SampleMode;
import org.epics.archiverappliance.engine.model.ScannedArchiveChannel;
import org.epics.archiverappliance.engine.pv.EPICS_V3_PV;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PV;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.engine.pv.EPICSV4.ArchiveEngine_EPICSV4;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;

/**
 * This class provides the static methods:
 * <ol>
	 * <li>create channel</li>
	 * <li>get pv meta data before archiving this pv</li>
	 * <li>pause archiving pv</li>
	 * <li>resume archiving pv</li>
	 * <li>get metics of one pv</li>
	 * <li>change the archival parameter of one pv </li>
	 * <li>destory pv</li>
 * </ol>
 * @author Luofeng Li
 *
 */

public class ArchiveEngine {
	private static final Logger logger = Logger.getLogger(ArchiveEngine.class.getName());

	/**
	 * Create the channel for the PV and register it in the places where it needs to be registered.
	 * @param name
	 * @param writer
	 * @param enablement
	 * @param sample_mode
	 * @param last_sampleTimestamp
	 * @param configservice
	 * @param archdbrtype
	 * @param controlPVname
	 * @param isMetaField
	 * @param parentChannel - Should not be null if this is a metafield.
	 * @param iocHostName - Can be null.
	 * @return
	 * @throws Exception
	 */
	private static ArchiveChannel addChannel(final String name, final Writer writer, 
			final Enablement enablement, final SampleMode sample_mode, 
			final Timestamp last_sampleTimestamp, 
			final ConfigService configservice, final ArchDBRTypes archdbrtype, 
			final String controlPVname, final boolean isMetaField, final ArchiveChannel parentChannel, final String iocHostName) throws Exception {
		EngineContext engineContext = configservice.getEngineContext();
		ArchiveChannel channel = null;
		if(!isMetaField){
			// Is this an existing channel?
			channel= engineContext.getChannelList().get(name);
			if (channel != null) {
					logger.debug(String.format(" Channel '%s' already exist'", name));
					return channel;
			}
		} else {
			String pvName = PVNames.normalizePVName(name);
			channel= engineContext.getChannelList().get(pvName);
			if(channel != null) { 
				ArchiveChannel metaChannel = channel.getMetaChannel(PVNames.getFieldName(name));
				if(metaChannel != null) { 
					logger.debug(String.format(" Channel '%s' already exist'", name));
					return channel;
				}
			}
		}

		// Determine buffer capacity
		double write_period = configservice.getEngineContext().getWritePeriod();
		double pvSamplingPeriod = sample_mode.getPeriod();
		if(pvSamplingPeriod <= 0.0) {
			logger.warn("Sampling period is invalid " + pvSamplingPeriod + ". Resetting this to " + PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD);
			pvSamplingPeriod = PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
		}

		int buffer_capacity = ((int) Math.round(Math.max((write_period/pvSamplingPeriod)*engineContext.getSampleBufferCapacityAdjustment(), 1.0))) + 1;
		if (buffer_capacity < 2) {
			logger.debug("Enforcing a minimum capacity for sample buffer size.");
			buffer_capacity = 2;
		}
		if(isMetaField) { 
			// logger.debug("We store meta fields as part of the main PV; don't need much capacity here.");
			buffer_capacity = 2;
		}
		logger.debug("Final buffer capacity for pv " + name + " is " + buffer_capacity);
		
		int JCACommandThreadID = engineContext.assignJCACommandThread(name, iocHostName);
		if(isMetaField) { 
			if(parentChannel == null) { 
				throw new Exception("Trying to create a meta field without specifying the parent channel");
			}
			JCACommandThreadID = parentChannel.getJCACommandThreadID();
		}

		// Create new channel
		if (sample_mode.isMonitor()) {
			if (sample_mode.getDelta() > 0) {
				channel = new DeltaArchiveChannel(name, writer, enablement, buffer_capacity, last_sampleTimestamp, pvSamplingPeriod, sample_mode.getDelta(),configservice, archdbrtype, controlPVname, isMetaField, JCACommandThreadID);
			} else { 
				channel = new MonitoredArchiveChannel(name, writer, enablement, buffer_capacity, last_sampleTimestamp, pvSamplingPeriod, configservice, archdbrtype, controlPVname, isMetaField,JCACommandThreadID);
			}
		} else {
			channel = new ScannedArchiveChannel(name, writer, enablement, buffer_capacity, last_sampleTimestamp, pvSamplingPeriod, configservice, archdbrtype, controlPVname, isMetaField,JCACommandThreadID);
		}

		if(!isMetaField) { 
			configservice.getEngineContext().getChannelList().put(channel.getName(), channel);
			engineContext.getWriteThead().addChannel(channel);
		} else { 
			// Do nothing..
		}

		return channel;
	}



	private static void createChannels4PVWithMetaField(final String pvName,
			final float samplingPeriod, final SamplingMethod mode,
			final int secondstoBuffer, final Writer writer,
			final ConfigService configservice, final ArchDBRTypes archdbrtype,
			final Timestamp lastKnownEventTimeStamp, final boolean start, final String controlPVname, final String[] metaFields, final String iocHostName) throws Exception {
		EngineContext engineContext = configservice.getEngineContext();
		ScheduledThreadPoolExecutor scheduler = engineContext.getScheduler();

		if (!engineContext.isWriteThreadStarted()) {
			engineContext.startWriteThread(configservice);
		}
		
		HashSet<String> runtTimeFieldsCopy = new HashSet<String>(configservice.getRuntimeFields());
		if (mode == SamplingMethod.SCAN) {
			SampleMode scan_mode2 = new SampleMode(false, 0, samplingPeriod);
			ArchiveChannel channel = ArchiveEngine.addChannel(pvName, writer,
					Enablement.Enabling, scan_mode2, lastKnownEventTimeStamp,
					configservice, archdbrtype, controlPVname, false, null, iocHostName);
			if (metaFields != null) { 
				channel.setHasMetaField(true);
			}

			if (start) {
				channel.start();
			}

			scheduler.scheduleAtFixedRate((ScannedArchiveChannel) channel, 0,
					(long) (samplingPeriod * 1000), TimeUnit.MILLISECONDS);



			// handle the meta field
			if(metaFields != null && metaFields.length > 0) { 
				for (String fieldName : metaFields) {
					logger.debug("Adding monitor for meta field " + fieldName);
					SampleMode monitor_mode3 = new SampleMode(true, 0, samplingPeriod);
					ArchiveChannel channel3 = ArchiveEngine.addChannel(pvName + "."
							+ fieldName, writer, Enablement.Enabling, monitor_mode3,
							null, configservice, null,
							controlPVname, true, channel, iocHostName);

					channel3.setPVChannelWhereThisMetaFieldIn(channel);
					// We do not start the meta field unless the main PV is connected in order to speedup reconnect times
					runtTimeFieldsCopy.remove(fieldName);
				}

				for(String runtimeField : runtTimeFieldsCopy) { 
					logger.debug("Adding monitor for runtime field " + runtimeField);
					SampleMode monitor_mode3 = new SampleMode(true, 0, samplingPeriod);
					ArchiveChannel channel3 = ArchiveEngine.addChannel(pvName + "." + runtimeField, writer, Enablement.Enabling, monitor_mode3,
							null, configservice, null,
							controlPVname, true, channel, iocHostName);
					channel3.setPVChannelWhereThisMetaFieldIn(channel, true);
					// We do not start the meta field unless the main PV is connected in order to speedup reconnect times
				}
			}
		} else if (mode == SamplingMethod.MONITOR) {
			SampleMode scan_mode2 = new SampleMode(true, 0, samplingPeriod);
			ArchiveChannel channel = ArchiveEngine.addChannel(pvName, writer,
					Enablement.Enabling, scan_mode2, lastKnownEventTimeStamp,
					configservice, archdbrtype, controlPVname, false, null, iocHostName);

			if (metaFields != null) { 
				channel.setHasMetaField(true);
			}

			if (start) { 
				channel.start();
			}

			// handle the meta field
			if(metaFields != null && metaFields.length > 0) { 
				for (String fieldName : metaFields) {
					logger.debug("Adding monitor for meta field " + fieldName);
					SampleMode monitor_mode3 = new SampleMode(true, 0, samplingPeriod);
					ArchiveChannel channel3 = ArchiveEngine.addChannel(pvName + "."
							+ fieldName, writer, Enablement.Enabling, monitor_mode3,
							null, configservice, null,
							controlPVname, true, channel, iocHostName);

					channel3.setPVChannelWhereThisMetaFieldIn(channel);
					// We do not start the meta field unless the main PV is connected in order to speedup reconnect times
					runtTimeFieldsCopy.remove(fieldName);
				}

				for(String runtimeField : runtTimeFieldsCopy) { 
					logger.debug("Adding monitor for runtime field " + runtimeField);
					SampleMode monitor_mode3 = new SampleMode(true, 0, samplingPeriod);
					ArchiveChannel channel3 = ArchiveEngine.addChannel(pvName + "." + runtimeField, writer, Enablement.Enabling, monitor_mode3,
							null, configservice, null,
							controlPVname, true, channel, iocHostName);
					channel3.setPVChannelWhereThisMetaFieldIn(channel, true);
					// We do not start the meta field unless the main PV is connected in order to speedup reconnect times
				}
			}
		} else if (mode == SamplingMethod.DONT_ARCHIVE) {
			// Do nothing..
		}
	}



	/**
	 * Get the meta data for pv - used for policy computation.
	 * 
	 * @param pvName Name of the channel (PV)
	 * @param configservice
	 * @param metadatafields other field such as MDEL,ADEL, except basical info in DBR_CTRL
	 * @param metaListener the callback interface where you handle the info.
	 * @return the info of pv
	 * @throws Exception
	 *             On error in getting pv's info
	 */
	public static void getArchiveInfo(final String pvName,
			final ConfigService configservice, final String metadatafields[],
			final MetaCompletedListener metaListener) throws Exception {
		MetaGet metaget = new MetaGet(pvName, configservice, metadatafields, metaListener);
		metaget.initpv();
	}



	/**
	 * See the {@link #archivePV(String, float, SamplingMethod, int, Writer, ConfigService, ArchDBRTypes, Timestamp, String, String[]) full} method
	 */
	public static void archivePV(final String pvName,
			final float samplingPeriod, final SamplingMethod mode,
			final int secondstoBuffer, final Writer writer,
			final ConfigService configservice, final ArchDBRTypes archdbrtype,
			final Timestamp lastKnownEventTimeStamp, final String controllingPVName) throws Exception {
		archivePV(pvName, samplingPeriod, mode, secondstoBuffer, writer, configservice, archdbrtype, lastKnownEventTimeStamp, controllingPVName, null, null);
	}



	/**
	 * See the {@link #archivePV(String, float, SamplingMethod, int, Writer, ConfigService, ArchDBRTypes, Timestamp, String, String[]) full} method
	 */
	public static void archivePV(final String pvName,
			final float samplingPeriod, final SamplingMethod mode,
			final int secondstoBuffer, final Writer writer,
			final ConfigService configservice, final ArchDBRTypes archdbrtype, final Timestamp lastKnownEventTimeStamp) throws Exception {
		archivePV(pvName, samplingPeriod, mode, secondstoBuffer, writer, configservice, archdbrtype, lastKnownEventTimeStamp, null, null, null);
	}



	/**
	 * See the {@link #archivePV(String, float, SamplingMethod, int, Writer, ConfigService, ArchDBRTypes, Timestamp, String, String[]) full} method
	 */
	public static void archivePV(final String pvName,
			final float samplingPeriod, final SamplingMethod mode,
			final int secondstoBuffer, final Writer writer,
			final ConfigService configservice, final ArchDBRTypes archdbrtype,
			final Timestamp lastKnownEventTimeStamp,
			final String[] metaFieldNames) throws Exception {
		archivePV(pvName, samplingPeriod, mode, secondstoBuffer, writer, configservice, archdbrtype, lastKnownEventTimeStamp, null, metaFieldNames, null);
	}



	/**
	 * Create a new channel in monitor mode or in scan mode
	 * @param pvName - Name of the channel (PV)
	 * @param samplingPeriod - The minimal sample period for channel in scan mode.  Attention: the same data with same value and timestamp is not saved again in scan mode. This period is meanlingless for channel in monitor mode.
	 * @param mode - scan or monitor
	 * @param secondstoBuffer - Not really used
	 * @param writer - First destination 
	 * @param configservice 
	 * @param archdbrtype - Expected DBR type. 
	 * @param lastKnownEventTimeStamp - Last known event from all the stores.
	 * @param controllingPVName - The PV that controls archiving for this PV
	 * @param metaFieldNames - An array of EPICS fields that gets stored along with the stream. Needs rethinking once we have EPICS V4
	 * @param iocHostName - IOC hosting this PV; this is used for some optimization. This will often be null.
	 * @throws Exception
	 */
	public static void archivePV(final String pvName, 
			final float samplingPeriod, final SamplingMethod mode, 
			final int secondstoBuffer, final Writer writer, 
			final ConfigService configservice, final ArchDBRTypes archdbrtype,
			final Timestamp lastKnownEventTimeStamp,
			final String controllingPVName, final String[] metaFieldNames, final String iocHostName) throws Exception {

		boolean start = true;
		if (controllingPVName != null) {
			ConcurrentHashMap<String, PV> controlingPVList = configservice .getEngineContext().getControlingPVList();
			PV controllingPV = controlingPVList.get(controllingPVName);

			if (controllingPV == null) {
				ArchiveEngine.createChannels4PVWithMetaField(pvName, samplingPeriod, mode, secondstoBuffer, writer, configservice, archdbrtype, lastKnownEventTimeStamp, start, controllingPVName, metaFieldNames, iocHostName);
				controllingPV = new EPICS_V3_PV(controllingPVName, configservice, true, archdbrtype, null, configservice.getEngineContext().assignJCACommandThread(controllingPVName, null));
				controlingPVList.put(controllingPVName, controllingPV);
				controllingPV.addControledPV(pvName);
				controllingPV.start();
			} else {
				controllingPV.addControledPV(pvName);
				if (((EPICS_V3_PV) controllingPV).isEnableAllPV()) {
					start = true;
				} else {
					start = false;
				}

				ArchiveEngine.createChannels4PVWithMetaField(pvName,
						samplingPeriod, mode, secondstoBuffer, writer,
						configservice, archdbrtype, lastKnownEventTimeStamp,
						start, controllingPVName, metaFieldNames, iocHostName);
			}
		} else {
			ArchiveEngine.createChannels4PVWithMetaField(pvName,
					samplingPeriod, mode, secondstoBuffer, writer,
					configservice, archdbrtype, lastKnownEventTimeStamp, start,
					null, metaFieldNames, iocHostName);
		}
	}

	/**
	 * pause the pv
	 * 
	 * @param pvName
	 *            Name of the channel (PV)
	 * @param configservice 
	 * @throws Exception
	 *              error in pausing the channel .
	 */
	public static void pauseArchivingPV(final String pvName, ConfigService configservice) throws Exception {
		EngineContext engineContext = configservice.getEngineContext();
       // pause the pv
		ArchiveChannel channel = engineContext.getChannelList().get(pvName);
		if (channel != null) {
			channel.shutdownMetaChannels();
			channel.stop();
			destoryPv(pvName, configservice);
		}
	}



	/**
	 * restart the pv
	 * 
	 * @param pvName
	 *            Name of the channel (PV)
	 * @throws Exception
	 *              error in restarting the channel .
	 */

	public static void resumeArchivingPV(final String pvName, ConfigService configservice) throws Exception {
		EngineContext engineContext = configservice.getEngineContext();
		ArchiveChannel channel = engineContext.getChannelList().get(pvName);
		if (channel != null) {
			channel.stop();
			channel.start();
		} else { 
			// We have not created the channel on startup.
			// We should start it up
			logger.debug("We had not created the channel on startup. Creating it " + pvName);
			startChannelsForPV(pvName, configservice);
		}
	}



	/**
	 * Start up the channels for a PV.
	 * Should be called on startup or on resume of a PV that was paused on startup.
	 * @param pvName
	 * @param configservice
	 * @throws IOException
	 * @throws Exception
	 */
	public static void startChannelsForPV(final String pvName, ConfigService configservice) throws IOException, Exception {
		logger.debug("Starting up channels for pv " + pvName);
		PVTypeInfo typeInfo = configservice.getTypeInfoForPV(pvName);
		int secondsToBuffer = PVTypeInfo.getSecondsToBuffer(configservice);
		if(typeInfo == null) { 
			logger.error("Cannot resume PV for which we cannot typeinfo " + pvName);
			return;
		}
		ArchDBRTypes dbrType = typeInfo.getDBRType();
		float samplingPeriod = typeInfo.getSamplingPeriod();
		SamplingMethod samplingMethod = typeInfo.getSamplingMethod();
		StoragePlugin firstDest = StoragePluginURLParser.parseStoragePlugin(typeInfo.getDataStores()[0], configservice);
		Timestamp lastKnownTimestamp = typeInfo.determineLastKnownEventFromStores(configservice);
		if(logger.isDebugEnabled()) logger.debug("Last known timestamp from ETL stores is for pv " + pvName + " is "+ TimeUtils.convertToHumanReadableString(lastKnownTimestamp));

		if(!dbrType.isV3Type()) {
			ArchiveEngine_EPICSV4.archivePV(pvName, samplingPeriod, samplingMethod, secondsToBuffer, firstDest, configservice, dbrType);
		} else {
			ArchiveEngine.archivePV(pvName, samplingPeriod, samplingMethod, secondsToBuffer, firstDest, configservice, dbrType,lastKnownTimestamp, typeInfo.getControllingPV(), typeInfo.getArchiveFields(), typeInfo.getHostName()); 
		}
	}



	/**
	 * get the pv's info and status
	 * @param pvName
	 *            Name of the channel (PV)
	 * @throws Exception
	 *             On error in getting the pv info and status .
	 */
	public static PVMetrics getMetricsforPV(String pvName, ConfigService configservice) throws Exception {
		EngineContext engineContext = configservice.getEngineContext();
		ArchiveChannel channel = engineContext.getChannelList().get(pvName);
		if (channel == null) { 
			return null;
		}
		return channel.getPVMetrics();
	}
	
	
	/**
	 * Return info from CAJ
	 * @param pvName
	 * @param configservice
	 * @return
	 * @throws Exception
	 */
	public static String getLowLevelStateInfo(String pvName, ConfigService configservice) throws Exception { 
		EngineContext engineContext = configservice.getEngineContext();
		ArchiveChannel channel = engineContext.getChannelList().get(pvName);
		if (channel == null)
			return null;
		return channel.getLowLevelChannelStateInfo();
	}



	/**
	 * change pv's sample period or sample mode
	 * 
	 * @param pvName
	 *            Name of the channel (PV)
	 * @param samplingPeriod
	 *            new sampling Period of the channel (PV)
	 * @param configservice
	 * @param writer
	 *            the writer to protocol buffer
	 * @throws Exception
	 *             On error in getting the pv info and status .
	 */
	public static void changeArchivalParameters(final String pvName,
			final float samplingPeriod, final SamplingMethod mode,
			final ConfigService configservice, final Writer writer) throws Exception {
		EngineContext engineContext = configservice.getEngineContext();
		ArchiveChannel channel = engineContext.getChannelList().get(pvName);
		if (channel == null) {
			throw new Exception(String.format(" Channel '%s' doesn't exist'", pvName));
		}

		PVMetrics pvMetrics = channel.getPVMetrics();
		boolean isMonitor = pvMetrics.isMonitor();
		double samplePeriodOld = pvMetrics.getSamplingPeriod();
		if (mode == SamplingMethod.SCAN) {
			if (isMonitor) {
				// mode is changed from monitor to scan,new mode is scan
				// stop channel and remove id from ChannelList and buffer
				channel.stop();
				engineContext.getWriteThead().removeChannel(pvName);
				// add new channel in scan mode
				ArchiveEngine.archivePV(pvName, samplingPeriod,
						SamplingMethod.SCAN,
						(int) engineContext.getWritePeriod(), writer,
						configservice, pvMetrics.getArchDBRTypes(), null);
			} else {
				// mode is not changed the mode is still scan
				// the new sample period is the same with the old sample period
				double perioddelt = Math.abs(samplePeriodOld - samplingPeriod);
				if (perioddelt < 0.1) {
					// the same sample period
					// do nothing
				} else {
					// different period
					engineContext.getScheduler().remove((ScannedArchiveChannel) channel);
					engineContext.getScheduler().purge();
					// stop channel and remove id from ChannelList and buffer

					channel.stop();
					engineContext.getWriteThead().removeChannel(pvName);
					// add new channel in scan mode
					ArchiveEngine.archivePV(pvName, samplingPeriod, SamplingMethod.SCAN, (int) engineContext.getWritePeriod(), writer, configservice, pvMetrics.getArchDBRTypes(), null);
				}
			}
		} else if (mode == SamplingMethod.MONITOR) {
			if (isMonitor) {
				// mode is not changed, is monior
				// remove the channel in monitor mode
				channel.stop();
				engineContext.getWriteThead().removeChannel(pvName);
				// add new channel in monitor mode
				ArchiveEngine.archivePV(pvName, samplingPeriod, SamplingMethod.MONITOR, (int) engineContext.getWritePeriod(), writer, configservice, pvMetrics.getArchDBRTypes(), null);
			} else {
				// mode is changed from scan to monitor ,new mode is monitor
				engineContext.getScheduler().remove((ScannedArchiveChannel) channel);
				engineContext.getScheduler().purge();
				channel.stop();
				engineContext.getWriteThead().removeChannel(pvName);

				// add new channel in monitor mode
				ArchiveEngine.archivePV(pvName, samplingPeriod, SamplingMethod.MONITOR, (int) engineContext.getWritePeriod(), writer, configservice, pvMetrics.getArchDBRTypes(), null);
			}
		}
	}

/**
 * destroy the pv
 * @param pvName  the pv's name to destroy
 * @param configservice the configSerivice of the application
 * @throws Exception
 *        error when destroy the PV
 */
	public static void destoryPv(String pvName, final ConfigService configservice) throws Exception {
		EngineContext engineContext = configservice.getEngineContext();
		ArchiveChannel channel = engineContext.getChannelList().get(pvName);

		if (channel == null) {
			logger.debug("Skipping deleting PV that does not have channel info " + pvName);
			return;
		}

		PVMetrics pvMetrics = channel.getPVMetrics();
		boolean isMonitor = pvMetrics.isMonitor();
		if (isMonitor) {
			// pv is in monitor mode
			// remove the channel in monitor mode
			channel.stop();
			engineContext.getWriteThead().removeChannel(pvName);
		} else {
			// pv is in scan mode
			// remove the channel in scan mode
			engineContext.getScheduler().remove((ScannedArchiveChannel) channel);
			engineContext.getScheduler().purge();
			channel.stop();
			engineContext.getWriteThead().removeChannel(pvName);
		}
	}
}
