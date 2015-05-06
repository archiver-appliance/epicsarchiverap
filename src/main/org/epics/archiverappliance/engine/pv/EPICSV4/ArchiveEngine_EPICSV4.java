package org.epics.archiverappliance.engine.pv.EPICSV4;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.metadata.MetaCompletedListener;
import org.epics.archiverappliance.engine.metadata.MetaGet;
import org.epics.archiverappliance.engine.model.Enablement;
import org.epics.archiverappliance.engine.model.SampleMode;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;

import edu.stanford.slac.archiverappliance.PB.data.PBScalarDouble;

public class ArchiveEngine_EPICSV4 {

	private static Logger logger = Logger.getLogger(ArchiveEngine_EPICSV4.class
			.getName());

	private static ArchiveChannel_EPICSV4 addChannel(final String name,
			final Enablement enablement, final SampleMode sample_mode,
			final java.sql.Timestamp last_sample_time,
			final ConfigService configservice, final ArchDBRTypes archdbrtype)
			throws Exception {
		EngineContext_EPICSV4 engineContext = EngineContext_EPICSV4
				.getInstance(configservice);

		// Is this an existing channel?
		ArchiveChannel_EPICSV4 channel = engineContext.getChannelList().get(
				name);
		if (channel != null)
		/*
		 * throw new Exception(String.format( " Channel '%s' already exist'",
		 * name));
		 */
		{
			logger.error(String.format(" Channel '%s' already exist'", name));
		}
		// Channel is new to this engine.
		// See if there's already a sample in the archive,
		// because we won't be able to go back-in-time before that sample.
		DBRTimeEvent last_sample;

		if (last_sample_time == null)
			last_sample = null;
		else {
			last_sample = new PBScalarDouble(TimeUtils.getCurrentYear(),
					new ByteArray("0".getBytes()));
		}

		// Determine buffer capacity
		double write_period = configservice.getEngineContext().getWritePeriod();
		double pvSamplingPeriod = sample_mode.getPeriod();
		if(pvSamplingPeriod <= 0.0) {
			logger.warn("Sampling period is invalid " + pvSamplingPeriod + ". Resetting this to " + PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD);
			pvSamplingPeriod = PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
		}
		
		int buffer_capacity = (int) Math.round(Math.max(write_period / pvSamplingPeriod, 1.0));

		if (buffer_capacity < 1) {
			logger.debug("Enforcing a minimum capacity for sample buffer size.");
			buffer_capacity = 1;
		}

		// Create new channel
		if (sample_mode.isMonitor()) {
			if (sample_mode.getDelta() > 0)
				channel = new DeltaArchiveChannel_EPICSV4(name, enablement,
						buffer_capacity, last_sample, sample_mode.getPeriod(),
						sample_mode.getDelta(), configservice, archdbrtype);
			else
				channel = new MonitoredArchiveChannel_EPICSV4(name, enablement,
						buffer_capacity, last_sample, sample_mode.getPeriod(),
						configservice, archdbrtype);
		} else {
			channel = new ScannedArchiveChannel_EPICSV4(name, enablement,
					buffer_capacity, last_sample, sample_mode.getPeriod(),
					configservice, archdbrtype);

		}

		EngineContext_EPICSV4.getInstance(configservice).getWriteThead()
				.addChannel(channel);

		return channel;
	}

	/**
	 * create a new channel in scan mode
	 * 
	 * @param pvName
	 *            Name of the channel (PV) suggestions: when pv changes faster
	 *            than 10 seconds, you'd better create channels in monitor mode
	 * @param scanperiod
	 *            the minimal sampling period . it is also used to compute the
	 *            buffer sizeAttention: the same data with same value and
	 *            timestamp is not saved again.
	 * @param secondstoBuffer
	 *            the writing period to storage. this param is initilize on by
	 *            the first channel when creating.
	 * @param configservice
	 * @param writer
	 *            writing to storage.
	 * @param archdbrtype
	 *            the dbr type of channel,
	 * @throws Exception
	 *             On error in creating channel in scan mode.
	 */

	private static void createScannedChannel(final String pvname,
			final float scanperiod, final int secondstoBuffer,
			final Writer writer, final ConfigService configservice,
			final ArchDBRTypes archdbrtype) throws Exception {
		EngineContext_EPICSV4 engineContext = EngineContext_EPICSV4
				.getInstance(configservice);
		ScheduledThreadPoolExecutor scheduler = engineContext.getScheduler();
		if (!engineContext.isWriteThreadStarted()) {
			engineContext.startWriteThread(secondstoBuffer, writer,
					configservice);
		}
		SampleMode scan_mode2 = new SampleMode(false, 0, scanperiod);
		ArchiveChannel_EPICSV4 channel = ArchiveEngine_EPICSV4.addChannel(
				pvname, Enablement.Enabling, scan_mode2, null, configservice,
				archdbrtype);
		channel.start();
		scheduler.scheduleAtFixedRate((ScannedArchiveChannel_EPICSV4) channel,
				0, (long) (scanperiod * 1000), TimeUnit.MILLISECONDS);

	}

	/**
	 * create a new channel in monitor mode
	 * 
	 * @param pvName
	 *            Name of the channel (PV)
	 * @param scanperiod
	 *            this period is used to compute the buffer size for channels in
	 *            monitor mode. So you should estimate the actual changing
	 *            period of pv to avoid data losing
	 * @param secondstoBuffer
	 *            the writing period to storage. this param is initialize on by
	 *            the first channel when creating.
	 * @param configservice
	 * @param writer
	 *            writing to storage.
	 * @param archdbrtype
	 *            the dbr type of channel,
	 * @throws Exception
	 *             On error in creating channel in monitor mode.
	 */

	private static void createMonitoredChannel(final String pvname,
			final float scanperiod, final int secondstoBuffer,
			final Writer writer, final ConfigService configservice,
			final ArchDBRTypes archdbrtype) throws Exception {
		EngineContext_EPICSV4 engineContext = EngineContext_EPICSV4
				.getInstance(configservice);
		if (!engineContext.isWriteThreadStarted())
			engineContext.startWriteThread(secondstoBuffer, writer,
					configservice);
		SampleMode scan_mode2 = new SampleMode(true, 0, scanperiod);
		ArchiveChannel_EPICSV4 channel = ArchiveEngine_EPICSV4.addChannel(
				pvname, Enablement.Enabling, scan_mode2, null, configservice,
				archdbrtype);
		channel.start();

	}

	/**
	 * get the meta data for pv
	 * 
	 * @param pvName
	 *            Name of the channel (PV)
	 * @param configservice
	 * @param metadatafields
	 *            other field such as MDEL,ADEL, except basical info in DBR_CTRL
	 * @param metaListener
	 *            the callback interface where you handle the info.
	 * @return the info of pv
	 * @throws Exception
	 *             On error in getting pv's info
	 */

	public static void getArchiveInfo(final String pvName,
			final ConfigService configservice, final String metadatafields[],
			final MetaCompletedListener metaListener) throws Exception {
		ScheduledThreadPoolExecutor scheduler = EngineContext_EPICSV4
				.getInstance(configservice).getScheduler();

		MetaGet metaget = new MetaGet(pvName, configservice, metadatafields,
				metaListener);
		metaget.initpv();
		scheduler.schedule(metaget, 60, TimeUnit.SECONDS);
	}

	/**
	 * create a new channel in monitor mode or in scan mode
	 * 
	 * @param pvName
	 *            Name of the channel (PV)
	 * @param samplingPeriod
	 *            the minimal sample period for channel in scan mode.Attention:
	 *            the same data with same value and timestamp is not saved again
	 *            in scan mode. This period is meanlingless for channel in
	 *            monitor mode.
	 * @param secondstoBuffer
	 *            the writing period to storage. this param is initilize on by
	 *            the first channel when creating.
	 * @param configservice
	 * @param writer
	 *            writing to storage.
	 * @param archdbrtype
	 *            the dbr type of channel,
	 * @throws Exception
	 *             On error in creating channel .
	 */

	public static void archivePV(final String pvName,
			final float samplingPeriod, final SamplingMethod mode,
			final int secondstoBuffer, final Writer writer,
			final ConfigService configservice, final ArchDBRTypes archdbrtype)
			throws Exception {
		if (mode == SamplingMethod.SCAN) {

			createScannedChannel(pvName, samplingPeriod, secondstoBuffer,
					writer, configservice, archdbrtype);
		} else if (mode == SamplingMethod.MONITOR) {

			createMonitoredChannel(pvName, samplingPeriod, secondstoBuffer,
					writer, configservice, archdbrtype);
		} else if (mode == SamplingMethod.DONT_ARCHIVE) {

		}
	}

	/**
	 * pause the pv
	 * 
	 * @param pvName
	 *            Name of the channel (PV)
	 * @throws Exception
	 *             On error in pausing the channel .
	 */

	public static void pauseArchivingPV(final String pvName,
			ConfigService configservice) throws Exception {
		EngineContext_EPICSV4 engineContext = EngineContext_EPICSV4
				.getInstance(configservice);

		ArchiveChannel_EPICSV4 channel = engineContext.getChannelList().get(
				pvName);
		if (channel != null) {
			channel.stop();
		}
	}

	/**
	 * restart the pv
	 * 
	 * @param pvName
	 *            Name of the channel (PV)
	 * @throws Exception
	 *             On error in restarting the channel .
	 */
	public static void resumeArchivingPV(final String pvName,
			ConfigService configservice) throws Exception {
		EngineContext_EPICSV4 engineContext = EngineContext_EPICSV4
				.getInstance(configservice);

		ArchiveChannel_EPICSV4 channel = engineContext.getChannelList().get(
				pvName);
		if (channel != null) {

			channel.start();
		}
	}

	/**
	 * get the pv's info and status, including:
	 * isArchving,ArchDBRTypes,elementCount
	 * ,sampleMode,samplingPeriod,isConnected,lastRotateLogsEpochSeconds,
	 * connectionFirstEstablishedEpochSeconds
	 * ,connectionLastRestablishedEpochSeconds,connectionLossRegainCount,
	 * EventRate,StorageRate
	 * 
	 * @param pvName
	 *            Name of the channel (PV)
	 * @throws Exception
	 *             On error in getting the pv info and status .
	 */

	public static PVMetrics getMetricsforPV(String pvName,
			ConfigService configservice) throws Exception {
		EngineContext_EPICSV4 engineContext = EngineContext_EPICSV4
				.getInstance(configservice);

		ArchiveChannel_EPICSV4 channel = engineContext.getChannelList().get(
				pvName);
		if (channel == null)
			throw new Exception(String.format(" Channel '%s' doesn't exist'",
					pvName));

		return channel.getPVMetrics();
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
			final ConfigService configservice, final Writer writer)
			throws Exception {
		EngineContext_EPICSV4 engineContext = EngineContext_EPICSV4
				.getInstance(configservice);
		ArchiveChannel_EPICSV4 channel = engineContext.getChannelList().get(
				pvName);

		if (channel == null)
			throw new Exception(String.format(" Channel '%s' doesn't exist'",
					pvName));
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
				ArchiveEngine_EPICSV4.archivePV(pvName, samplingPeriod,
						SamplingMethod.SCAN,
						(int) engineContext.getWritePeriod(), writer,
						configservice, pvMetrics.getArchDBRTypes());

			} else {
				// mode is not changed the mode is still scan

				// the new sample period is the same with the old sample period

				double perioddelt = Math.abs(samplePeriodOld - samplingPeriod);
				if (perioddelt < 0.1) {
					// the same sample period
					// do nothing

				} else {
					// different period

					engineContext.getScheduler().remove(
							(ScannedArchiveChannel_EPICSV4) channel);
					engineContext.getScheduler().purge();

					// stop channel and remove id from ChannelList and buffer
					channel.stop();
					engineContext.getWriteThead().removeChannel(pvName);

					// add new channel in scan mode
					ArchiveEngine_EPICSV4.archivePV(pvName, samplingPeriod,
							SamplingMethod.SCAN,
							(int) engineContext.getWritePeriod(), writer,
							configservice, pvMetrics.getArchDBRTypes());

				}

			}

		} else if (mode == SamplingMethod.MONITOR) {

			if (isMonitor) {
				// mode is not changed, is monior

				// remove the channel in monitor mode
				channel.stop();
				engineContext.getWriteThead().removeChannel(pvName);

				// add new channel in monitor mode
				ArchiveEngine_EPICSV4.archivePV(pvName, samplingPeriod,
						SamplingMethod.MONITOR,
						(int) engineContext.getWritePeriod(), writer,
						configservice, pvMetrics.getArchDBRTypes());

			} else {
				// mode is changed from scan to monitor ,new mode is monitor

				engineContext.getScheduler().remove(
						(ScannedArchiveChannel_EPICSV4) channel);
				engineContext.getScheduler().purge();
				channel.stop();
				engineContext.getWriteThead().removeChannel(pvName);

				// add new channel in monitor mode
				ArchiveEngine_EPICSV4.archivePV(pvName, samplingPeriod,
						SamplingMethod.MONITOR,
						(int) engineContext.getWritePeriod(), writer,
						configservice, pvMetrics.getArchDBRTypes());

			}

		}

	}

	public static void destoryPv(String pvName,
			final ConfigService configservice) throws Exception {
		EngineContext_EPICSV4 engineContext = EngineContext_EPICSV4
				.getInstance(configservice);
		ArchiveChannel_EPICSV4 channel = engineContext.getChannelList().get(
				pvName);

		if (channel == null)
			throw new Exception(String.format(" Channel '%s' doesn't exist'",
					pvName));
		PVMetrics pvMetrics = channel.getPVMetrics();

		boolean isMonitor = pvMetrics.isMonitor();

		if (isMonitor) {
			// pv is in monitor mode
			// remove the channel in monitor mode
			channel.stop();
			engineContext.getWriteThead().removeChannel(pvName);

		}

		else {
			// pv is in scan mode
			// remove the channel in scan mode

			engineContext.getScheduler().remove(
					(ScannedArchiveChannel_EPICSV4) channel);
			engineContext.getScheduler().purge();
			channel.stop();
			engineContext.getWriteThead().removeChannel(pvName);

		}
	}

}
