package org.epics.archiverappliance.engine.pv.EPICSV4;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.config.ConfigService;

public class EngineContext_EPICSV4 {

	// private static EngineContext engineContext=null;
	/** writing thread to write samplebuffer to protocol buffer */
	final private WriterRunnable_EPICSV4 writer;
	private static EngineContext_EPICSV4 engineContext = null;
	private boolean isWriteThreadStarted = false;
	private ScheduledThreadPoolExecutor scheduler = null;
	private double write_period;
	/** Thread that runs the scanner */
	// private final ScanThread scan_thread ;
	private final ConcurrentHashMap<String, ArchiveChannel_EPICSV4> channelList;
	private static final Logger logger = Logger.getLogger(EngineContext_EPICSV4.class.getName());

	public static EngineContext_EPICSV4 getInstance(
			final ConfigService configservice) {
		if (engineContext == null)
			engineContext = new EngineContext_EPICSV4(configservice);
		return engineContext;
	}

	private EngineContext_EPICSV4(final ConfigService configservice) {

		// scan_thread= new ScanThread(scanner);
		writer = new WriterRunnable_EPICSV4(configservice);
		// scheduler=(ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1);
		channelList = new ConcurrentHashMap<String, ArchiveChannel_EPICSV4>();

		configservice.addShutdownHook(new Runnable() {

			@Override
			public void run() {

				logger.info("the archive engine will shutdown");
				try {

					if (scheduler != null) {
						scheduler.shutdown();
					}
					Iterator<Entry<String, ArchiveChannel_EPICSV4>> itChannel = channelList
							.entrySet().iterator();

					while (itChannel.hasNext()) {
						Entry<String, ArchiveChannel_EPICSV4> channelentry = (Entry<String, ArchiveChannel_EPICSV4>) itChannel
								.next();
						ArchiveChannel_EPICSV4 channeltemp = channelentry
								.getValue();

						channeltemp.stop();
					}

					channelList.clear();

					scheduler = null;
					isWriteThreadStarted = false;
					PVContext_EPIVCSV4.destoryChannelAccess();

				} catch (Exception e) {

					logger.error(
							"Exception when execuing ShutdownHook inconfigservice",
							e);
				}

				logger.info("the archive engine has been shutdown");

			}

		});
	}

	public ConcurrentHashMap<String, ArchiveChannel_EPICSV4> getChannelList() {
		return channelList;
	}

	public void setScheduler(ScheduledThreadPoolExecutor newscheduler) {
		if (scheduler == null)
			scheduler = newscheduler;
		// else throw new
		// Exception("scheduler has been initialized and you cannot initialize it again!");
		else
			logger.error("scheduler has been initialized and you cannot initialize it again!");
	}

	/*
	 * static public EngineContext getInstance() { if(engineContext==null)
	 * engineContext=new EngineContext(); return engineContext; }
	 */

	public ScheduledThreadPoolExecutor getScheduler() {
		if (scheduler == null)
			scheduler = (ScheduledThreadPoolExecutor) Executors
					.newScheduledThreadPool(1);
		return scheduler;

	}

	public WriterRunnable_EPICSV4 getWriteThead() {
		return writer;

	}

	/*
	 * public Scanner getScanner() { return scanner; }
	 */
	/*
	 * public ScanThread getScanThread() { return scan_thread; }
	 */

	/*
	 * public void startScanThread() { //scan_thread.start();
	 * 
	 * 
	 * isScanThreadStarted=true; }
	 */

	public void startWriteThread(double write_period, Writer write,
			ConfigService configservice) {
		writer.start(write_period, write);
		this.write_period = write_period;
		if (scheduler == null)
			scheduler = (ScheduledThreadPoolExecutor) Executors
					.newScheduledThreadPool(1);
		scheduler.scheduleAtFixedRate(writer, 0, (long) (write_period * 1000),
				TimeUnit.MILLISECONDS);

		isWriteThreadStarted = true;
	}

	public double getWritePeriod() {
		return write_period;
	}

	public boolean isWriteThreadStarted() {

		return isWriteThreadStarted;
	}

	/*
	 * public boolean isScanThreadStarted() {
	 * 
	 * return isScanThreadStarted; }
	 */

}
