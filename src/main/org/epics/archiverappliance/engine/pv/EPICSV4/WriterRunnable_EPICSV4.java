/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv.EPICSV4;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.SampleBuffer;
import org.epics.archiverappliance.engine.model.YearListener;
import org.epics.archiverappliance.engine.util.Average;

/**
 * Thread that writes values from multiple <code>SampleBuffer</code>s to an
 * <code>RDBArchiveServer</code>.
 * <p>
 * When there are write errors, it sets the sample buffer error state and tries
 * to reconnect to the database and write again until successful. Since the
 * Oracle batch mechanism doesn't tell us what exactly failed in a batch, all
 * the samples that were part of the batch might be lost. The channels that add
 * samples to the sample buffer supposedly notice the error condition and add a
 * special indicator once we recover.
 * 
 * @author Kay Kasemir
 */
public class WriterRunnable_EPICSV4 implements Runnable {
	private static final Logger logger = Logger.getLogger(WriterRunnable_EPICSV4.class);
	/** Minimum write period [seconds] */
	private static final double MIN_WRITE_PERIOD = 5.0;

	final private ConcurrentHashMap<String, SampleBuffer> buffers = new ConcurrentHashMap<String, SampleBuffer>();

	/**
	 * Synchronization block for waiting. Signaled in stop().
	 */
	private Object wait_block = new Object();

	/** Number of values to place into one batch */
	// private int batch_size = 500;

	/** Time of end of last write run */
	// private ITimestamp last_write_stamp = null;

	/** Average number of values per write run */
	private Average write_count = new Average();

	/** Average duration of write run */
	private Average write_time = new Average();

	/** Thread the executes this.run() */
	// private Thread thread;

	private Writer writeStream;
	private ConfigService configservice = null;

	public WriterRunnable_EPICSV4(ConfigService configservice) {

		this.configservice = configservice;
	}

	/** Add a channel's buffer that this thread reads */
	public void addChannel(final ArchiveChannel_EPICSV4 channel) {

		EngineContext_EPICSV4.getInstance(configservice).getChannelList()
				.put(channel.getName(), channel);
		addSampleBuffer(channel.getName(), channel.getSampleBuffer());

	}

	public void removeChannel(final String ChannelName) {
		buffers.remove(ChannelName);
		EngineContext_EPICSV4.getInstance(configservice).getChannelList()
				.remove(ChannelName);
	}

	/** Add a sample buffer that this thread reads */
	void addSampleBuffer(final String name, final SampleBuffer buffer) {
		// buffers.add(buffer);
		buffers.put(name, buffer);
		buffer.addYearListener(new YearListener() {

			@Override
			public void yearChanged(SampleBuffer sampleBuffer) {
				//
				try {
					write(sampleBuffer);
					logger.info(sampleBuffer.getChannelName() + ":year change");
				} catch (IOException e) {
					logger.error("exception in yearChanged", e);
				}
			}

		});
	}

	/**
	 * Start the write thread.
	 * 
	 * @param write_period
	 *            Period between writes in seconds
	 * @param batch_size
	 *            Number of values to batch
	 */
	@SuppressWarnings("nls")
	public void start(double write_period, Writer writeStream) {
		if (write_period < MIN_WRITE_PERIOD) {
			// Activator.getLogger().log(Level.INFO,
			// "Adjusting write period from {0} to {1}",
			// new Object[] { write_period, MIN_WRITE_PERIOD });
			write_period = MIN_WRITE_PERIOD;
		}

		// this.batch_size = batch_size;
		// thread = new Thread(this, "WriteThread");
		// thread.start();
		this.writeStream = writeStream;

	}

	/** Reset statistics */
	public void reset() {
		write_count.reset();
		write_time.reset();
	}

	/** Ask the write thread to stop ASAP. */
	private void stop() {

		synchronized (wait_block) {
			wait_block.notify();
		}
	}

	/** @return Timestamp of end of last write run */
	/*
	 * public ITimestamp getLastWriteTime() { return last_write_stamp; }
	 */
	/** @return Average number of values per write run */
	public double getWriteCount() {
		return write_count.get();
	}

	/** @return Average duration of write run in seconds */
	public double getWriteDuration() {
		return write_time.get();
	}

	/**
	 * 'Main loop' of the write thread.
	 * <p>
	 * Writes all values out, then waits. The idea is that waiting a little for
	 * values to accumulate actually helps, because then we may write a few
	 * values per channel, so the effort for locating a channel's ID and the
	 * batching actually pays off.
	 * <p>
	 * Since the wait time can be considerable (30 seconds?), we wait on a
	 * semaphore (wait_block), which can be notified in stop() to cause an ASAP
	 * exit.
	 */
	@Override
	@SuppressWarnings("nls")
	public void run() {
		// System.out.println("run write"+new Date());
		// Activator.getLogger().info("WriteThread starts");
		// final BenchmarkTimer timer = new BenchmarkTimer();

		try {
			// final long written = write();
			write();
		} catch (Exception e) {
			logger.error("exception in run", e);
		}

	}

	private void write(SampleBuffer buffer) throws IOException {
		ConcurrentHashMap<String, ArchiveChannel_EPICSV4> channelList = EngineContext_EPICSV4
				.getInstance(configservice).getChannelList();
		buffer.updateStats();
		String channelNname = buffer.getChannelName();
		// IValue sample=buffer.remove();
		// if("TrainIoc:test4999".equals(channelNname))
		// System.out.println("Channel Name:"+channelNname+"   time:"+new
		// Date()+"   pv number:"+buffers.size());
		// ArrayListEventStream samples =buffer.getSamples();
		// System.out.println("sample size:"+samples.size());
		buffer.resetSamples();
		ArrayListEventStream previousSamples = buffer.getPreviousSamples();
		// System.gc();
		/*
		 * for (int mm=0;mm<samples.size();mm++) { DBRTimeEvent
		 * timeEvent=(DBRTimeEvent)samples.get(mm); //
		 * if("TrainIoc:test4999".equals(channelNname)) //
		 * System.out.println(timeEvent
		 * .getEventTimeStamp()+":"+timeEvent.getSampleValue());
		 * //timeEvent.getSampleValue();
		 * 
		 * 
		 * }
		 */
		// samples.clear();

		if (previousSamples.size() > 0)
			channelList.get(channelNname).setlastRotateLogsEpochSeconds(
					System.currentTimeMillis() / 1000);
		try (BasicContext basicContext = new BasicContext()) {
			writeStream.appendData(basicContext, channelNname, previousSamples);
		} catch (IOException e) {
			throw (e);
		}

	}

	private void write() throws Exception {

		ConcurrentHashMap<String, ArchiveChannel_EPICSV4> channelList = EngineContext_EPICSV4
				.getInstance(configservice).getChannelList(); // System.gc();

		// for (SampleBuffer buffer : buffers)
		// for (int s=0;s<buffers.size();s++)
		// System.out.println("   time:"+new
		// Date()+"   pv number:"+buffers.size());

		Iterator<Entry<String, SampleBuffer>> it = buffers.entrySet()
				.iterator();

		// System.out.println("   time:"+new
		// Date()+"   pv number:"+buffers.size());
		/*
		 * Iterator<Entry<String, ArchiveChannel>> itChannel =
		 * channelList.entrySet().iterator();
		 * 
		 * int totalSucessNum=0; while(itChannel.hasNext()) { Entry<String,
		 * ArchiveChannel> channelentry = (Entry<String,
		 * ArchiveChannel>)itChannel.next(); ArchiveChannel
		 * channeltemp=channelentry.getValue();
		 * 
		 * if(channeltemp.isConnected()) totalSucessNum++; }
		 */
		// Enumeration <String> keys=buffers.keys();
		while (it.hasNext())
		// while(keys.hasMoreElements())
		{
			// SampleBuffer buffer=buffers.get(keys.nextElement());
			Entry<String, SampleBuffer> entry = (Entry<String, SampleBuffer>) it
					.next();
			SampleBuffer buffer = entry.getValue();
			// Update max buffer length etc. before we start to remove samples
			buffer.updateStats();
			String channelNname = buffer.getChannelName();
			// IValue sample=buffer.remove();
			// if("TrainIoc:test4999".equals(channelNname))
			// System.out.println("Channel Name:"+channelNname+"   time:"+new
			// Date()+"   pv number:"+buffers.size()+
			// "totalSucessNum:"+totalSucessNum);
			// ArrayListEventStream samples =buffer.getSamples();
			// System.out.println("sample size:"+samples.size());
			buffer.resetSamples();
			ArrayListEventStream PreviousSamples = buffer.getPreviousSamples();
			// System.gc();
			/*
			 * for (int mm=0;mm<samples.size();mm++) { DBRTimeEvent
			 * timeEvent=(DBRTimeEvent)samples.get(mm); //
			 * if("TrainIoc:test4999".equals(channelNname))
			 * System.out.println(timeEvent
			 * .getEventTimeStamp()+":"+((ScalarStringSampleValue
			 * )(timeEvent.getSampleValue())).toString());
			 * //timeEvent.getSampleValue();
			 * 
			 * 
			 * }
			 */
			// samples.clear();

			if (PreviousSamples.size() > 0)
				channelList.get(channelNname).setlastRotateLogsEpochSeconds(
						System.currentTimeMillis() / 1000);
			try (BasicContext basicContext = new BasicContext()) {
				writeStream.appendData(basicContext, channelNname,
						PreviousSamples);
			} catch (IOException e) {
				throw (e);
			}

		}

	}

	/** Stop the write thread, performing a final write. */
	public void shutdown() throws Exception {
		// Stop the thread
		stop();
		// Wait for it to end
		// thread.join();
		// Then write once more.
		// Errors in this last write are passed up.
		// try
		{
			write();
		}

	}

}
