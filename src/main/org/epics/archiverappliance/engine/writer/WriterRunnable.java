
/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/

package org.epics.archiverappliance.engine.writer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.model.SampleBuffer;
import org.epics.archiverappliance.engine.model.YearListener;

/**
 * WriterRunnable is scheduled by the executor in the engine context every writing period.
 * @author Luofeng Li
 *
 */
public class WriterRunnable implements Runnable {
	private static final Logger logger = LogManager.getLogger(WriterRunnable.class);
	/** Minimum write period [seconds] */
	private static final double MIN_WRITE_PERIOD = 10.0;
    /**the sample buffer hash map*/
	final private ConcurrentHashMap<String, SampleBuffer> buffers = new ConcurrentHashMap<String, SampleBuffer>();

	/**the configservice used by this WriterRunnable*/
	private ConfigService configservice = null;
	/**is running?*/
	private boolean isRunning=false;
/**
 * the constructor
 * @param configservice the configservice used by this WriterRunnable
 */
	public WriterRunnable(ConfigService configservice) {

		this.configservice = configservice;
	}

	/** Add a channel's buffer that this thread reads 
	 * @param channel ArchiveChannel
	 */
	public void addChannel(final ArchiveChannel channel) {
		addSampleBuffer(channel.getName(), channel.getSampleBuffer());
	}
/**
 * remove one sample buffer from the buffer hash map.
 * At the same time. it also removes the channel from the channel hash map in the engine context
 * @param channelName the name of the channel who and whose sample buffer are removed
 */
	public void removeChannel(final String channelName) {
		buffers.remove(channelName);
	}

	/**
	 * add sample buffer into this writer runnable and add year listener to each sample buffer
	 * @param name the name of the channel
	 * @param buffer the sample buffer for this channel
	 */
	void addSampleBuffer(final String name, final SampleBuffer buffer) {
		// buffers.add(buffer);
		buffers.put(name, buffer);
		buffer.addYearListener(new YearListener() {

			@Override
			public void yearChanged(final SampleBuffer sampleBuffer) {
				//
				configservice.getEngineContext().getScheduler().execute(new Runnable(){

					@Override
					public void run() {
						try {
							write(sampleBuffer);
							logger.info(sampleBuffer.getChannelName() + ":year change");
						} catch (IOException e) {
							logger.error("Exception", e);
						}
						
					}
					
				});
				
			}

		});
	}

/**
 * set the writing period. when the writing period is at least 10 seonds.
 * When write_period &lt; 10 , the writing period is 10 seconds actually.
 * @param write_period  the writing period in second
 * @return the actual writing period in second
 */
	public double setWritingPeriod(double write_period) {
		double tempwrite_period=write_period;
		if (tempwrite_period < MIN_WRITE_PERIOD) {
		
			tempwrite_period = MIN_WRITE_PERIOD;
		}
		return tempwrite_period;
		
	}
	

  
	@Override
	public void run() {
		try {
			// final long written = write();
			long startTime = System.currentTimeMillis();
			write();
			long endTime = System.currentTimeMillis();
			configservice.getEngineContext().setSecondsConsumedByWriter(
					(double) (endTime - startTime) / 1000);
		} catch (Exception e) {
			logger.error("Exception", e);
		}

	}
   /**
    * write the sample buffer to the short term storage
    * @param buffer the sample buffer to be written
    * @throws IOException  error occurs during writing the sample buffer to the short term storage
    */
	private void write(SampleBuffer buffer) throws IOException {
		if(isRunning) return;
		isRunning=true;
		ConcurrentHashMap<String, ArchiveChannel> channelList = configservice
				.getEngineContext().getChannelList();
		String channelNname = buffer.getChannelName();
		buffer.resetSamples();
		ArrayListEventStream previousSamples = buffer.getPreviousSamples();
		

		try (BasicContext basicContext = new BasicContext()) {

			if (previousSamples.size() > 0) {
				ArchiveChannel tempChannel = channelList.get(channelNname);
				tempChannel.setlastRotateLogsEpochSeconds(System
						.currentTimeMillis() / 1000);
				tempChannel.getWriter().appendData(basicContext, channelNname,
						previousSamples);
			}
		} catch (IOException e) {
			throw (e);
		}
		finally{
			isRunning=false;
		}
		isRunning=false;
	}
/**
 * write all sample buffers into short term storage
 * @throws Exception error occurs during writing the sample buffer to the short term storage
 */
	private void write() throws Exception {
		if(isRunning) return;
		isRunning=true;
		ConcurrentHashMap<String, ArchiveChannel> channelList = configservice
				.getEngineContext().getChannelList();
		

		Iterator<Entry<String, SampleBuffer>> it = buffers.entrySet()
				.iterator();

		
		while (it.hasNext())
		
		{
		
			Entry<String, SampleBuffer> entry = (Entry<String, SampleBuffer>) it
					.next();
			SampleBuffer buffer = entry.getValue();
		
			String channelNname = buffer.getChannelName();
			
			buffer.resetSamples();
			ArrayListEventStream previousSamples = buffer.getPreviousSamples();
			try (BasicContext basicContext = new BasicContext()) {
				if (previousSamples.size() > 0)

				{
					ArchiveChannel tempChannel = channelList.get(channelNname);
					tempChannel.aboutToWriteBuffer((DBRTimeEvent)previousSamples.get(previousSamples.size() -1));
					tempChannel.setlastRotateLogsEpochSeconds(System
							.currentTimeMillis() / 1000);
					tempChannel.getWriter().appendData(basicContext,
							channelNname, previousSamples);
				}
			} catch (IOException e) {
				throw (e);
			}finally{
				isRunning=false;
			}

		}
		
		isRunning=false;
		
		


	}
	/**
	 * flush out the sample buffer to the short term storage before shutting down the engine
	 * @throws Exception  error occurs during writing the sample buffer to the short term storage
	 */
	public void flushBuffer() throws Exception {
		
			write();
	}

}
