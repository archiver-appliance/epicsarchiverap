package org.epics.archiverappliance.engine.test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.SampleBuffer;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.retrieval.channelarchiver.HashMapEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A small test to test the double buffering algorithm we use. 
 * We reuse the SampleBuffer; but simulate the writer using the same approach that the WriterRunnable uses.
 * @author mshankar
 *
 */
public class DoubleBufferTest {
	private static Logger logger = Logger.getLogger(DoubleBufferTest.class.getName());
	private int eventsAdded = 0;
	private int eventsStored = 0;

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	enum StoredState { ALL_STORED, SOME_LOSS, MAYBE_SOME_LOSS};
	
	@Test
	public void testDoubleBuffering() throws Exception { 
		testSampleBufferDoubleBufferingForBufferSize(7, StoredState.SOME_LOSS);
		testSampleBufferDoubleBufferingForBufferSize(8, StoredState.SOME_LOSS);
		testSampleBufferDoubleBufferingForBufferSize(9, StoredState.SOME_LOSS);
		testSampleBufferDoubleBufferingForBufferSize(10, StoredState.MAYBE_SOME_LOSS);
		testSampleBufferDoubleBufferingForBufferSize(11, StoredState.ALL_STORED);
		testSampleBufferDoubleBufferingForBufferSize(12, StoredState.ALL_STORED);
		testSampleBufferDoubleBufferingForBufferSize(13, StoredState.ALL_STORED);
		testSampleBufferDoubleBufferingForBufferSize(14, StoredState.ALL_STORED);
		testSampleBufferDoubleBufferingForBufferSize(15, StoredState.ALL_STORED);
		testSampleBufferDoubleBufferingForBufferSize(16, StoredState.ALL_STORED);
		testSampleBufferDoubleBufferingForBufferSize(17, StoredState.ALL_STORED);
	}
	
	private void testSampleBufferDoubleBufferingForBufferSize(int bufferSize, StoredState allStored) throws Exception {
		logger.info("Testing for buffer size " + bufferSize);
		this.eventsAdded = 0;
		this.eventsStored = 0;
		SampleBuffer buffer = new SampleBuffer("TestSampleBuffer", bufferSize, ArchDBRTypes.DBR_SCALAR_DOUBLE, new PVMetrics("TestSampleBuffer", null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));
		ScheduledExecutorService dataGen = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "Data generator");
				return t;
			}
		});
		
		dataGen.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					HashMap<String, Object> eventAttrs = new HashMap<String, Object>();
					eventAttrs.put(HashMapEvent.SECS_FIELD_NAME, Long.toString(TimeUtils.getCurrentEpochSeconds()));
					eventAttrs.put(HashMapEvent.NANO_FIELD_NAME, "0");
					eventAttrs.put(HashMapEvent.VALUE_FIELD_NAME, TimeUtils.getCurrentEpochSeconds());
					buffer.add(new HashMapEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, eventAttrs));
					eventsAdded++;
				} catch(Exception ex) { 
					ex.printStackTrace();
				}
			}
		}, 0, 1, TimeUnit.MILLISECONDS);

		ScheduledExecutorService writer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "Writer");
				return t;
			}
		});
		writer.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				buffer.resetSamples();
				ArrayListEventStream previousSamples = buffer.getPreviousSamples();
				try (BasicContext basicContext = new BasicContext()) {
					if (previousSamples.size() > 0) {
						for(@SuppressWarnings("unused") Event event : previousSamples) {
							eventsStored++;
						}
					}
				} catch (IOException e) {
					logger.error("Exception saving data in test", e);
				}
			}
		}, 10, 10, TimeUnit.MILLISECONDS);
		
		Thread.currentThread().sleep(1*1000);
		dataGen.shutdown();
		Thread.currentThread().sleep(1*1000);
		writer.shutdown();
		Thread.currentThread().sleep(1*1000);	
		
		assertTrue("No data generated for event count " + eventsAdded + " and stored events " + eventsStored, eventsAdded> 0 && eventsStored > 0);
		if(allStored == StoredState.ALL_STORED) {
			assertTrue("Generated event count " + eventsAdded + " and stored events " + eventsStored + " are not the same for buffer size " + bufferSize, eventsAdded == eventsStored);
		} else if(allStored == StoredState.SOME_LOSS) { 
			assertTrue("Generated event count " + eventsAdded + " and stored events " + eventsStored + " must have some loss for buffer size " + bufferSize, eventsAdded > eventsStored);			
		}
	}
}
