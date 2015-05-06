package org.epics.archiverappliance.engine.pv.EPICSV4;

import java.util.LinkedList;

public class CommandThread_EPICSV4 extends Thread {

	/**
	 * Delay between queue inspection. Longer delay results in bigger 'batches',
	 * which is probably good, but also increases the latency.
	 */
	final private static long DELAY_MILLIS = 100;
	/**
	 * Command queue.
	 * <p>
	 * SYNC on access
	 */
	final private LinkedList<Runnable> command_queue = new LinkedList<Runnable>();

	/** Maximum size that command_queue reached at runtime */
	private int max_size_reached = 0;

	/** Flag to tell thread to run or quit */
	private boolean run = false;

	public CommandThread_EPICSV4() {
		super("CommandThread_EPICSV4");
	}

	/**
	 * Version of <code>start</code> that may be called multiple times.
	 * <p>
	 * The thread must only be started after the first PV has been created.
	 * Otherwise, if flush is called without PVs, JNI JCA reports pthread
	 * errors.
	 * <p>
	 * NOP when already running
	 */
	@Override
	public synchronized void start() {
		if (run)
			return;
		run = true;
		super.start();
	}

	/** Stop the thread and wait for it to finish */
	void shutdown() {
		run = false;

		// Activator.getLogger().log(Level.FINE,
		// "JCACommandThread queue reached up to {0} entries",
		// max_size_reached);
	}

	/**
	 * Add a command to the queue.
	 * 
	 * @param command
	 */
	public void addCommand(final Runnable command) {
		synchronized (command_queue) {
			// New maximum queue length (+1 for the one about to get added)
			if (command_queue.size() >= max_size_reached)
				max_size_reached = command_queue.size() + 1;
			command_queue.addLast(command);
		}
	}

	/** @return Oldest queued command or <code>null</code> */
	private Runnable getCommand() {
		synchronized (command_queue) {
			if (command_queue.size() > 0)
				return command_queue.removeFirst();
		}
		return null;
	}

	@Override
	public void run() {
		while (run) {
			// Execute all the commands currently queued...
			Runnable command = getCommand();
			while (command != null) { // Execute one command
				try {
					command.run();
				} catch (Throwable ex) {
					// Activator.getLogger().log(Level.WARNING,
					// "JCACommandThread exception", ex);
				}
				// Get next command
				command = getCommand();
			}

			// Then wait.
			try {
				Thread.sleep(DELAY_MILLIS);
			} catch (InterruptedException ex) { /* don't even ignore */
			}
		}
	}
}
