package org.epics.archiverappliance.engine.pv.EPICSV4;

import java.util.HashMap;

import org.epics.archiverappliance.engine.pv.RefCountedChannel;
import org.epics.ca.client.Channel;
import org.epics.ca.client.ChannelAccess;
import org.epics.ca.client.ChannelAccessFactory;
import org.epics.ca.client.ChannelProvider;
import org.epics.ca.client.ChannelRequester;

public class PVContext_EPIVCSV4 {
	private static String providerName = "pvAccess";
	private static ChannelAccess channelAccess;
	private static ChannelProvider channelProvider;
	private static CommandThread_EPICSV4 commandThread = null;
	/** map of channels. */
	static private HashMap<String, RefCountedChannel_EPICSV4> channels = new HashMap<String, RefCountedChannel_EPICSV4>();
	private static int ca_ref = 0;

	private static void init() {
		if (ca_ref == 0) {
			if (commandThread == null) {
				org.epics.ca.ClientFactory.start();
				commandThread = new CommandThread_EPICSV4();
				commandThread.start();
			}
			if (channelAccess == null) {
				channelAccess = ChannelAccessFactory.getChannelAccess();
				channelProvider = channelAccess.getProvider(providerName);
			}

		}
		++ca_ref;
	}

	/**
	 * Get a new channel, or a reference to an existing one.
	 * 
	 * @param name
	 *            Channel name
	 * @return reference to channel
	 * @throws Exception
	 *             on error
	 * @see #releaseChannel(RefCountedChannel)
	 */
	public synchronized static RefCountedChannel_EPICSV4 getChannel(
			final String name, final ChannelRequester conn_callback)
			throws Exception {

		init();
		RefCountedChannel_EPICSV4 channel_ref = channels.get(name);
		if (channel_ref == null) {
			// Activator.getLogger().log(Level.FINER, "Creating CA channel {0}",
			// name);
			// final Channel channel = channelProvider.createChannel(name,
			// conn_callback);
			final Channel channel = channelProvider.createChannel(name,
					conn_callback, ChannelProvider.PRIORITY_DEFAULT);
			// System.out.println("getChannel");
			if (channel == null)
				throw new Exception("Cannot create channel '" + name + "'");
			channel_ref = new RefCountedChannel_EPICSV4(channel);
			channels.put(name, channel_ref);

		} else {
			channel_ref.incRefs();
			//
			// Must have been getChannel() == null, but how is that possible?
			// channel_ref.getChannel().addConnectionListener(conn_callback);

			// Activator.getLogger().log(Level.FINER, "Re-using CA channel {0}",
			// name);
		}
		return channel_ref;
	}

	public static void exitPVChannelAccess() {
		--ca_ref;
		if (ca_ref > 0)
			return;

		if (commandThread == null)
			return;
		org.epics.ca.ClientFactory.stop();
		commandThread.shutdown();
		commandThread = null;

		// org.epics.ca.ClientFactory.stop();

	}

	/**
	 * Release a channel.
	 * 
	 * @param channel_ref
	 *            Channel to release.
	 * @see #getChannel(String)
	 */
	synchronized static void releaseChannel(
			final RefCountedChannel_EPICSV4 channel_ref) {
		final String name = channel_ref.getChannel().getChannelName();

		if (channel_ref.decRefs() <= 0) {
			// Activator.getLogger().finer("Deleting CA channel " + name);
			channels.remove(name);
			channel_ref.dispose();
		}

		// Activator.getLogger().finer("CA channel " + name + " still ref'ed");
		exitPVChannelAccess();
	}

	/**
	 * Add a command to the JCACommandThread.
	 * <p>
	 * 
	 * @param command
	 *            Command to schedule.
	 * @throws NullPointerException
	 *             when JCA not active
	 */
	public static void scheduleCommand(final Runnable command) {
		// Debug: Run immediately
		// command.run();
		if (commandThread == null) {
			org.epics.ca.ClientFactory.start();
			commandThread = new CommandThread_EPICSV4();
			commandThread.start();
		}
		commandThread.addCommand(command);
	}

	/**
	 * Helper for unit test.
	 * 
	 * @return <code>true</code> if all has been release.
	 */
	static boolean allReleased() {
		return ca_ref == 0;
	}

	public static void destoryChannelAccess() throws InterruptedException {
		if (commandThread == null)
			return;
		org.epics.ca.ClientFactory.stop();
		commandThread.shutdown();
		commandThread = null;
	}
}
