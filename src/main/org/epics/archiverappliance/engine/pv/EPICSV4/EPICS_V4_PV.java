package org.epics.archiverappliance.engine.pv.EPICSV4;



import gov.aps.jca.dbr.TimeStamp;

import java.lang.reflect.Constructor;
import java.util.Calendar;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.EPICSV42DBRType;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PV;
import org.epics.archiverappliance.engine.pv.PVListener;
import org.epics.ca.client.Channel;
import org.epics.ca.client.Channel.ConnectionState;
import org.epics.ca.client.ChannelGet;
import org.epics.ca.client.ChannelGetRequester;
import org.epics.ca.client.ChannelRequester;
import org.epics.ca.client.CreateRequestFactory;
import org.epics.pvData.misc.BitSet;
import org.epics.pvData.monitor.Monitor;
import org.epics.pvData.monitor.MonitorElement;
import org.epics.pvData.monitor.MonitorRequester;
import org.epics.pvData.pv.MessageType;
import org.epics.pvData.pv.PVDouble;
import org.epics.pvData.pv.PVInt;
import org.epics.pvData.pv.PVLong;
import org.epics.pvData.pv.PVStructure;
import org.epics.pvData.pv.Status;
import org.epics.pvData.pv.Structure;



public class EPICS_V4_PV implements PV, ChannelRequester, MonitorRequester {



	final private String name;

	private boolean monitorIsDestroyed = false;

	private boolean running = false;

	private boolean connected = false;

	/** PVListeners of this PV */

	final private CopyOnWriteArrayList<PVListener> listeners = new CopyOnWriteArrayList<PVListener>();

	private Calendar cal = Calendar.getInstance();

	private Constructor<? extends DBRTimeEvent> con;



	private static final Logger logger = Logger.getLogger(EPICS_V4_PV.class.getName());

	private State state = State.Idle;

	private RefCountedChannel_EPICSV4 channel_ref = null;

	private long connectionFirstEstablishedEpochSeconds;

	private long connectionLastRestablishedEpochSeconds;

	private long connectionLossRegainCount;

	private boolean firstTimeConnection = true;

	private long connectionEstablishedEpochSeconds;

	private long connectionRequestMadeEpochSeconds;

	private DBRTimeEvent dbrtimeevent;

	private ConfigService configservice;



	private ArchDBRTypes archDBRTypes = null;

	/**

	 * Either <code>null</code>, or the subscription identifier. LOCK

	 * <code>this</code> on change

	 */

	private Monitor subscription = null;

	private MetaInfo_EPICSV4 totalMetaInfo = new MetaInfo_EPICSV4();



	private enum State {

		/** Nothing happened, yet */

		Idle,

		/** Trying to connect */

		Connecting,

		/** Got basic connection */

		Connected,

		/** Requested MetaData */

		GettingMetadata,

		/** Received MetaData */

		GotMetaData,

		/** Subscribing to receive value updates */

		Subscribing,

		/**

		 * Received Value Updates

		 * <p>

		 * This is the ultimate state!

		 */

		GotMonitor,

		/** Got disconnected */

		Disconnected

	}



	@Override

	public String getHostName(){

		return null;

	}

	

	/**

	 * Generate an EPICS PV.

	 * 

	 * @param name

	 *            The PV name.

	 * @param plain

	 *            When <code>true</code>, only the plain value is requested. No

	 *            time etc. Some PVs only work in plain mode, example:

	 *            "record.RTYP".

	 */

	public EPICS_V4_PV(final String name, ConfigService configservice) {

		this.name = name;



		this.configservice = configservice;

		// Activator.getLogger().finer(name + " created as EPICS_V3_PV");

	}



	private ChannelGetRequester getMetaListener = new ChannelGetRequester() {



		// private ChannelGet channelGet;

		private PVStructure pvstructure;



		// private BitSet bitset;

		@Override

		public String getRequesterName() {



			return this.getClass().getSimpleName() + "[getMeataListener]";

		}



		@Override

		public void message(String message, MessageType messageType) {



			System.err.println("[" + messageType + "]" + message);

		}



		@Override

		public void channelGetConnect(final Status status,

				final ChannelGet channelGet, final PVStructure pvstructure2,

				final BitSet bitset) {



			PVContext_EPIVCSV4.scheduleCommand(new Runnable() {



				@Override

				public void run() {

					if (status.isSuccess()) {

						// this.channelGet=channelGet;

						pvstructure = pvstructure2;

						// this.bitset=bitset;

						channelGet.get(true);



					} else {

						System.err.println(status.getMessage());



					}

				}

			});



		}



		@Override

		public void getDone(final Status status) {

			PVContext_EPIVCSV4.scheduleCommand(new Runnable() {



				@Override

				public void run() {

					if (status.isSuccess()) {

						System.out.println("get meta info");

						// System.out.println(pvstructure);

						totalMetaInfo.applyBasicalInfo(pvstructure);

					}

				}

			});



		}



	};



	public EPICS_V4_PV(String name) {

		this.name = name;

	}



	@Override

	public String getName() {



		return name;

	}



	/** {@inheritDoc} */

	@Override

	public void addListener(final PVListener listener) {

		listeners.add(listener);

		if (running && isConnected())

			listener.pvValueUpdate(this);

	}



	/** {@inheritDoc} */

	@Override

	public void removeListener(final PVListener listener) {

		listeners.remove(listener);

	}



	@Override

	public void start() throws Exception {



		if (running) {

			return;

		}



		running = true;



		this.connect();

	}



	@Override

	public boolean isRunning() {



		return running;

	}



	@Override

	public boolean isConnected() {



		return connected;

	}

	@Override
	public boolean hasSearchBeenIssued() {
		if(connected) {
			return true;
		} else { 
			if(this.state == State.Connecting) { 
				return true;
			} else { 
				return false;
			}
		}
	}


	@Override

	public boolean isWriteAllowed() {



		return connected;

	}



	@Override

	public String getStateInfo() {



		return state.toString();

	}



	@Override

	public void stop() {



		running = false;



		PVContext_EPIVCSV4.scheduleCommand(new Runnable() {



			@Override

			public void run() {

				unsubscribe();

				disconnect();

			}

		});

	}



	private void disconnect() {

		// Releasing the _last_ channel will close the context,

		// which waits for the JCA Command thread to exit.

		// If a connection or update for the channel happens at that time,

		// the JCA command thread will send notifications to this PV,

		// which had resulted in dead lock:

		// This code locked the PV, then tried to join the JCA Command thread.

		// JCA Command thread tried to lock the PV, so it could not exit.

		// --> Don't lock while calling into the PVContext.

		RefCountedChannel_EPICSV4 channel_ref_copy;

		synchronized (this) {

			// Never attempted a connection?

			if (channel_ref == null)

				return;

			channel_ref_copy = channel_ref;

			channel_ref = null;

			connected = false;

		}

		try {



			PVContext_EPIVCSV4.releaseChannel(channel_ref_copy);

		} catch (final Throwable e) {

			logger.error("exception when disconnecting pv", e);

		}

		fireDisconnected();

	}



	@Override

	public DBRTimeEvent getValue() {

		return this.dbrtimeevent;

	}



	@Override

	public void setValue(Object new_value) throws Exception {



	}



	@Override

	public DBRTimeEvent getDBRTimeEvent() {



		return this.dbrtimeevent;

	}



	@Override

	public ArchDBRTypes getArchDBRTypes() {



		return archDBRTypes;

	}



	@Override

	public long getConnectionFirstEstablishedEpochSeconds() {



		return this.connectionFirstEstablishedEpochSeconds;

	}



	@Override

	public long getConnectionLastRestablishedEpochSeconds() {



		return this.connectionLastRestablishedEpochSeconds;

	}



	@Override

	public long getConnectionLossRegainCount() {



		return this.connectionLossRegainCount;

	}



	@Override

	public long getConnectionEstablishedEpochSeconds() {



		return this.connectionEstablishedEpochSeconds;

	}



	@Override

	public long getConnectionRequestMadeEpochSeconds() {



		return this.connectionRequestMadeEpochSeconds;

	}



	@Override

	public String getRequesterName() {



		return this.getClass().getName() + "   channelName:" + this.name;

	}



	@Override

	public void message(String message, MessageType messageType) {



		System.err.println("[" + messageType + "] " + message);

	}



	@Override

	public void channelCreated(Status arg0, Channel arg1) {



	}



	@Override

	public void channelStateChange(final Channel channel,

			final ConnectionState connectionStatus) {



		// channel_ref.setChannel(channel);

		PVContext_EPIVCSV4.scheduleCommand(new Runnable() {



			@Override

			public void run() {

				if (connectionStatus == ConnectionState.CONNECTED)



				{



					System.out.println("channelStateChange:connected");



					// createMonitor(channel);

					handleConnected(channel);



				} else if (connectionStatus == ConnectionState.DISCONNECTED) {

					System.out.println("channelStateChange:disconnected");

					// stopMonitor();

					state = State.Disconnected;

					connected = false;



					unsubscribe();

					fireDisconnected();



				}



			}



		});



	}



	/*

	 * private void createMonitor(Channel channel) { //

	 * channelMonitorRequester=new MonitorImp(channel); PVStructure pvRequest =

	 * CreateRequestFactory.createRequest("record[queueSize=" + 2 + "]" +

	 * "field(timeStamp,value,scan,alarm,arrayStructureArray,structureArray,arrayArray)"

	 * ,this); // //arrayStructureArray channel.createMonitor(this, pvRequest);

	 * }

	 */



	@Override

	public void monitorConnect(Status status, Monitor channelMonitor,

			Structure structure) {



		if (monitorIsDestroyed)

			return;

		synchronized (this) {

			// this.channelMonitor = channelMonitor;

			if (status.isSuccess()) {

				// connected = new Boolean(status.isOK());

				channelMonitor.start();

				this.notify();

			}

			if (status.isSuccess())

				System.out.println("monitorConnect:" + "connect successfully");

			else {

				System.out.println("monitorConnect:" + "connect failed");

			}



			if (channelMonitor == null) {

				System.out.println("channelMonitor is null");

			}



		}

	}



	@Override

	public void unlisten(Monitor monitor) {



		monitor.stop();

		monitor.destroy();

	}



	/*

	 * private void stopMonitor() { channelMonitor.stop();

	 * channelMonitor.destroy(); monitorIsDestroyed = true; }

	 */



	private void connect() {

		logger.info("pv connectting");

		PVContext_EPIVCSV4.scheduleCommand(new Runnable() {



			@Override

			public void run() {

				//

				try {

					state = State.Connecting;

					// Already attempted a connection?

					synchronized (this) {

						if (channel_ref == null) {



							channel_ref = PVContext_EPIVCSV4.getChannel(name,

									EPICS_V4_PV.this);



						}

						connectionRequestMadeEpochSeconds = System

								.currentTimeMillis() / 1000;

						if (channel_ref.getChannel() == null)

							return;



						if (channel_ref.getChannel().getConnectionState() == ConnectionState.CONNECTED) {

							// System.out.println("connect successful");

							// Activator.getLogger().log(Level.FINEST,

							// "{0} is immediately connected", name);

							handleConnected(channel_ref.getChannel());

						} else {

							// System.out.println("connect unsuccessful");

						}

					}



				} catch (Exception e) {

					logger.error("exception when connecting pv", e);

				}

			}



		});



	}



	/**

	 * PV is connected. Get meta info, or subscribe right away.

	 */

	private void handleConnected(final Channel channel) {

		// Activator.getLogger().log(Level.FINEST, "{0} connected ({1})", new

		// Object[] { name, state.name() });

		if (state == State.Connected)

			return;

		state = State.Connected;



		connectionEstablishedEpochSeconds = System.currentTimeMillis() / 1000;



		if (firstTimeConnection) {

			this.connectionFirstEstablishedEpochSeconds = System

					.currentTimeMillis() / 1000;



			firstTimeConnection = false;

		} else {

			this.connectionLastRestablishedEpochSeconds = System

					.currentTimeMillis() / 1000;

			this.connectionLossRegainCount = this.connectionLossRegainCount + 1;

		}

		for (final PVListener listener : listeners) {

			listener.pvConnected(this);

		}



		// If we're "running", we need to get the meta data and

		// then subscribe.

		// Otherwise, we're done.

		if (!running) {

			connected = true;

			// meta = null;

			synchronized (this) {

				this.notifyAll();

			}

			return;

		}

		// else: running, get meta data, then subscribe



		PVStructure pvRequest = CreateRequestFactory.createRequest(

				"record[queueSize=" + 1 + "]"

						+ "field(timeStamp,value,scan,alarm)", this);

		channel.createChannelGet(getMetaListener, pvRequest);



		subscribe();

	}



	/** Subscribe for value updates. */

	private void subscribe() {



		synchronized (this) {

			// Prevent multiple subscriptions.

			if (subscription != null) {

				return;

			}

			// Late callback, channel already closed?

			final RefCountedChannel_EPICSV4 ch_ref = channel_ref;

			if (ch_ref == null) {

				return;

			}

			final Channel channel = ch_ref.getChannel();

			// final Logger logger = Activator.getLogger();

			try {



				// final DBRType type = DBR_Helper.getTimeType(plain,

				// channel.getFieldType());

				//

				state = State.Subscribing;

				/*

				 * subscription = channel.addMonitor(type,

				 * channel.getElementCount(), mask.getMask(), this);

				 */

				// startTime=System.currentTimeMillis();

				totalMetaInfo.setStartTime(System.currentTimeMillis());



				// System.out.println("subscribe");

				// subscription = channel.addMonitor(type,

				// channel.getElementCount(),

				// 1, this);

				PVStructure pvRequest = CreateRequestFactory.createRequest(

						"record[queueSize=" + 1 + "]"

								+ "field(timeStamp,value,scan,alarm)", this);

				subscription = channel.createMonitor(this, pvRequest);



			} catch (final Exception ex) {

				// logger.log(Level.SEVERE, name + " subscribe error", ex);

				logger.error("exception when subscribing pv", ex);

			}

		}

	}



	/** Unsubscribe from value updates. */

	private void unsubscribe() {

		Monitor sub_copy;

		// Atomic access

		synchronized (this) {

			sub_copy = subscription;

			subscription = null;

		}

		if (sub_copy == null) {

			return;

		}

		try {

			// sub_copy.clear();

			sub_copy.stop();

			sub_copy.destroy();

		} catch (final Exception ex) {

			// Activator.getLogger().log(Level.SEVERE, name +

			// " unsubscribe error", ex);

			logger.error("exception when unsubscribing pv", ex);

		}

	}



	/** Notify all listeners. */

	private void fireDisconnected() {

		for (final PVListener listener : listeners) {

			listener.pvDisconnected(this);

		}

	}



	/** Notify all listeners. */

	private void fireValueUpdate() {

		for (final PVListener listener : listeners) {

			listener.pvValueUpdate(this);

		}

	}



	@Override

	public void monitorEvent(Monitor monitor) {



		MonitorElement monitorElement = null;



		try {



			// monitor.release(monitorElement);

			// System.out.println(monitorElement.getPVStructure());



			if (monitorIsDestroyed)

				return;

			if (!running) {

				// log.finer(name + " monitor while not running (" +

				// state.name() + ")");

				return;

			}



			if (subscription == null) {

				// log.finer(name + " monitor while not subscribed (" +

				// state.name() + ")");

				return;

			}

			state = State.GotMonitor;

			monitorElement = monitor.poll();

			if (monitorElement == null)

				return; // no monitors are present



			// System.out.println("monitor event");

			// System.out.println(monitorElement.getPVStructure());

			PVStructure totalPVStructure = monitorElement.getPVStructure();

			PVStructure valuePVStructure = totalPVStructure

					.getStructureField("value");

			PVDouble phasePVDouble = valuePVStructure.getDoubleField("phase");

			PVDouble amplitudePVDouble = valuePVStructure

					.getDoubleField("amplitude");

			double phase = phasePVDouble.get();

			double amplitude = amplitudePVDouble.get();

			PVStructure alarmPVStructure = totalPVStructure

					.getStructureField("alarm");

			PVInt severrityPVInt = alarmPVStructure.getIntField("severity");

			int serverity = severrityPVInt.get();



			PVInt statusPVInt = alarmPVStructure.getIntField("status");

			int status = statusPVInt.get();

			PVStructure timeStampPVStructure = totalPVStructure

					.getStructureField("timeStamp");

			// timeStamp

			PVLong secondsPastEpochPVLong = timeStampPVStructure

					.getLongField("secondsPastEpoch");

			long secondsPastEpoch = secondsPastEpochPVLong.get();



			PVInt nanoSecondsPVInt = timeStampPVStructure

					.getIntField("nanoSeconds");

			int nanoSeconds = nanoSecondsPVInt.get();



			// StructrueParser_EPICSV4.parseStructure(totalPVStructure);



			cal.setTimeInMillis(secondsPastEpoch * 1000 + nanoSeconds / 1000000);

			cal.add(Calendar.YEAR, -20);

			TimeStamp timestamp = new TimeStamp(cal.getTimeInMillis() / 1000,

					nanoSeconds);



			String allvalue = "" + phase + "|" + amplitude;



			Data_EPICSV4 dataepicsv4 = new Data_EPICSV4(timestamp, serverity,

					status, allvalue.getBytes());



			try {

				if (archDBRTypes == null) {

					archDBRTypes = EPICSV42DBRType

							.valueOf(DataType_EPICSV4.TIME_VSTATIC_BYTES);

					con = configservice.getArchiverTypeSystem()

							.getV4Constructor(archDBRTypes);

				}



				dbrtimeevent = con.newInstance(dataepicsv4);



			} catch (Exception e) {

				logger.error(

						"exception in monitor changed function when converting DBR to dbrtimeevent",

						e);

			}



			totalMetaInfo.computeRate(dataepicsv4, System.currentTimeMillis());

			if (!connected)

				connected = true;

			fireValueUpdate();

		} catch (final Exception ex) {

			// log.log(Level.WARNING, name + " monitor value error", ex);

			logger.error("exception in monitor changed ", ex);

		}



		finally {

			monitor.release(monitorElement);

		}



	}



	@Override

	public void addControledPV(String pvName) {



	}



	@Override

	public void setParentChannel(ArchiveChannel channel) {

	}
	
	@Override
	public void setParentChannel(ArchiveChannel channel, boolean isRuntimeOnly) {
		
	}





	@Override

	public void updataMetaField(String pvName, String fieldValue) {



	}



	@Override

	public void setHasMetaField(boolean hasMetaField) {

	}



	@Override
	public HashMap<String, String> getCurrentCopyOfMetaFields() {
		return null;
	}



}
