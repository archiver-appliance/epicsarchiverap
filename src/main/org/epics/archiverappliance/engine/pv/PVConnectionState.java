package org.epics.archiverappliance.engine.pv;

enum PVConnectionState {
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