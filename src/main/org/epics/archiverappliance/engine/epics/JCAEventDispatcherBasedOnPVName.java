package org.epics.archiverappliance.engine.epics;

import gov.aps.jca.Channel;
import gov.aps.jca.event.AbstractEventDispatcher;
import gov.aps.jca.event.AccessRightsEvent;
import gov.aps.jca.event.AccessRightsListener;
import gov.aps.jca.event.ConnectionEvent;
import gov.aps.jca.event.ConnectionListener;
import gov.aps.jca.event.ContextExceptionEvent;
import gov.aps.jca.event.ContextExceptionListener;
import gov.aps.jca.event.ContextMessageEvent;
import gov.aps.jca.event.ContextMessageListener;
import gov.aps.jca.event.GetEvent;
import gov.aps.jca.event.GetListener;
import gov.aps.jca.event.MonitorEvent;
import gov.aps.jca.event.MonitorListener;
import gov.aps.jca.event.PutEvent;
import gov.aps.jca.event.PutListener;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

/**
 * Attempt to distribute the load of serializing the event across multiple threads
 * All events that have a Channel as the source are sent to a thread based on the hash of the pv name.
 * Everything else uses one thread similar to QueuedEventDispacther.
 * @author mshankar
 *
 */
public class JCAEventDispatcherBasedOnPVName extends AbstractEventDispatcher {
	ExecutorService allOtherEventsHandler = null;
	ExecutorService[] pvNameEventsHandlers = new ExecutorService[Math.max(Runtime.getRuntime().availableProcessors()/4, 4)];
	int numThreads =  pvNameEventsHandlers.length;
	private static Logger logger = Logger.getLogger(JCAEventDispatcherBasedOnPVName.class.getName());

	public JCAEventDispatcherBasedOnPVName() {
		super();
		allOtherEventsHandler = Executors.newFixedThreadPool(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "PVNameDispatcherAllOtherEvents");
				return t;
			}
		});
		
		for (int i = 0; i < numThreads; i++) {
			final int tnum = i;
			pvNameEventsHandlers[i] = Executors.newFixedThreadPool(1, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread t = new Thread(r, "PVNameDispatcherPVNameEvents " + tnum);
					return t;
				}
			});
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void dispatch(ContextMessageEvent arg0, List arg1) {
		try { 
			allOtherEventsHandler.submit(new Runnable() {
				ContextMessageEvent arg0;
				List<ContextMessageListener> arg1;
				public Runnable initialize(ContextMessageEvent arg0, List<ContextMessageListener> arg1) { 
					this.arg0 = arg0;
					this.arg1 = arg1;
					return this;
				}
				@Override
				public void run() {
					for(ContextMessageListener listener : arg1) {
						try { 
							listener.contextMessage(arg0);
						} catch(Throwable t) { 
							logger.warn("Exception dispatching context message event", t);
						}
					}
				}
			}.initialize(arg0, (List<ContextMessageListener>) arg1));
		} catch(Throwable t) { 
			logger.warn("Exception dispatching context message event", t);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void dispatch(ContextExceptionEvent arg0, List arg1) {
		try { 
			allOtherEventsHandler.submit(new Runnable() {
				ContextExceptionEvent arg0;
				List<ContextExceptionListener> arg1;
				public Runnable initialize(ContextExceptionEvent arg0, List<ContextExceptionListener> arg1) { 
					this.arg0 = arg0;
					this.arg1 = arg1;
					return this;
				}
				@Override
				public void run() {
					for(ContextExceptionListener listener : arg1) {
						try { 
							listener.contextException(arg0);
						} catch(Throwable t) { 
							logger.warn("Exception dispatching context exception event", t);		
						}
					}
				}
			}.initialize(arg0, (List<ContextExceptionListener>) arg1));
		} catch(Throwable t) { 
			logger.warn("Exception dispatching context exception event", t);		
		}
	}

	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void dispatch(ConnectionEvent arg0, List arg1) {
		String pvName = ((Channel)arg0.getSource()).getName();
		String pvNameOnly = pvName.split("\\.")[0];
		int threadId =  Math.abs(pvNameOnly.hashCode()) % numThreads;
		try { 
			pvNameEventsHandlers[threadId].submit(new Runnable() {
				ConnectionEvent arg0;
				List<ConnectionListener> arg1;
				public Runnable initialize(ConnectionEvent arg0, List<ConnectionListener> arg1) { 
					this.arg0 = arg0;
					this.arg1 = arg1;
					return this;
				}
				@Override
				public void run() {
					for(ConnectionListener listener : arg1) {
						try { 
							listener.connectionChanged(arg0);
						} catch(Throwable t) { 
							logger.warn("Exception dispatching connection event", t);		
						}
					}
				}
			}.initialize(arg0, (List<ConnectionListener>) arg1));
		} catch(Throwable t) { 
			logger.warn("Exception dispatching connection event", t);		
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void dispatch(AccessRightsEvent arg0, List arg1) {
		String pvName = ((Channel)arg0.getSource()).getName();
		String pvNameOnly = pvName.split("\\.")[0];
		int threadId =  Math.abs(pvNameOnly.hashCode()) % numThreads;
		try { 
			pvNameEventsHandlers[threadId].submit(new Runnable() {
				AccessRightsEvent arg0;
				List<AccessRightsListener> arg1;
				public Runnable initialize(AccessRightsEvent arg0, List<AccessRightsListener> arg1) { 
					this.arg0 = arg0;
					this.arg1 = arg1;
					return this;
				}
				@Override
				public void run() {
					for(AccessRightsListener listener : arg1) {
						try { 
							listener.accessRightsChanged(arg0);
						} catch(Throwable t) { 
							logger.warn("Exception dispatching access rights event", t);		
						}
					}
				}
			}.initialize(arg0, (List<AccessRightsListener>) arg1));
		} catch(Throwable t) { 
			logger.warn("Exception dispatching access rights event", t);		
		}

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void dispatch(MonitorEvent arg0, List arg1) {
		String pvName = ((Channel)arg0.getSource()).getName();
		String pvNameOnly = pvName.split("\\.")[0];
		int threadId =  Math.abs(pvNameOnly.hashCode()) % numThreads;
		try { 
			pvNameEventsHandlers[threadId].submit(new Runnable() {
				MonitorEvent arg0;
				List<MonitorListener> arg1;
				public Runnable initialize(MonitorEvent arg0, List<MonitorListener> arg1) { 
					this.arg0 = arg0;
					this.arg1 = arg1;
					return this;
				}
				@Override
				public void run() {
					for(MonitorListener listener : arg1) {
						try { 
							listener.monitorChanged(arg0);
						} catch(Throwable t) { 
							logger.warn("Exception dispatching monitor event", t);		
						}
					}
				}
			}.initialize(arg0, (List<MonitorListener>) arg1));
		} catch(Throwable t) { 
			logger.warn("Exception dispatching monitor event", t);		
		}
	}
	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void dispatch(GetEvent arg0, List arg1) {
		String pvName = ((Channel)arg0.getSource()).getName();
		String pvNameOnly = pvName.split("\\.")[0];
		int threadId =  Math.abs(pvNameOnly.hashCode()) % numThreads;
		try { 
			pvNameEventsHandlers[threadId].submit(new Runnable() {
				GetEvent arg0;
				List<GetListener> arg1;
				public Runnable initialize(GetEvent arg0, List<GetListener> arg1) { 
					this.arg0 = arg0;
					this.arg1 = arg1;
					return this;
				}
				@Override
				public void run() {
					for(GetListener listener : arg1) {
						try { 
							listener.getCompleted(arg0);
						} catch(Throwable t) { 
							logger.warn("Exception dispatching get event", t);		
						}
					}
				}
			}.initialize(arg0, (List<GetListener>) arg1));
		} catch(Throwable t) { 
			logger.warn("Exception dispatching get event", t);		
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void dispatch(PutEvent arg0, List arg1) {
		String pvName = ((Channel)arg0.getSource()).getName();
		String pvNameOnly = pvName.split("\\.")[0];
		int threadId =  Math.abs(pvNameOnly.hashCode()) % numThreads;
		try { 
			pvNameEventsHandlers[threadId].submit(new Runnable() {
				PutEvent arg0;
				List<PutListener> arg1;
				public Runnable initialize(PutEvent arg0, List<PutListener> arg1) { 
					this.arg0 = arg0;
					this.arg1 = arg1;
					return this;
				}
				@Override
				public void run() {
					for(PutListener listener : arg1) {
						try { 
							listener.putCompleted(arg0);
						} catch(Throwable t) { 
							logger.warn("Exception dispatching put event", t);		
						}
					}
				}
			}.initialize(arg0, (List<PutListener>) arg1));
		} catch(Throwable t) { 
			logger.warn("Exception dispatching put event", t);		
		}
	}
}
