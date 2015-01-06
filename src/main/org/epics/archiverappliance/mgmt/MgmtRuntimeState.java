package org.epics.archiverappliance.mgmt;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigService.WAR_FILE;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.config.pubsub.PubSubEvent;
import org.epics.archiverappliance.mgmt.archivepv.ArchivePVState;
import org.epics.archiverappliance.mgmt.archivepv.ArchivePVState.ArchivePVStateMachine;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.google.common.eventbus.Subscribe;

/**
 * Runtime state for the mgmt app.
 * @author mshankar
 *
 */
public class MgmtRuntimeState {
	private ConfigService configService;
	private ConcurrentHashMap<String, ArchivePVState> currentPVRequests = new ConcurrentHashMap<String, ArchivePVState>();
	private static Logger logger = Logger.getLogger(MgmtRuntimeState.class.getName());
	private static Logger configlogger = Logger.getLogger("config." + MgmtRuntimeState.class.getName());
	private String myIdentity;
	private ConcurrentSkipListSet<WAR_FILE> componentsThatHaveCompletedStartup = new ConcurrentSkipListSet<WAR_FILE>();

	public void startPVWorkflow(String pvName) throws IOException {
		this.startPVWorkflow(pvName, 10);
	}
	
	/**
	 * Initiate archive PV workflow for PV.
	 * @param pvName
	 * @param initialDelay - Initial delay to start the workflow in seconds
	 * @throws IOException
	 */
	public void startPVWorkflow(String pvName, long initialDelay) throws IOException {
		if(!currentPVRequests.containsKey(pvName)) {
			logger.debug("Starting pv archiving workflow for " + pvName);
			ArchivePVState pvState = new ArchivePVState(pvName, configService);
			currentPVRequests.put(pvName, pvState);
			ScheduledFuture<?> future = archivePVWorkflow.scheduleAtFixedRate(pvState, initialDelay, 20, TimeUnit.SECONDS);
			// Pass the future to the workflow object so that the scheduled task can be cancelled.
			pvState.setCancellingFuture(future);
		} else { 
			logger.error("We already have a request for pv " + pvName + " in the workflow.");
		}
	}
	
	
	public boolean abortPVWorkflow(String pvName) throws IOException {
		if(!currentPVRequests.containsKey(pvName)) {
			logger.error("We do not have a request for pv " + pvName + " in the workflow.");
			return false;
		} else { 
			logger.debug("Aborting pv archiving workflow for " + pvName);
			ArchivePVState pvState = currentPVRequests.get(pvName);
			pvState.getFutureForCancelling().cancel(true);
			currentPVRequests.remove(pvName);
			logger.debug("Removing " + pvName + " from config service archive pv requests");
			configService.archiveRequestWorkflowCompleted(pvName);
			logger.debug("Aborted pv archiving workflow for " + pvName);
			return true;
		}
	}
	
	
	private static int threadNumber = 1;
	ScheduledExecutorService archivePVWorkflow = Executors.newScheduledThreadPool(1, new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setName("MgmtArchivePVWorkflow" + threadNumber++);
			return t;
		}
	});
	
	public MgmtRuntimeState(final ConfigService configService) {
		this.configService = configService;
		myIdentity = this.configService.getMyApplianceInfo().getIdentity();
		configService.addShutdownHook(new Runnable() {
			@Override
			public void run() {
				archivePVWorkflow.shutdown();
			}
		});
		configService.getEventBus().register(this);
	}
	
	public void finishedPVWorkflow(String pvName) throws IOException {
		currentPVRequests.remove(pvName);
	}
	
	public class NeverConnectedRequestState { 
		String pvName;
		Timestamp startOfWorkflow;
		ArchivePVState.ArchivePVStateMachine currentState;
		public NeverConnectedRequestState(String pvName, Timestamp startOfWorkflow, ArchivePVStateMachine currentState) {
			this.pvName = pvName;
			this.startOfWorkflow = startOfWorkflow;
			this.currentState = currentState;
		}
		public String getPvName() {
			return pvName;
		}
		public Timestamp getStartOfWorkflow() {
			return startOfWorkflow;
		}
		public ArchivePVState.ArchivePVStateMachine getCurrentState() {
			return currentState;
		}
		
		
	}
	
	public List<NeverConnectedRequestState> getNeverConnectedRequests() {
		List<NeverConnectedRequestState> neverConnectedRequests = new LinkedList<NeverConnectedRequestState>();
		for(String pvName : currentPVRequests.keySet()) {
			ArchivePVState pvState = currentPVRequests.get(pvName);
			if(pvState != null && pvState.hasNotConnectedSoFar()) {
				neverConnectedRequests.add(new NeverConnectedRequestState(pvName, pvState.getStartOfWorkflow(), pvState.getCurrentState()));
			}
		}
		return neverConnectedRequests;
	}
	
	public int getPVsPendingInWorkflow() {
		return currentPVRequests.size();
	}	
	
	
	@Subscribe public void computeMetaInfo(PubSubEvent pubSubEvent) {
		if(pubSubEvent.getDestination().equals("ALL") 
				|| (pubSubEvent.getDestination().startsWith(myIdentity) && pubSubEvent.getDestination().endsWith(ConfigService.WAR_FILE.MGMT.toString()))) {
			if(pubSubEvent.getType().equals("MetaInfoRequested")) {
				String pvName = pubSubEvent.getPvName();
				logger.debug("MetaInfoRequested for " + pvName);
				ArchivePVState pvState = currentPVRequests.get(pvName);
				if(pvState != null) {
					pvState.metaInfoRequestAcknowledged();
				}
			} else if (pubSubEvent.getType().equals("MetaInfoFinished")) {
				String pvName = pubSubEvent.getPvName();
				logger.debug("MetaInfoFinished for " + pvName);
				ArchivePVState pvState = currentPVRequests.get(pvName);
				if(pvState != null) {
					try {
						MetaInfo metaInfo = new MetaInfo();
						JSONObject metaInfoObj = (JSONObject) JSONValue.parse(pubSubEvent.getEventData());
						JSONDecoder<MetaInfo> decoder = JSONDecoder.getDecoder(MetaInfo.class);
						decoder.decode(metaInfoObj, metaInfo);
						pvState.metaInfoObtained(metaInfo);
					} catch(Exception ex) {
						logger.error("Exception processing metainfo for pv " + pvName, ex);
						pvState.errorGettingMetaInfo();
					}
				}
			} else if (pubSubEvent.getType().equals("StartedArchivingPV")) {
				String pvName = pubSubEvent.getPvName();
				logger.debug("Stared archiving pv confirmation for " + pvName);
				ArchivePVState pvState = currentPVRequests.get(pvName);
				if(pvState != null) {
					pvState.confirmedStartedArchivingPV();
				}
			}
		} else {
			logger.debug("Skipping processing event meant for " + pubSubEvent.getDestination());
		}
	}
	
	
	public void componentStartedUp(WAR_FILE component) { 
		componentsThatHaveCompletedStartup.add(component);
		if(this.haveChildComponentsStartedUp()) { 
			configlogger.info("All components in this appliance have started up. We should be ready to start accepting UI requests");
			this.startArchivePVRequests();
		}
	}
	
	
	private void startArchivePVRequests() { 
		logger.info("Starting archive requests ");
		int appliancesInCluster = 0;
		for(@SuppressWarnings("unused") ApplianceInfo info : configService.getAppliancesInCluster()) { 
			appliancesInCluster++;
		}
		int pvsInWorkflow = 0;
		for(String pvNameFromPersistence : configService.getArchiveRequestsCurrentlyInWorkflow()) {
			try { 
				long initialDelayInSeconds = 10;
				if(appliancesInCluster > 1) { 
					// We use a longer initial delay here to get all the appliances in the cluster a chance to restart
					initialDelayInSeconds = 30*60;
				}
				logger.debug("Starting archive PV request for " + pvNameFromPersistence);
				this.startPVWorkflow(pvNameFromPersistence, initialDelayInSeconds);
				pvsInWorkflow++;
			} catch(IOException ex) { 
				logger.error("Exception starting archive workflow for PV " + pvNameFromPersistence, ex);
			}
		}
		logger.info("Done starting archive requests for " + pvsInWorkflow + " pvs.");
	}
	
	public boolean haveChildComponentsStartedUp() { 
		return componentsThatHaveCompletedStartup.contains(WAR_FILE.ENGINE) 
				&& componentsThatHaveCompletedStartup.contains(WAR_FILE.ETL) 
				&& componentsThatHaveCompletedStartup.contains(WAR_FILE.RETRIEVAL);
	}
	
	/**
	 * Is the specified PV in the mgmt workflow?
	 * @param pvName
	 * @return
	 */
	public boolean isPVInWorkflow(String pvName) { 
		return this.currentPVRequests.containsKey(pvName);
	}
}
