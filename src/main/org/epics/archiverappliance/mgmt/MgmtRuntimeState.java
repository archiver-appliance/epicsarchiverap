package org.epics.archiverappliance.mgmt;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
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
	private Map<String, ArchivePVState> currentPVRequests = Collections.synchronizedMap(new HashMap<String, ArchivePVState>());
	private static Logger logger = LogManager.getLogger(MgmtRuntimeState.class.getName());
	private static Logger configlogger = LogManager.getLogger("config." + MgmtRuntimeState.class.getName());
	private String myIdentity;
	private ConcurrentSkipListSet<WAR_FILE> componentsThatHaveCompletedStartup = new ConcurrentSkipListSet<WAR_FILE>();
	/**
	 * Throttle the archive PV workflow to this many PV's at a time. 
	 * This seems to control the resource consumption during archive requests well
	 * Since we are throttling the workflow; we can have this many invalid archive PV requests in the system.
	 * Use the abortArchivingPV BPL to clean up requests for PVs that will never connect.
	 */
	private static final int DEFAULT_ARCHIVE_PV_WORKFLOW_BATCH_SIZE = 1000;
	
	private int archivePVWorkflowBatchSize = DEFAULT_ARCHIVE_PV_WORKFLOW_BATCH_SIZE;
	
	private static final int DEFAULT_ARCHIVE_PV_WORKFLOW_TICK_SECONDS = 10;

	private int archivePVWorkflowTickSeconds = DEFAULT_ARCHIVE_PV_WORKFLOW_TICK_SECONDS;
	
	private static final int DEFAULT_ABORT_ARCHIVE_REQUEST_TIMEOUT_MINS = 24*60;
	/**
	 * Abort PV's in the archive PV workflow after this many minutes if the archiver is not able to connect to the PV. 
	 * The workflow can take a few minutes; so this should be set to a reasonable value (for example, 1 minute would mean that no PV would complete the workflow) 
	 */
	private int abortArchiveWorkflowInMins = DEFAULT_ABORT_ARCHIVE_REQUEST_TIMEOUT_MINS;


	/**
	 * Initiate archive PV workflow for PV.
	 * @param pvName The name of PV.
	 * @throws IOException  &emsp;
	 */
	public void startPVWorkflow(String pvName) throws IOException {
		if(!currentPVRequests.containsKey(pvName)) {
			logger.debug("Starting pv archiving workflow for " + pvName);
			ArchivePVState pvState = new ArchivePVState(pvName, configService);
			currentPVRequests.put(pvName, pvState);
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
			currentPVRequests.remove(pvName);
			logger.debug("Removing " + pvName + " from config service archive pv requests");
			configService.archiveRequestWorkflowCompleted(pvName);
			logger.debug("Aborted pv archiving workflow for " + pvName + "Publishing event for engine...");
			PubSubEvent pubSubEvent = new PubSubEvent("AbortComputeMetaInfo", myIdentity + "_" + ConfigService.WAR_FILE.ENGINE, pvName);
			configService.getEventBus().post(pubSubEvent);
			return true;
		}
	}
	
	
	private static int threadNumber = 1;
	ScheduledExecutorService archivePVWorkflow = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setName("MgmtArchivePVWorkflow" + threadNumber++);
			return t;
		}
	});
	
	private ScheduledFuture<?> theArchiveWorkflow = null;
	
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
		Properties installationProperties = configService.getInstallationProperties();
		String batchSizeName = "org.epics.archiverappliance.mgmt.MgmtRuntimeState.archivePVWorkflowBatchSize";
		if(installationProperties.containsKey(batchSizeName)) { 
			this.archivePVWorkflowBatchSize = Integer.parseInt(installationProperties.getProperty(batchSizeName));
			configlogger.info("Setting the archive PV workflow batch size to " + this.archivePVWorkflowBatchSize);
		}
		
		String batchTickName = "org.epics.archiverappliance.mgmt.MgmtRuntimeState.archivePVWorkflowTickSeconds";
		if(installationProperties.containsKey(batchTickName)) { 
			this.archivePVWorkflowTickSeconds = Integer.parseInt(installationProperties.getProperty(batchTickName));
			configlogger.info("Setting the archive PV workflow tick (seconds) to " + this.archivePVWorkflowTickSeconds);
		}
		
		
		String abortArchiveWorkflowStr = "org.epics.archiverappliance.mgmt.MgmtRuntimeState.abortArchiveRequestInMins";
		if(installationProperties.containsKey(abortArchiveWorkflowStr)) { 
			this.abortArchiveWorkflowInMins = Integer.parseInt(installationProperties.getProperty(abortArchiveWorkflowStr).trim());
			configlogger.info("Setting the abort archive PV workflow timeout (in mins) to " + this.abortArchiveWorkflowInMins);
		}
		
		
	}
	
	public void finishedPVWorkflow(String pvName) throws IOException {
		currentPVRequests.remove(pvName);
	}
	
	public class NeverConnectedRequestState { 
		String pvName;
		Timestamp metInfoRequestSubmitted;
		Timestamp startOfWorkflow;
		ArchivePVState.ArchivePVStateMachine currentState;
		public NeverConnectedRequestState(String pvName, Timestamp metInfoRequestSubmitted, ArchivePVStateMachine currentState, Timestamp startOfWorkflow) {
			this.pvName = pvName;
			this.metInfoRequestSubmitted = metInfoRequestSubmitted;
			this.startOfWorkflow = startOfWorkflow;
			this.currentState = currentState;
		}
		public String getPvName() {
			return pvName;
		}
		public Timestamp getMetInfoRequestSubmitted() {
			return metInfoRequestSubmitted;
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
			if(pvState != null) {
				neverConnectedRequests.add(new NeverConnectedRequestState(pvName, pvState.getMetaInfoRequestedSubmitted(), pvState.getCurrentState(), pvState.getStartOfWorkflow()));
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
		int initialDelayInSeconds = configService.getInitialDelayBeforeStartingArchiveRequestWorkflow();
		configlogger.info("Starting archive requests after an initial delay (in s) of " + initialDelayInSeconds);
		
		for(String pvNameFromPersistence : configService.getArchiveRequestsCurrentlyInWorkflow()) {
			try { 
				this.startPVWorkflow(pvNameFromPersistence);
			} catch(IOException ex) { 
				logger.error("Exception adding request for PV " + pvNameFromPersistence, ex);
			}
		}

		
		startArchivePVWorkflow(initialDelayInSeconds);

		logger.info("Done starting archive requests");
	}

	private void startArchivePVWorkflow(int initialDelayInSeconds) {
		theArchiveWorkflow = archivePVWorkflow.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				try {
					if(!configService.hasClusterFinishedInitialization()) {
						// If you have defined spare appliances in the appliances.xml that will never come up; you should remove them
						// This seems to be one of the few ways we can prevent split brain clusters from messing up the pv <-> appliance mapping.
						configlogger.info("Waiting for all the appliances listed in appliances.xml to finish loading up their PVs into the cluster");
						return;
					}
					LinkedList<ArchivePVState> archivePVStates = new LinkedList<ArchivePVState>(currentPVRequests.values());
					logger.info("Running the archive PV workflow with " + archivePVStates.size() + " requests pending");
					Collections.sort(archivePVStates, new Comparator<ArchivePVState>() {
						@Override
						public int compare(ArchivePVState state0, ArchivePVState state1) {
							if(state0.getStartOfWorkflow().equals(state1.getStartOfWorkflow())) { 
								return state0.getPvName().compareTo(state1.getPvName());
							} else { 
								return state0.getStartOfWorkflow().compareTo(state1.getStartOfWorkflow());
							}
						}
					});
					int totRequests = archivePVStates.size();
					int maxRequestsToProcess = Math.min(archivePVWorkflowBatchSize, totRequests);
					int pvCount = 0;
					while(pvCount < maxRequestsToProcess) { 
						ArchivePVState runWorkFlowForPV = archivePVStates.pop();
						String pvName = runWorkFlowForPV.getPvName();
						// It takes a few minutes for the workflow to complete; so you should be setting this to a reasonably high value.
						if(abortArchiveWorkflowInMins > 0 
								&& ( runWorkFlowForPV.getCurrentState() != ArchivePVStateMachine.START ) 
								&& runWorkFlowForPV.getMetaInfoRequestedSubmitted() != null 
								&& (TimeUtils.now().getTime() - runWorkFlowForPV.getMetaInfoRequestedSubmitted().getTime()) > abortArchiveWorkflowInMins*60*1000) {
							try {
								runWorkFlowForPV.setAbortReason("Aborting PV after user specified timeout " + TimeUtils.convertToHumanReadableString(runWorkFlowForPV.getMetaInfoRequestedSubmitted()));
								abortPVWorkflow(pvName);
							} catch(Exception ex) { 
								logger.error("Exception aborting PV after timeout " + pvName, ex);
							}
						} else {
							logger.debug("Running the next step in the workflow for PV " + pvName);
							runWorkFlowForPV.nextStep();
						}
						pvCount++;
					}				
				} catch(Throwable t) {
					logger.error("Exception processing next step in archive PV workflow", t);
				}

			}
		}, initialDelayInSeconds, archivePVWorkflowTickSeconds, TimeUnit.SECONDS);
	}
	
	
	/**
	 * Abort all pending PV requests and then restart the thread.
	 * Use only if your main archive thread seems to be stuck.
	 */
	public void abortAllAndRestartArchiveRequestsThread() throws IOException {
		configlogger.info("Cancelling the main archive PV workflow thread ( to restart it ).");
		this.theArchiveWorkflow.cancel(false);
		configlogger.info("Aborting all existing requests.");
		LinkedList<String> currPVs = new LinkedList<String>(this.currentPVRequests.keySet());
		for(String pvName : currPVs) {
			this.abortPVWorkflow(pvName);
		}
		configlogger.info("Aborted all the PV requests");
		startArchivePVWorkflow(archivePVWorkflowTickSeconds);
		configlogger.info("Restarted the arvhive PV workflow with an initial delay of " + archivePVWorkflowTickSeconds);
	}

	
	
	public boolean haveChildComponentsStartedUp() { 
		return componentsThatHaveCompletedStartup.contains(WAR_FILE.ENGINE) 
				&& componentsThatHaveCompletedStartup.contains(WAR_FILE.ETL) 
				&& componentsThatHaveCompletedStartup.contains(WAR_FILE.RETRIEVAL);
	}
	
	/**
	 * Is the specified PV in the mgmt workflow?
	 * @param pvName The name of PV.
	 * @return boolean True or False
	 */
	public boolean isPVInWorkflow(String pvName) { 
		return this.currentPVRequests.containsKey(pvName);
	}


	/**
	 * Get the batch size for PV archive requests workflow.
 	 * We throttle the archive PV workflow to this many PV's at a time to conserve resources and prevent CA storms.
 	 * This can be configured using a property in archappl.properties. 
	 * @return archivePVWorkflowBatchSize   &emsp;
	 */
	public int getArchivePVWorkflowBatchSize() {
		return archivePVWorkflowBatchSize;
	}
}
