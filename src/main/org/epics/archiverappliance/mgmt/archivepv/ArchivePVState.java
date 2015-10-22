package org.epics.archiverappliance.mgmt.archivepv;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.epics.archiverappliance.config.exception.AlreadyRegisteredException;
import org.epics.archiverappliance.config.pubsub.PubSubEvent;
import org.epics.archiverappliance.mgmt.bpl.ArchivePVAction;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.JSONEncoder;

/**
 * State for the archive PV workflow
 * @author mshankar
 *
 */
public class ArchivePVState {
	private static Logger logger = Logger.getLogger(ArchivePVState.class.getName());
	public enum ArchivePVStateMachine { START, METAINFO_REQUESTED, METAINFO_OBTAINED, POLICY_COMPUTED, TYPEINFO_STABLE, ARCHIVE_REQUEST_SUBMITTED, ARCHIVING, ABORTED, FINISHED};

	private ArchivePVStateMachine currentState = ArchivePVStateMachine.START;
	private String pvName;
	private String abortReason = "";
	private ConfigService configService;
	private String applianceIdentityAfterCapacityPlanning;
	private Timestamp startOfWorkflow = TimeUtils.now();
	private Timestamp metaInfoRequestedSubmitted = null;
	private String myIdentity;
	private MetaInfo metaInfo = null;

	public ArchivePVState(String pvName, ConfigService configService) {
		this.pvName = pvName;
		this.configService = configService;
		this.myIdentity = this.configService.getMyApplianceInfo().getIdentity();
	}

	public void nextStep() {
		try { 
			logger.debug("Archive workflow for pv " + pvName + " in state " + currentState);
				switch(currentState) {
				case START: {
					PubSubEvent pubSubEvent = new PubSubEvent("ComputeMetaInfo", myIdentity + "_" + ConfigService.WAR_FILE.ENGINE, pvName);
					UserSpecifiedSamplingParams userSpec = configService.getUserSpecifiedSamplingParams(pvName);
					JSONEncoder<UserSpecifiedSamplingParams> encoder = JSONEncoder.getEncoder(UserSpecifiedSamplingParams.class);
					pubSubEvent.setEventData(encoder.encode(userSpec).toJSONString());
					configService.getEventBus().post(pubSubEvent);
					return;	
				}
				case METAINFO_REQUESTED: {
					logger.debug("Metainfo has been requested for " + pvName);
					return;
				}
				case METAINFO_OBTAINED: {
					logger.debug("Metainfo obtained for pv " + pvName);
					if(metaInfo == null) {
						logger.error("We are in state METAINFO_OBTAINED but the metainfo object is null");
						currentState = ArchivePVStateMachine.ABORTED;
						return;
					}

					if(metaInfo.getArchDBRTypes() == null) {
						logger.error("Invalid/null DBR type for pv " + pvName);
						currentState = ArchivePVStateMachine.ABORTED;
						return;
					}

					UserSpecifiedSamplingParams userSpec = configService.getUserSpecifiedSamplingParams(pvName);
					if(userSpec == null) {
						logger.error("Unable to find user sepcification of archival parameters for pv " + pvName);
						currentState = ArchivePVStateMachine.ABORTED;
						return;
					}
					

					logger.debug("About to compute policy for " + pvName);
					PolicyConfig thePolicy = configService.computePolicyForPV(pvName, metaInfo, userSpec);
					if(thePolicy.getSamplingMethod() == SamplingMethod.DONT_ARCHIVE) {
						logger.error("According to the policy, we must not archive pv as the sampling method is DONT_ARCHIVE for PV " + pvName);
						currentState = ArchivePVStateMachine.ABORTED;
						return;
					} else {
						logger.info("Policy for pv " + pvName+ " is " + thePolicy.generateStringRepresentation());
					}

					SamplingMethod theSamplingMethod = thePolicy.getSamplingMethod();
					float theSamplingPeriod = thePolicy.getSamplingPeriod();
					if(userSpec.isUserOverrideParams()) {
						theSamplingMethod = userSpec.userSpecifedsamplingMethod;
						theSamplingPeriod = userSpec.getUserSpecifedSamplingPeriod();
						logger.debug("Overriding sampling period  to " + theSamplingPeriod + " and using " + theSamplingMethod);
					}
					
					boolean isField = PVNames.isField(pvName);
					
					PVTypeInfo typeInfo = new PVTypeInfo(pvName, metaInfo.getArchDBRTypes(), !metaInfo.isVector(), metaInfo.getCount());
					typeInfo.absorbMetaInfo(metaInfo);
					typeInfo.setSamplingMethod(theSamplingMethod);
					typeInfo.setSamplingPeriod(theSamplingPeriod);
					typeInfo.setDataStores(thePolicy.getDataStores());
					typeInfo.setCreationTime(TimeUtils.now());
					typeInfo.setControllingPV(userSpec.getControllingPV());
					typeInfo.setUsePVAccess(userSpec.isUsePVAccess());
					
					String aliasFieldName = "NAME";
					if(typeInfo.hasExtraField(aliasFieldName)) {
						if(userSpec.isSkipAliasCheck()) { 
							logger.info("Skipping alias check for pv " + pvName + " as the isSkipAliasCheck in userparams is set.");
						} else { 

							String realName = typeInfo.lookupExtraField(aliasFieldName);
							String pvNameAlone = PVNames.stripFieldNameFromPVName(pvName);
							if(!realName.equals(pvNameAlone)) {
								logger.info("PV " + pvName + " is an alias of " + realName);
								// It is possible that some implementations use the .NAME field for purposes other than the alias.
								// In this case, we make sure that the real name matches the PV name by checking that it at least has one or more separators.
								if(!configService.getPVNameToKeyConverter().containsSiteSeparators(realName)) { 
									logger.debug("There seem to be no siteNameSpaceSeparators in the real name " + realName + " in workflow for pv " + pvName + ". Archiving under the PV name instead");
								} else { 
									convertAliasToRealWorkflow(userSpec, realName);
									abortReason = "Aborting this pv " + pvName + " (which is an alias) and using the real name " + realName + " instead.";
									logger.debug(abortReason);
									currentState = ArchivePVStateMachine.ABORTED;
									return;
								}
							} else { 
								logger.debug("Name from alias and normalized name are the same");
							}
						}
					}
					
					if(!isField) {
						for(String field : thePolicy.getArchiveFields()) {
							typeInfo.addArchiveField(field);
						}

						// Copy over any archive fields from the user spec
						if(userSpec != null && userSpec.wereAnyFieldsSpecified()) {
							for(String fieldName : userSpec.getArchiveFields()) {
								typeInfo.addArchiveField(fieldName);
							}
						}
					}
					
					ApplianceInfo applianceInfoForPV = null;
					
					if(userSpec.isSkipCapacityPlanning()) { 
						logger.info("Skipping capacity planning for pv " + pvName + ". Assigning to myself.");
						applianceInfoForPV = configService.getMyApplianceInfo();
					} else if (thePolicy.getAppliance() != null) {
						// Hopefully the poicy is configured correctly and we get a valid appliance
						applianceInfoForPV = configService.getAppliance(thePolicy.getAppliance());
						assert(applianceInfoForPV != null);
						logger.info("Assigning pv " + pvName + " to appliance " + thePolicy.getAppliance() + " based on policy " + thePolicy.getPolicyName());
					} else { 
						if(isField) {
							String pvNameAlone = PVNames.stripFieldNameFromPVName(pvName);
							ApplianceInfo pvNameAloneAssignedToAppliance = configService.getApplianceForPV(pvNameAlone);
							if(pvNameAloneAssignedToAppliance != null) {
								logger.info("Assinging field " + pvName + " to the same appliance as the pv itself " + pvNameAloneAssignedToAppliance.getIdentity());
								applianceInfoForPV = pvNameAloneAssignedToAppliance;
							} else {
								logger.info("Assinging field " + pvName + " to a new appliance");
								applianceInfoForPV = CapacityPlanningBPL.pickApplianceForPV(pvName, configService, typeInfo);
							}
						} else {
							logger.debug("Not a field " + pvName + " picking a new appliance.");
							applianceInfoForPV = CapacityPlanningBPL.pickApplianceForPV(pvName, configService, typeInfo);
						}
					}

					assert(applianceInfoForPV != null);
					logger.info("After capacity planning, appliance for pv " + pvName + " is " + applianceInfoForPV.getIdentity());
					applianceIdentityAfterCapacityPlanning = applianceInfoForPV.getIdentity();

					try {
						configService.registerPVToAppliance(pvName, applianceInfoForPV);
						typeInfo.setApplianceIdentity(applianceIdentityAfterCapacityPlanning);
						configService.updateTypeInfoForPV(pvName, typeInfo);
						currentState = ArchivePVStateMachine.POLICY_COMPUTED;
					} catch(AlreadyRegisteredException ex) {
						logger.error("PV " + pvName + " is already registered. Aborting this request");
						currentState = ArchivePVStateMachine.ABORTED;
					}
					return;
				}
				case POLICY_COMPUTED: {
					PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
					if(typeInfo.getApplianceIdentity().equals(applianceIdentityAfterCapacityPlanning)) {
						currentState = ArchivePVStateMachine.TYPEINFO_STABLE;
					}
					return;
				}
				case TYPEINFO_STABLE: {
					PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
					ArchivePVState.startArchivingPV(pvName, configService, configService.getAppliance(typeInfo.getApplianceIdentity()));
					registerAliasesIfAny(typeInfo);
					currentState = ArchivePVStateMachine.ARCHIVE_REQUEST_SUBMITTED;
					return;
				}
				case ARCHIVE_REQUEST_SUBMITTED:
					// The part in mgmt is basically over; we are waiting for the engine to indicate that it has commenced archiving
					// Until then, we stay in this state.
					return;
				case ARCHIVING: {
					logger.debug("We are in the Archiving state. So, cancelling the periodic ping of the workflow object for pv " + pvName);
					configService.archiveRequestWorkflowCompleted(pvName);
					configService.getMgmtRuntimeState().finishedPVWorkflow(pvName);
					currentState = ArchivePVStateMachine.FINISHED;
					return;
				}
				case ABORTED: {
					configService.archiveRequestWorkflowCompleted(pvName);
					configService.getMgmtRuntimeState().finishedPVWorkflow(pvName);
					logger.error("Aborting archive request for pv " + pvName + " Reason: " + abortReason);
					currentState = ArchivePVStateMachine.FINISHED;
					return;
				}
				case FINISHED: {
					logger.error("Archive state for PV " + this.pvName + " is finished.");
					return;
				}
				default: {
					logger.error("Invalid state when going thru the archive pv workflow");
					return;
				}
			}
		} catch(Exception ex) {
			logger.error("Exception transitioning archive pv state for pv " + pvName + " in state " + currentState, ex);
		}
	}

	public boolean hasNotConnectedSoFar() {
		return this.currentState.equals(ArchivePVState.ArchivePVStateMachine.METAINFO_REQUESTED) || this.currentState.equals(ArchivePVState.ArchivePVStateMachine.ABORTED);
	}

	/**
	 * Start archiving the PV as specified in the PVTypeInfo in configService.
	 * This method expects to be called after the PVTypeInfo for this PV has been completely determined and has settled in the cache. 
	 * @param pvName
	 * @param configService
	 * @param applianceInfoForPV
	 * @return
	 * @throws IOException
	 */
	public static void startArchivingPV(String pvName, ConfigService configService, ApplianceInfo applianceInfoForPV) throws IOException {
		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.error("Unable to find pvTypeInfo for PV" + pvName + ". This is an error; this method should be called after the pvTypeInfo has been determined and settled in the DHT");
			throw new IOException("Unable to find pvTypeInfo for PV" + pvName);
		}
		
		logger.debug("Setting up archiving of pv " + pvName);
		PubSubEvent pubSubEvent = new PubSubEvent("StartArchivingPV", applianceInfoForPV.getIdentity()  + "_" + ConfigService.WAR_FILE.ENGINE, pvName);
		configService.getEventBus().post(pubSubEvent);
	}

	public Timestamp getStartOfWorkflow() {
		return startOfWorkflow;
	}
	
	
	public void metaInfoRequestAcknowledged() { 
		metaInfoRequestedSubmitted = TimeUtils.now();
		this.currentState = ArchivePVStateMachine.METAINFO_REQUESTED;
	}
	
	public void metaInfoObtained(MetaInfo metaInfo) { 
		this.metaInfo = metaInfo;
		this.currentState = ArchivePVStateMachine.METAINFO_OBTAINED;
	}
	
	public void errorGettingMetaInfo() { 
		abortReason = "Error getting meta info";
		this.currentState = ArchivePVStateMachine.ABORTED;
	}	
	
	public void confirmedStartedArchivingPV() {
		this.currentState = ArchivePVStateMachine.ARCHIVING;
	}

	/**
	 * If the user specified params has any aliases specified, we register the alias now.
	 * @param typeInfo
	 */
	private void registerAliasesIfAny(PVTypeInfo typeInfo) { 
		// If we are already archiving this PV, use addAlias to add an alias to the appliance hosting the pvTypeInfo
		String addAliasURL = null;
		try {
			UserSpecifiedSamplingParams userSpec = configService.getUserSpecifiedSamplingParams(typeInfo.getPvName());
			logger.debug("Adding aliases for " + typeInfo.getPvName());
			if(userSpec != null && userSpec.getAliases() != null && userSpec.getAliases().length > 0) { 
				logger.debug("Adding " + userSpec.getAliases().length + " aliases for " + typeInfo.getPvName());
				for(String aliasName : userSpec.getAliases()) {
					addAliasURL = configService.getAppliance(typeInfo.getApplianceIdentity()).getMgmtURL() + "/addAlias?pv=" 
							+ URLEncoder.encode(typeInfo.getPvName(), "UTF-8") 
							+ "&aliasname=" 
							+ URLEncoder.encode(aliasName, "UTF-8");
					logger.debug("Adding an alias " + aliasName + " for pv " + typeInfo.getPvName());
					GetUrlContent.getURLContentAsJSONObject(addAliasURL);
				}
			}
		} catch(Throwable t) { 
			logger.error("Exception adding alias using URL " + addAliasURL, t);
		}
	}
	
	/**
	 * Convert a alias workflow into a workflow entry for a real PV and add the alias as a user specified param. 
	 * @param userSpec
	 * @param realName
	 */
	private void convertAliasToRealWorkflow(UserSpecifiedSamplingParams userSpec, String realName) {
		try {
			ApplianceInfo applianceForPV = configService.getApplianceForPV(realName);
			if(applianceForPV != null || configService.doesPVHaveArchiveRequestInWorkflow(realName)) { 
				logger.debug("We are already archiving or about to archive the real PV " + realName + " which is the alias for " + pvName + ". Aborting this request");
				if(applianceForPV != null) { 
					// If we are already archiving this PV, use addAlias to add an alias to this appliance
					String addAliasURL = null;
					try { 
						addAliasURL = applianceForPV.getMgmtURL() + "/addAlias?pv=" 
								+ URLEncoder.encode(realName, "UTF-8") 
								+ "&aliasname=" 
								+ URLEncoder.encode(pvName, "UTF-8")
								+ "&useThisAppliance=true";
						GetUrlContent.getURLContentAsJSONObject(addAliasURL);
					} catch(Throwable t) { 
						logger.error("Exception adding alias using URL " + addAliasURL, t);
					}
				} else if (configService.doesPVHaveArchiveRequestInWorkflow(realName)) { 
					// Change the UserSpecifiedSamplingParams to include this alias.
					// We really can't tell which appliance is processing the workflow. 
					// AddAlias uses getMgmtRuntimeState to see if the PV is in the workflow and then updates the user specified params.
					// So we call all the appliances.
					LinkedList<String> addAliasURLs = new LinkedList<String>();
					for(ApplianceInfo info : configService.getAppliancesInCluster()) {
						addAliasURLs.add(info.getMgmtURL() + "/addAlias?pv=" + URLEncoder.encode(realName, "UTF-8") + "&aliasname=" + URLEncoder.encode(pvName, "UTF-8"));
					}
					for(String addAliasURL : addAliasURLs) {
						try { 
							GetUrlContent.getURLContentAsJSONObject(addAliasURL, false);
						} catch(Throwable t ) { 
							logger.debug("Exception adding alias for pv" + pvName, t);
						}
					}
					
				} else { 
					logger.error("Missed the boat adding alias " + pvName + " for real name " + realName + ". Please try again later.");
				}
			} else {
				logger.debug("We are not archiving the real PV " + realName + " which is the alias for " + pvName + ". Aborting this request and asking to archive " + realName);
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				PrintWriter out = new PrintWriter(bos);
				ArchivePVAction.archivePV(out, realName, userSpec.isUserOverrideParams(), userSpec.getUserSpecifedsamplingMethod(), userSpec.getUserSpecifedSamplingPeriod(), userSpec.getControllingPV(), userSpec.getPolicyName(), pvName, userSpec.isSkipCapacityPlanning(), configService, ArchivePVAction.getFieldsAsPartOfStream(configService));
				out.close();
			}
		} catch(Exception ex) { 
			logger.error("Exception archiving alias " + realName + " in workflow for " + pvName, ex);
		}
	}

	/**
	 * @return The current archiving state machine state
	 */
	public ArchivePVStateMachine getCurrentState() {
		return currentState;
	}

	public String getPvName() {
		return pvName;
	}

	public Timestamp getMetaInfoRequestedSubmitted() {
		return metaInfoRequestedSubmitted;
	}
}