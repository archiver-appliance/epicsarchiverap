
/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.metadata;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.archiverappliance.engine.pv.PV;
import org.epics.archiverappliance.engine.pv.PVFactory;
import org.epics.archiverappliance.engine.pv.PVListener;
/**
 * this class is used to create channel for pv and compute the meta info for one pv.
 * @author Luofeng Li
 *
 */
public class MetaGet implements Runnable {
	private static ConcurrentHashMap<String, MetaGet> metaGets = new ConcurrentHashMap<String, MetaGet>();

	private String pvName;
	private String metadatafields[];
	private boolean usePVAccess = false;
	private MetaCompletedListener metaListener;
	private Hashtable<String, PV> pvList = new Hashtable<String, PV>();

	final private ConfigService configservice;
	private static final Logger logger = Logger.getLogger(MetaGet.class.getName());
	private boolean isScheduled = false;
	private long scheduleStartEpochSecs = -1L;
	private ScheduledFuture<?> samplingFuture = null;

	public MetaGet(String pvName, ConfigService configservice,
			String metadatafields[], boolean usePVAccess, MetaCompletedListener metaListener) {
		this.pvName = pvName;
		this.usePVAccess = usePVAccess;
		this.metadatafields = metadatafields;
		this.metaListener = metaListener;
		this.configservice = configservice;
		metaGets.put(pvName, this);
	}
/**
 * create channel of pv and its meta field
 * @throws Exception error when creating channel for pv and its meta field
 */
	public void initpv() throws Exception {
		try {

			int jcaCommandThreadId = configservice.getEngineContext().assignJCACommandThread(pvName, null);
			PV pv = PVFactory.createPV(pvName, configservice, jcaCommandThreadId, usePVAccess);
			pv.addListener(new PVListener() {
				@Override
				public void pvValueUpdate(PV pv) {
				}

				@Override
				public void pvDisconnected(PV pv) {
				}

				@Override
				public void pvConnected(PV pv) {
					if (!isScheduled) {
						logger.debug("Starting the timer to measure event and storage rates for about 60 seconds for pv " + MetaGet.this.pvName);
						ScheduledThreadPoolExecutor scheduler = configservice.getEngineContext().getMiscTasksScheduler();
						samplingFuture = scheduler.schedule(MetaGet.this, 60, TimeUnit.SECONDS);
						MetaGet.this.scheduleStartEpochSecs = System.currentTimeMillis()/1000;
						isScheduled = true;
					}
				}

				@Override
				public void pvConnectionRequestMade(PV pv) {
				}

				@Override
				public void sampleDroppedTypeChange(PV pv, ArchDBRTypes newDBRtype) {
				}
			});
			pvList.put("main", pv);
			pv.start();

			if(this.usePVAccess) {
				logger.debug("Skipping getting meta fields for a PVAccess PV " + this.pvName);
			} else { 
				PV pv2 = PVFactory.createPV(PVNames.normalizePVNameWithField(pvName, "NAME"), configservice, jcaCommandThreadId, usePVAccess);
				pvList.put("NAME", pv2);
				pv2.start();
				
				PV pv3 = PVFactory.createPV(PVNames.normalizePVNameWithField(pvName, "NAME$"), configservice, jcaCommandThreadId, usePVAccess);
				pvList.put("NAME$", pv3);
				pv3.start();
				
				if (metadatafields != null) {
					for (int i = 0; i < metadatafields.length; i++) {
						String metaField = metadatafields[i];
						// We return the fields of the src PV even if we are archiving a field...
						PV pvTemp = PVFactory.createPV(PVNames.normalizePVNameWithField(pvName, metaField), configservice, jcaCommandThreadId, usePVAccess);
						pvTemp.start();
						pvList.put(metaField, pvTemp);
					}
				}
			}			
		} catch (Throwable t) {
			logger.error("Exception when initializing MetaGet " + pvName, t);
			throw (t);
		}
	}

	@Override
	public void run() {
		logger.debug("Finished the timer to measure event and storage rates for about 60 seconds for pv " + MetaGet.this.pvName);
		try {
			PV pvMain = pvList.get("main");
			MetaInfo mainMeta = pvMain.getTotalMetaInfo();
			if(this.usePVAccess) { 
				logger.debug(this.pvName + " is a PVAccess PV; so we are pretty much done with the metaget");
			} else { 
				// Per Dirk Zimoch, we first check the NAME$.
				// If that exists, we use it. If not, we use the NAME
				PV pv_NameDollar = pvList.get("NAME$");
				DBRTimeEvent nameDollarValue = pv_NameDollar.getDBRTimeEvent();
				if (nameDollarValue != null && nameDollarValue.getSampleValue() != null) {
					logger.debug("Using the NAME$ value as the NAME for pv " + pvName);
					SampleValue sampleValue = nameDollarValue.getSampleValue();
					parseAliasInfo(sampleValue, mainMeta);
				} else { 
					logger.debug("Using the NAME value as the NAME for pv " + pvName);
					PV pv_Name = pvList.get("NAME");
					DBRTimeEvent nameValue = pv_Name.getDBRTimeEvent();
					if (nameValue != null && nameValue.getSampleValue() != null) {
						SampleValue sampleValue = nameValue.getSampleValue();
						parseAliasInfo(sampleValue, mainMeta);
					} else { 
						logger.error("Either we probably did not have time to determine .NAME for " + MetaGet.this.pvName + " or the field does not exist");
					}
				}
			
				Enumeration<String> fieldNameList = pvList.keys();
				while (fieldNameList.hasMoreElements()) {
					String fieldName = fieldNameList.nextElement();
					if (fieldName.equals("main") || fieldName.equals("NAME") || fieldName.equals("NAME$")) {
						// These have already been processed; so do nothing.
					} else { 
						if (fieldName.endsWith("RTYP")) {
							if(pvList.get(fieldName) != null && pvList.get(fieldName).getDBRTimeEvent() != null && pvList.get(fieldName).getDBRTimeEvent().getSampleValue() != null) { 
								String rtyp = pvList.get(fieldName).getDBRTimeEvent().getSampleValue().toString();
								mainMeta.addOtherMetaInfo(fieldName, rtyp);
								logger.info("The RTYP for the PV " + MetaGet.this.pvName + " is " + rtyp);
							} else { 
								logger.debug("Something about RTYP is null for PV " + MetaGet.this.pvName);
							}
						} else {
							DBRTimeEvent valueTemp = pvList.get(fieldName).getDBRTimeEvent();
							if (valueTemp != null) {
								SampleValue tempvalue = valueTemp.getSampleValue();
								parseOtherInfo(tempvalue, mainMeta, fieldName);
							} else { 
								logger.warn("Either we probably did not have time to determine " + fieldName + " for " + MetaGet.this.pvName + " or the field does not exist");
							}
						}
					}
	
					pvList.get(fieldName).stop();
				}
			}
			// Make sure we have at least the DBR type here. 
			if(mainMeta.getArchDBRTypes() == null) { 
				logger.error("Cannot determine DBR type for pv " + MetaGet.this.pvName);
			}  
			metaListener.completed(mainMeta);
			metaGets.remove(pvName);
		} catch (Throwable t) {
			logger.error("Exception when scheduling MetaGet " + pvName, t);
		}
	}
/**
 * parse the sample value and save the meta info in it.
 * @param tempvalue  the sample value
 * @param mainMeta the MetaInfo object for this pv.
 */
	private void parseAliasInfo(SampleValue tempvalue, MetaInfo mainMeta) {
		if (tempvalue instanceof ScalarValue<?>) {
			// We have a number for a NAME????
			logger.error("We have a number as the NAME field for pv " + pvName);
			mainMeta.setAliasName(PVNames.transferField(pvName, "" + ((ScalarValue<?>) tempvalue).getValue().doubleValue()));
		} else if (tempvalue instanceof ScalarStringSampleValue) {
			String tempName = PVNames.transferField(pvName, ((ScalarStringSampleValue) tempvalue).toString());
			mainMeta.setAliasName(tempName);
			mainMeta.addOtherMetaInfo("NAME", tempName);
		} else if (tempvalue instanceof VectorValue<?>) {
			VectorValue<?> vectorValue = (VectorValue<?>) tempvalue;
			int elementCount = vectorValue.getElementCount();
			byte[] namebuf = new byte[elementCount];
			String nameDollar = null;
			for(int i = 0; i < elementCount; i++) { 
				byte byteValue = (byte) vectorValue.getValue(i).byteValue();
				if(byteValue == 0) { 
					try {
						nameDollar = new String(namebuf, 0, i, "UTF-8");
					} catch (UnsupportedEncodingException e) {
						logger.fatal(e.getMessage(), e);
					}
					break;
				}
				namebuf[i] = byteValue;
			}
			if(nameDollar != null) { 
				nameDollar = PVNames.transferField(pvName, nameDollar);
				mainMeta.setAliasName(nameDollar);
				mainMeta.addOtherMetaInfo("NAME", nameDollar);
			} else { 
				logger.error("We got a NAME$ value but could not use it for some reason for PV " + pvName);
			}
		} else if (tempvalue instanceof VectorStringSampleValue) {
			// We have an array of strings? for a NAME????
			String tempName = PVNames.transferField(pvName, ((VectorStringSampleValue) tempvalue).toString());
			if (!pvName.equals(tempName))
				mainMeta.setAliasName(tempName);
		}

	}
/**
 * parse the other meta info from the sample value
 * @param tempvalue sample value
 * @param mainMeta  the MetaInfo object for this pv
 * @param fieldName the info name to be parsed
 */
	private void parseOtherInfo(SampleValue tempvalue, MetaInfo mainMeta, String fieldName) {
		logger.debug("In MetaGet, processing field " + fieldName);
		if(fieldName.equals("SCAN")) {
			if(tempvalue instanceof ScalarValue<?>) {
				int enumIndex = ((ScalarValue<?>) tempvalue).getValue().intValue();
				String[] labels = pvList.get(fieldName).getTotalMetaInfo().getLabel();
				if(labels != null && enumIndex < labels.length) { 
					String scanValue = labels[enumIndex];
					logger.debug("Looked up scan value enum name and it is " + scanValue);
					mainMeta.addOtherMetaInfo("SCAN", scanValue);
					return;
				} else { 
					logger.warn("SCAN does not seem to be a valid label");
					mainMeta.addOtherMetaInfo("SCAN", Integer.toString(enumIndex));
				}
			} else if (tempvalue instanceof ScalarStringSampleValue) { 
				logger.debug("PVs from IOC's hosted on pcaspy send the SCAN as a string");
				mainMeta.addOtherMetaInfo("SCAN", ((ScalarStringSampleValue) tempvalue).toString());				
			} else {
				logger.warn("The SCAN field for this PV is not a enum or a string " + this.pvName);
			}
		}
		
		if (tempvalue instanceof ScalarValue<?>) {
			mainMeta.addOtherMetaInfo(fieldName, Double.valueOf(
					((ScalarValue<?>) tempvalue).getValue().doubleValue()));
		} else if (tempvalue instanceof ScalarStringSampleValue) {
			mainMeta.addOtherMetaInfo(fieldName,
					((ScalarStringSampleValue) tempvalue).toString());
		} else if (tempvalue instanceof VectorValue<?>) {
			mainMeta.addOtherMetaInfo(fieldName, Double.valueOf(
					((VectorValue<?>) tempvalue).getValue().doubleValue()));
		} else if (tempvalue instanceof VectorStringSampleValue) {
			mainMeta.addOtherMetaInfo(fieldName,
					((VectorStringSampleValue) tempvalue).toString());
		}

	}
	
	
	public static boolean abortMetaGet(String pvName) { 
		MetaGet metaGet = metaGets.get(pvName);
		if(metaGet != null) { 
			metaGets.remove(pvName);
			for(PV pv : metaGet.pvList.values()) { 
				pv.stop();
			}
			return true;
		}
		
		return false;
	}
	
	public static int getPendingMetaGetsSize() { 
		return metaGets.size();
	}
	public boolean isUsePVAccess() {
		return usePVAccess;
	}
	
	
	public static List<Map<String, String>> getPendingMetaDetails(String appliance) {
		List<Map<String, String>> ret = new LinkedList<Map<String, String>>();
		for(String pvNm : metaGets.keySet()) {
			MetaGet mg = metaGets.get(pvNm);
			HashMap<String, String> st = new HashMap<String, String>();
			st.put("pvName", pvNm);
			st.put("appliance", appliance);
			st.put("isScheduled", Boolean.toString(mg.isScheduled));
			st.put("scheduleStart", TimeUtils.convertToHumanReadableString(mg.scheduleStartEpochSecs));
			st.put("usePVAccess", Boolean.toString(mg.usePVAccess));
			PV pvMain = mg.pvList.get("main");
			if(pvMain != null) {
				pvMain.getLowLevelChannelInfo(ret); 
				MetaInfo mainMeta = pvMain.getTotalMetaInfo();
				if(mainMeta != null) {
					st.put("eventsSoFar", Long.toString(mainMeta.getEventCount()));
					st.put("storageSoFar", Long.toString(mainMeta.getStorageSize()));
					StringWriter buf = new StringWriter();
					buf.write("<ul><li>");
					buf.write(mainMeta.toString().replaceAll("\r\n", "</li><li>"));
					boolean first = true;
					for(String k : mainMeta.getOtherMetaInfo().keySet()) {
						if(first) { first = false; } else { buf.write("<li>"); }
						buf.write(mainMeta.getOtherMetaInfo().get(k));
						buf.write("</li>");
					}
					buf.write("</ul>");
					st.put("mainMeta", buf.toString());
				}
			} else {
				st.put("internalState", "Null"); 
			}
			if(mg.samplingFuture != null) { 
				st.put("timerRemaining", Long.toString(mg.samplingFuture.getDelay(TimeUnit.SECONDS)));
				st.put("timerDone", Boolean.toString(mg.samplingFuture.isDone()));
			}
			ret.add(st);
		}
		
		return ret;
	}
}
