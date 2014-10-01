
/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.metadata;

import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.archiverappliance.engine.pv.EPICS_V3_PV;
import org.epics.archiverappliance.engine.pv.PV;
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
	private MetaCompletedListener metaListener;
	private Hashtable<String, EPICS_V3_PV> pvList = new Hashtable<String, EPICS_V3_PV>();

	final private ConfigService configservice;
	private static final Logger logger = Logger.getLogger(MetaGet.class.getName());
	private boolean isScheduled = false;

	public MetaGet(String pvName, ConfigService configservice,
			String metadatafields[], MetaCompletedListener metaListener) {
		this.pvName = pvName;
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
			EPICS_V3_PV pv = new EPICS_V3_PV(pvName, configservice, jcaCommandThreadId);
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
						ScheduledThreadPoolExecutor scheduler = configservice.getEngineContext().getScheduler();
						scheduler.schedule(MetaGet.this, 60, TimeUnit.SECONDS);
						isScheduled = true;
					}
				}

			});
			pvList.put("main", pv);
			pv.start();

			EPICS_V3_PV pv2 = new EPICS_V3_PV(pvName + ".NAME", configservice, jcaCommandThreadId);
			pvList.put("NAME", pv2);
			pv2.start();
			
			EPICS_V3_PV pv3 = new EPICS_V3_PV(pvName + ".NAME$", configservice, jcaCommandThreadId);
			pvList.put("NAME$", pv3);
			pv3.start();
			
			if (metadatafields != null) {
				for (int i = 0; i < metadatafields.length; i++) {
					String metaField = metadatafields[i];
					// We return the fields of the src PV even if we are archiving a field...
					EPICS_V3_PV pvTemp = new EPICS_V3_PV(PVNames.normalizePVNameWithField(pvName, metaField), configservice, jcaCommandThreadId);
					pvTemp.start();
					pvList.put(metaField, pvTemp);
				}
			}
		} catch (Exception e) {
			throw (e);
		}
	}

	@Override
	public void run() {
		logger.debug("Finished the timer to measure event and storage rates for about 60 seconds for pv " + MetaGet.this.pvName);
		try {
			EPICS_V3_PV pvMain = pvList.get("main");
			MetaInfo mainMeta = pvMain.getToalMetaInfo();
			// Per Dirk Zimoch, we first check the NAME$.
			// If that exists, we use it. If not, we use the NAME
			EPICS_V3_PV pv_NameDollar = pvList.get("NAME$");
			DBRTimeEvent nameDollarValue = pv_NameDollar.getValue();
			if (nameDollarValue != null && nameDollarValue.getSampleValue() != null) {
				logger.debug("Using the NAME$ value as the NAME for pv " + pvName);
				SampleValue sampleValue = nameDollarValue.getSampleValue();
				parseAliasInfo(sampleValue, mainMeta);
			} else { 
				logger.debug("Using the NAME value as the NAME for pv " + pvName);
				EPICS_V3_PV pv_Name = pvList.get("NAME");
				DBRTimeEvent nameValue = pv_Name.getValue();
				if (nameValue != null && nameValue.getSampleValue() != null) {
					SampleValue sampleValue = nameValue.getSampleValue();
					parseAliasInfo(sampleValue, mainMeta);
				} else { 
					logger.warn("Either we probably did not have time to determine .NAME for " + MetaGet.this.pvName + " or the field does not exist");
				}
			}
			
			Enumeration<String> fieldNameList = pvList.keys();
			while (fieldNameList.hasMoreElements()) {
				String fieldName = fieldNameList.nextElement();
				if (fieldName.equals("main") || fieldName.equals("NAME") || fieldName.equals("NAME$")) {
					// These have already been processed; so do nothing.
				} else { 
					if (fieldName.endsWith("RTYP")) {
						mainMeta.addOtherMetaInfo(fieldName, pvList.get(fieldName).getReacordTypeName());
					} else {
						DBRTimeEvent valueTemp = pvList.get(fieldName).getValue();
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
			// Make sure we have at least the DBR type here. 
			if(mainMeta.getArchDBRTypes() == null) { 
				logger.error("Cannot determine DBR type for pv " + MetaGet.this.pvName);
			}  
			metaListener.completed(mainMeta);
			metaGets.remove(pvName);
		} catch (Exception ex) {
			logger.error("Exception when schecule MetaGet " + pvName, ex);
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
			mainMeta.setAliasName("" + ((ScalarValue<?>) tempvalue).getValue().doubleValue());
		} else if (tempvalue instanceof ScalarStringSampleValue) {
			String tempName = ((ScalarStringSampleValue) tempvalue).toString();
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
				mainMeta.setAliasName(nameDollar);
				mainMeta.addOtherMetaInfo("NAME", nameDollar);
			} else { 
				logger.error("We got a NAME$ value but could not use it for some reason for PV " + pvName);
			}
		} else if (tempvalue instanceof VectorStringSampleValue) {
			// We have an array of strings? for a NAME????
			String tempName = ((VectorStringSampleValue) tempvalue).toString();
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
	private void parseOtherInfo(SampleValue tempvalue, MetaInfo mainMeta,
			String fieldName) {
		if (tempvalue instanceof ScalarValue<?>) {
			mainMeta.addOtherMetaInfo(fieldName, new Double(
					((ScalarValue<?>) tempvalue).getValue().doubleValue()));
		} else if (tempvalue instanceof ScalarStringSampleValue) {
			mainMeta.addOtherMetaInfo(fieldName,
					((ScalarStringSampleValue) tempvalue).toString());
		} else if (tempvalue instanceof VectorValue<?>) {
			mainMeta.addOtherMetaInfo(fieldName, new Double(
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
			for(EPICS_V3_PV pv : metaGet.pvList.values()) { 
				pv.stop();
			}
			return true;
		}
		
		return false;
	}
	
	public static int getPendingMetaGetsSize() { 
		return metaGets.size();
	}
}
