package org.epics.archiverappliance.retrieval;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PoorMansProfiler;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.mgmt.bpl.PVsMatchingParameter;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.ExtraFieldsPostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

public class GetDataAtTime {
	private static final Logger logger = Logger.getLogger(GetDataAtTime.class);
	
	static class Appliance2PVs {
		ApplianceInfo applianceInfo;
		LinkedList<String> pvsFromAppliance;
		LinkedList<String> remainingPVs;
		HashMap<String, HashMap<String, Object>> pvValues;	
		Appliance2PVs(ApplianceInfo applianceInfo) {
			this.applianceInfo = applianceInfo;
			this.pvsFromAppliance = new LinkedList<String>();
			this.remainingPVs = new LinkedList<String>();
			this.pvValues = new HashMap<String, HashMap<String, Object>>();
		}
	}
	
	@SuppressWarnings("unchecked")
	private static Appliance2PVs getPVSFromAppliance(Appliance2PVs gatherer, LinkedList<String> pvNames) {
		try {
			JSONArray resp = GetUrlContent.postStringListAndGetJSON(gatherer.applianceInfo.getMgmtURL() + "/archivedPVsForThisAppliance", "pv", pvNames);
			logger.debug("Done calling remote appliance " + gatherer.applianceInfo.getIdentity() + " and got PV count " + resp.size());
			gatherer.pvsFromAppliance = new LinkedList<String>(resp);
			gatherer.remainingPVs = new LinkedList<String>(gatherer.pvsFromAppliance);
		} catch (IOException ex) {
			logger.error("Exception getting list of PVs from appliance", ex);
		}
		return gatherer;
	}
	
	private static Appliance2PVs getDataFromEngine(Appliance2PVs gatherer, Timestamp atTime) {
		try {
			if(gatherer.remainingPVs.size() <= 0) return gatherer;
			HashMap<String, HashMap<String, Object>> resp = GetUrlContent.postStringListAndGetJSON(gatherer.applianceInfo.getEngineURL() + "/getDataAtTime?at="+TimeUtils.convertToISO8601String(atTime), "pv", gatherer.remainingPVs);
			if(resp == null) return gatherer;
			logger.debug("Done calling engine for appliance " + gatherer.applianceInfo.getIdentity() + " and got PV count " + ((resp != null) ? resp.size() : 0));
			for(String pvName : resp.keySet()) {
				if(!gatherer.pvValues.containsKey(pvName)) {
					gatherer.pvValues.put(pvName, resp.get(pvName));
					gatherer.remainingPVs.remove(pvName);
				}
			}
		} catch (IOException ex) {
			logger.error("Exception getting data from the engine", ex);
		}
		return gatherer;
	}
	
	private static Appliance2PVs getDataFromRetrieval(Appliance2PVs gatherer, Timestamp atTime) {
		try {
			if(gatherer.remainingPVs.size() <= 0) return gatherer;
			HashMap<String, HashMap<String, Object>> resp = GetUrlContent.postStringListAndGetJSON(gatherer.applianceInfo.getRetrievalURL() + "/../data/getDataAtTimeForAppliance?at="+TimeUtils.convertToISO8601String(atTime), "pv", gatherer.remainingPVs);
			if(resp == null) return gatherer;
			logger.debug("Done calling retrieval for appliance " + gatherer.applianceInfo.getIdentity() + " and got PV count " + ((resp != null) ? resp.size() : 0));
			for(String pvName : resp.keySet()) {
				if(!gatherer.pvValues.containsKey(pvName)) {
					gatherer.pvValues.put(pvName, resp.get(pvName));
					gatherer.remainingPVs.remove(pvName);
				}
			}
		} catch (IOException ex) {
			logger.error("Exception getting data from the retrieval", ex);
		}
		return gatherer;
	}
	
	private static HashMap<String, HashMap<String, Object>>  getDataFromRemoteArchApplicance(String applianceRetrievalURL, LinkedList<String> remainingPVs, Timestamp atTime) {
		try {
			if(remainingPVs.size() <= 0) { return null; } ;
			HashMap<String, HashMap<String, Object>> resp = GetUrlContent.postStringListAndGetJSON(applianceRetrievalURL + "?at="+TimeUtils.convertToISO8601String(atTime)+"&includeProxies=false", "pv", remainingPVs);
			if(resp == null) return null;
			logger.debug("Done calling remote appliance at " + applianceRetrievalURL + " and got PV count " +  + ((resp != null) ? resp.size() : 0));
			return resp;
		} catch (Throwable t) {
			logger.error("Exception getting data from the remote appliance " + applianceRetrievalURL, t);
		}
		return null;
	}

	
	
	private static <T> CompletableFuture<T>[] toArray(List<CompletableFuture<T>> list) {
		@SuppressWarnings("unchecked")
		CompletableFuture<T>[] futures = list.toArray(new CompletableFuture[0]);
		return futures;
	}
	

	/**
	 * The main getDataAtTime function. Pass in a list of PVs and a time. 
	 * For now, we do not proxy external servers. 
	 * @param req
	 * @param resp
	 * @param configService
	 * @throws ServletException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static void getDataAtTime(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws ServletException, IOException, InterruptedException, ExecutionException {
		PoorMansProfiler pmansProfiler = new PoorMansProfiler();

		LinkedList<String> pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req, configService);
		String timeStr = req.getParameter("at");
		if(timeStr == null) {
			timeStr = TimeUtils.convertToISO8601String(TimeUtils.getCurrentEpochSeconds());
		}
		Timestamp atTime = TimeUtils.convertFromISO8601String(timeStr);
		
		boolean fetchFromExternalAppliances = req.getParameter("includeProxies") != null && Boolean.parseBoolean(req.getParameter("includeProxies"));		

		pmansProfiler.mark("After request params.");		

		HashMap<String, Appliance2PVs> valuesGatherer = new HashMap<String, Appliance2PVs>();
		List<CompletableFuture<Appliance2PVs>> pvBreakdownCalls = new LinkedList<CompletableFuture<Appliance2PVs>>();
		for(ApplianceInfo applianceInfo : configService.getAppliancesInCluster()) {
			Appliance2PVs gatherer = new Appliance2PVs(applianceInfo);
			valuesGatherer.put(applianceInfo.getIdentity(), gatherer);
			try {
				pvBreakdownCalls.add(CompletableFuture.supplyAsync(() -> { return getPVSFromAppliance(gatherer, pvNames); } ));
			} catch(Throwable t) {
				logger.error("Exception adding completable future", t);
			}
		}
		
		CompletableFuture.allOf(toArray(pvBreakdownCalls)).join();
		pmansProfiler.mark("After filtering calls.");
				
		List<CompletableFuture<Appliance2PVs>> engineCalls = new LinkedList<CompletableFuture<Appliance2PVs>>();
		for(ApplianceInfo applianceInfo : configService.getAppliancesInCluster()) {
			try {
				engineCalls.add(CompletableFuture.supplyAsync(() -> { return getDataFromEngine(valuesGatherer.get(applianceInfo.getIdentity()), atTime); } ));
			} catch(Throwable t) {
				logger.error("Exception adding completable future", t);
			}
		}
		CompletableFuture.allOf(toArray(engineCalls)).join();
		pmansProfiler.mark("After engine calls.");

		
		List<CompletableFuture<Appliance2PVs>> retrievalCalls = new LinkedList<CompletableFuture<Appliance2PVs>>();
		for(ApplianceInfo applianceInfo : configService.getAppliancesInCluster()) {
			try {
				retrievalCalls.add(CompletableFuture.supplyAsync(() -> { return getDataFromRetrieval(valuesGatherer.get(applianceInfo.getIdentity()), atTime); } ));
			} catch(Throwable t) {
				logger.error("Exception adding completable future", t);
			}
		}
		CompletableFuture.allOf(toArray(retrievalCalls)).join();
		pmansProfiler.mark("After retrieval calls.");
		
		HashMap<String, HashMap<String, Object>> ret = new HashMap<String, HashMap<String, Object>>();
		for(CompletableFuture<Appliance2PVs> retcl : retrievalCalls) {
			Appliance2PVs a2pv = retcl.get();
			ret.putAll(a2pv.pvValues);
		}
		
		if(fetchFromExternalAppliances) { 
			Map<String, String> externalServers = configService.getExternalArchiverDataServers();
			if(externalServers != null) {
				HashSet<String> pvsForExternalServers = new HashSet<String>(pvNames);
				pvsForExternalServers.removeAll(ret.keySet());
				if(pvsForExternalServers.size() > 0) {
					List<CompletableFuture<HashMap<String, HashMap<String, Object>>>> proxyCalls = new LinkedList<CompletableFuture<HashMap<String, HashMap<String, Object>>>>();				
					for(String serverUrl : externalServers.keySet()) { 
						String index = externalServers.get(serverUrl);
						if(index.equals("pbraw")) { 
							logger.debug("Adding external EPICS Archiver Appliance " + serverUrl);
							proxyCalls.add(CompletableFuture.supplyAsync(() -> { return getDataFromRemoteArchApplicance(serverUrl + "/data/getDataAtTime", new LinkedList<String>(pvsForExternalServers), atTime); } ));
						}
					}
					CompletableFuture.allOf(toArray(proxyCalls)).join();				
					pmansProfiler.mark("After calls to remote appliances.");
					for(CompletableFuture<HashMap<String, HashMap<String, Object>>> proxyCall : proxyCalls) {
						HashMap<String, HashMap<String, Object>> res = proxyCall.get();
						if(res != null && res.size() > 0) {
							for(String proxyPv : res.keySet()) { 
								if(!ret.containsKey(proxyPv)) {
									logger.debug("Adding data for PV from external server " + proxyPv);
									ret.put(proxyPv, res.get(proxyPv));
								}
							}
						}
					}
				} else {
					logger.debug("Not calling external servers as there are no PV's left in the request.");
				}
			}
		}
		
		try(PrintWriter out = resp.getWriter()) {
			JSONValue.writeJSONString(ret, out);
		} catch (Exception ex) {
			logger.error("Exception getting data", ex);
		}
		
		logger.info("Retrieval time for " + pvNames.size() + " PVs at " + timeStr + pmansProfiler.toString());

	}
	
	static class PVWithData {
		String pvName;
		HashMap<String, Object> sample;
		public PVWithData(String pvName, HashMap<String, Object> sample) {
			this.pvName = pvName;
			this.sample = sample;
		}
		
	}
	
	
	/**
	 * Async method for getting data for a pv from its list of stores.
	 * Walk thru the store till you find the closest sample before the requested time. 
	 * @param pvName
	 * @param atTime
	 * @param configService
	 * @return
	 */
	private static PVWithData getDataAtTimeForPVFromStores(String pvName, Timestamp atTime, ConfigService configService) {
		String nameFromUser = pvName;
		Timestamp startTime = atTime;
		String fieldName = PVNames.getFieldName(pvName);
		
		PVTypeInfo typeInfo = PVNames.determineAppropriatePVTypeInfo(pvName, configService);
		if(typeInfo == null) return null;
		if(!typeInfo.getApplianceIdentity().equals(configService.getMyApplianceInfo().getIdentity())) return null;

		pvName = typeInfo.getPvName();
		PostProcessor postProcessor = new DefaultRawPostProcessor();
		if(fieldName != null && Arrays.asList(typeInfo.getArchiveFields()).contains(fieldName)) {
			startTime = TimeUtils.minusDays(atTime, 1); // We typically write out embedded fields once a day; so go back a day to catch any changes if present.
			postProcessor = new ExtraFieldsPostProcessor(fieldName);
		}

		
		
		// There is a separate bulk call for the engine; so we can skip the engine. 
		// Go thru the stores in order...
		try {
			DBRTimeEvent potentialEvent = null, dEv = null;
			for(String store : typeInfo.getDataStores()) {
				StoragePlugin storagePlugin = StoragePluginURLParser.parseStoragePlugin(store, configService);
				try(BasicContext context = new BasicContext()) {
					List<Callable<EventStream>> streams = storagePlugin.getDataForPV(context, pvName, startTime, atTime, postProcessor);
					if(streams != null) {
						for(Callable<EventStream> stcl : streams) {
							EventStream stream = stcl.call();
							for(Event e : stream) {
								dEv = (DBRTimeEvent) e;
								if(dEv.getEventTimeStamp().before(atTime) || dEv.getEventTimeStamp().equals(atTime)) {
									if(potentialEvent != null) {
										if(dEv.getEventTimeStamp().after(potentialEvent.getEventTimeStamp())) {
											potentialEvent = (DBRTimeEvent) dEv.makeClone();							
										}
									} else {
										potentialEvent = dEv;
									}
								} else {
									if(potentialEvent != null) {
										HashMap<String, Object> evnt = new HashMap<String, Object>();
										evnt.put("secs", potentialEvent.getEpochSeconds());
										evnt.put("nanos", potentialEvent.getEventTimeStamp().getNanos());
										evnt.put("severity", potentialEvent.getSeverity());
										evnt.put("status", potentialEvent.getStatus());
										evnt.put("val", JSONValue.parse(potentialEvent.getSampleValue().toJSONString()));
										return new PVWithData(nameFromUser, evnt);
									}
								}
							}
						}
					} else {
						logger.warn("No eventstream when looking for point data for PV " + pvName);
					}
				}
			}
			if(potentialEvent != null) {
				HashMap<String, Object> evnt = new HashMap<String, Object>();
				evnt.put("secs", potentialEvent.getEpochSeconds());
				evnt.put("nanos", potentialEvent.getEventTimeStamp().getNanos());
				evnt.put("severity", potentialEvent.getSeverity());
				evnt.put("status", potentialEvent.getStatus());
				evnt.put("val", JSONValue.parse(potentialEvent.getSampleValue().toJSONString()));
				return new PVWithData(nameFromUser, evnt);
			}
		} catch(Exception ex) {
			logger.error("Getting data at time for PV " + pvName, ex);
		}
		
		return null;
	}
	
	/**
	 * Get data at a specified time from the data stores for the specified set of PV's.
	 * This only returns data for those PV's that are on this appliance.
	 */
	public static void getDataAtTimeForAppliance(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws ServletException, IOException, InterruptedException, ExecutionException {
		LinkedList<String> pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req, configService);
		String timeStr = req.getParameter("at");
		if(timeStr == null) {
			timeStr = TimeUtils.convertToISO8601String(TimeUtils.getCurrentEpochSeconds());
		}
		Timestamp atTime = TimeUtils.convertFromISO8601String(timeStr);
		
		logger.debug("Getting data from instance for " + pvNames.size() + " PVs at " + TimeUtils.convertToHumanReadableString(atTime));

		List<CompletableFuture<PVWithData>> retrievalCalls = new LinkedList<CompletableFuture<PVWithData>>();
		for(String pvName : pvNames) {
			retrievalCalls.add(CompletableFuture.supplyAsync(() -> { return getDataAtTimeForPVFromStores(pvName, atTime, configService); }));
		}
		
		CompletableFuture.allOf(toArray(retrievalCalls)).join();
		HashMap<String, HashMap<String, Object>> ret = new HashMap<String, HashMap<String, Object>>();
		for(CompletableFuture<PVWithData> res : retrievalCalls) {
			PVWithData pd = res.get();
			if(pd != null) {
				ret.put(pd.pvName, pd.sample);
			}
		}
		
		try(PrintWriter out = resp.getWriter()) {
				JSONValue.writeJSONString(ret, out);
		}
	}
}
