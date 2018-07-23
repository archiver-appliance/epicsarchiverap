/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;


import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PoorMansProfiler;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ChannelArchiverDataServerPVInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigService.STARTUP_SEQUENCE;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.mgmt.bpl.PVsMatchingParameter;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.retrieval.mimeresponses.FlxXMLResponse;
import org.epics.archiverappliance.retrieval.mimeresponses.JPlotResponse;
import org.epics.archiverappliance.retrieval.mimeresponses.JSONResponse;
import org.epics.archiverappliance.retrieval.mimeresponses.MatlabResponse;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.archiverappliance.retrieval.mimeresponses.PBRAWResponse;
import org.epics.archiverappliance.retrieval.mimeresponses.QWResponse;
import org.epics.archiverappliance.retrieval.mimeresponses.SVGResponse;
import org.epics.archiverappliance.retrieval.mimeresponses.SinglePVCSVResponse;
import org.epics.archiverappliance.retrieval.mimeresponses.TextResponse;
import org.epics.archiverappliance.retrieval.postprocessors.AfterAllStreams;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.ExtraFieldsPostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.FirstSamplePP;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadExecutorService;
import org.epics.archiverappliance.utils.simulation.SimulationEvent;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo.Builder;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

/**
 * Main servlet for retrieval of data.
 * All data retrieval is funneled thru here.
 * @author mshankar
 *
 */
@SuppressWarnings("serial")
public class DataRetrievalServlet  extends HttpServlet {
	public static final int SERIAL_PARALLEL_MEMORY_CUTOFF_MB = 60;
	private static final String ARCH_APPL_PING_PV = "ArchApplPingPV";
	private static Logger logger = Logger.getLogger(DataRetrievalServlet.class.getName());
	static class MimeMappingInfo {
		Class<? extends MimeResponse> mimeresponseClass;
		String contentType;
		public MimeMappingInfo(Class<? extends MimeResponse> mimeresponseClass, String contentType) {
			super();
			this.mimeresponseClass = mimeresponseClass;
			this.contentType = contentType;
		}
	}
	private static HashMap<String, MimeMappingInfo> mimeresponses = new HashMap<String, MimeMappingInfo>();
	static {
		mimeresponses.put("raw", new MimeMappingInfo(PBRAWResponse.class, "application/x-protobuf"));
		mimeresponses.put("svg", new MimeMappingInfo(SVGResponse.class, "image/svg+xml"));
		mimeresponses.put("json", new MimeMappingInfo(JSONResponse.class, "application/json"));
		mimeresponses.put("qw", new MimeMappingInfo(QWResponse.class, "application/json"));
		mimeresponses.put("jplot", new MimeMappingInfo(JPlotResponse.class, "application/json"));
		mimeresponses.put("csv", new MimeMappingInfo(SinglePVCSVResponse.class, "text/csv"));
		mimeresponses.put("flx", new MimeMappingInfo(FlxXMLResponse.class, "text/xml"));
		mimeresponses.put("txt", new MimeMappingInfo(TextResponse.class, "text/plain"));
		mimeresponses.put("mat", new MimeMappingInfo(MatlabResponse.class, "application/matlab"));
	}
	
	
	private ConfigService configService = null;

	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
		String[] pathnameSplit = req.getPathInfo().split("/");
		String requestName = (pathnameSplit[pathnameSplit.length - 1].split("\\."))[0];
		
		if (requestName.equals("getData")) {
			logger.info("User requesting data for single PV");
			doGetSinglePV(req, resp);
		} else if (requestName.equals("getDataForPVs")) {
			logger.info("User requesting data for multiple PVs");
			doGetMultiPV(req, resp);
		} else {
			String msg = "\"" + requestName + "\" is not a valid API method.";
			resp.setHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, msg);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
		}
		
		return;		
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
		String[] pathnameSplit = req.getPathInfo().split("/");
		String requestName = (pathnameSplit[pathnameSplit.length - 1].split("\\."))[0];
		
		if (requestName.equals("getDataForPVs")) {
			logger.info("User requesting data for multiple PVs");
			doGetMultiPV(req, resp);
		} else {
			String msg = "\"" + requestName + "\" is not a valid API method.";
			resp.setHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, msg);
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
		}
		return;
	}
	
	private void doGetSinglePV(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
		PoorMansProfiler pmansProfiler = new PoorMansProfiler();
		String pvName = req.getParameter("pv");
		
		if(configService.getStartupState() != STARTUP_SEQUENCE.STARTUP_COMPLETE) { 
			String msg = "Cannot process data retrieval requests for PV " + pvName + " until the appliance has completely started up.";
			logger.error(msg);
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, msg);
			return;
		}
		
		

		
		String startTimeStr = req.getParameter("from"); 
		String endTimeStr = req.getParameter("to");
		boolean useReduced = false;
		String useReducedStr = req.getParameter("usereduced");
		if(useReducedStr != null && !useReducedStr.equals("")) {
			try {
				useReduced = Boolean.parseBoolean(useReducedStr);
			} catch(Exception ex) {
				logger.error("Exception parsing usereduced", ex);
				useReduced = false;
			}
		}
		String extension = req.getPathInfo().split("\\.")[1];
		logger.info("Mime is " + extension);
		
		boolean useChunkedEncoding = true;
		String doNotChunkStr = req.getParameter("donotchunk");
		if(doNotChunkStr != null && !doNotChunkStr.equals("false")) { 
			logger.info("Turning off HTTP chunked encoding");
			useChunkedEncoding = false;
		}
		
		boolean fetchLatestMetadata = false;
		String fetchLatestMetadataStr = req.getParameter("fetchLatestMetadata");
		if(fetchLatestMetadataStr != null && fetchLatestMetadataStr.equals("true")) { 
			logger.info("Adding a call to the engine to fetch the latest metadata");
			fetchLatestMetadata = true;
		}

		// For data retrieval we need a PV info. However, in case of PV's that have long since retired, we may not want to have PVTypeInfo's in the system.
		// So, we support a template PV that lays out the data sources.
		// During retrieval, you can pass in the PV as a template and we'll clone this and make a temporary copy.
		String retiredPVTemplate = req.getParameter("retiredPVTemplate");
		
		
		if(pvName == null) {
			String msg = "PV name is null.";
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
			return;
		}

		if(pvName.equals(ARCH_APPL_PING_PV)) {
			logger.debug("Processing ping PV - this is used to validate the connection with the client.");
			processPingPV(req, resp);
			return;
		}
		
		if(pvName.endsWith(".VAL")) { 
			int len = pvName.length();
			pvName = pvName.substring(0, len-4);
			logger.info("Removing .VAL from pvName for request giving " + pvName);
		}

		// ISO datetimes are of the form "2011-02-02T08:00:00.000Z"
		Timestamp end = TimeUtils.plusHours(TimeUtils.now(), 1);
		if(endTimeStr != null) {
			try { 
				end = TimeUtils.convertFromISO8601String(endTimeStr);
			} catch(IllegalArgumentException ex) {
				try { 
					end = TimeUtils.convertFromDateTimeStringWithOffset(endTimeStr);
				} catch(IllegalArgumentException ex2) { 
					String msg = "Cannot parse time" + endTimeStr;
					logger.warn(msg, ex2);
					resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
					resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
					return;
				}
			}
		}
		
		// We get one day by default
		Timestamp start = TimeUtils.minusDays(end, 1);
		if(startTimeStr != null) {
			try { 
				start = TimeUtils.convertFromISO8601String(startTimeStr);
			} catch(IllegalArgumentException ex) {
				try { 
					start = TimeUtils.convertFromDateTimeStringWithOffset(startTimeStr);
				} catch(IllegalArgumentException ex2) { 
					String msg = "Cannot parse time " + startTimeStr;
					logger.warn(msg, ex2);
					resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
					resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
					return;
				}
			}
		}
		
		if(end.before(start)) {
			String msg = "For request, end " + end.toString() + " is before start " + start.toString() + " for pv " + pvName;
			logger.error(msg);
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		LinkedList<TimeSpan> requestTimes = new LinkedList<TimeSpan>();
		
		// We can specify a list of time stamp pairs using the optional timeranges parameter
		String timeRangesStr = req.getParameter("timeranges");
		if(timeRangesStr != null) { 
			boolean continueWithRequest = parseTimeRanges(resp, pvName, requestTimes, timeRangesStr);
			if(!continueWithRequest) {
				// Cannot parse the time ranges properly; we so abort the request.
				return;
			}
			
			// Override the start and the end so that the mergededup consumer works correctly.
			start = requestTimes.getFirst().getStartTime();
			end = requestTimes.getLast().getEndTime();
			
		} else { 
			requestTimes.add(new TimeSpan(start, end));			
		}

		assert(requestTimes.size() > 0);
		

		
		String postProcessorUserArg = req.getParameter("pp");
		if(pvName.contains("(")) {
			if(!pvName.contains(")")) {
				logger.error("Unbalanced paran " + pvName);
				resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
				return;
			}
			String[] components = pvName.split("[(,)]");
			postProcessorUserArg = components[0];
			pvName = components[1];
			if(components.length > 2) {
				for(int i = 2; i < components.length; i++) {
					postProcessorUserArg = postProcessorUserArg + "_" + components[i];
				}
			}
			logger.info("After parsing the function call syntax pvName is " + pvName + " and postProcessorUserArg is " + postProcessorUserArg);
		}
		
		PostProcessor postProcessor = PostProcessors.findPostProcessor(postProcessorUserArg);


		PVTypeInfo typeInfo  = PVNames.determineAppropriatePVTypeInfo(pvName, configService);
		pmansProfiler.mark("After PVTypeInfo");
		
		if(typeInfo == null && RetrievalState.includeExternalServers(req)) {
			logger.debug("Checking to see if pv " + pvName + " is served by a external Archiver Server");
			typeInfo = checkIfPVisServedByExternalServer(pvName, start, end, req, resp, useChunkedEncoding);
		}
		
		
		if(typeInfo == null) {
			if(resp.isCommitted()) { 
				logger.debug("Proxied the data thru an external server for PV " + pvName);
				return;
			}
		}
			
		if(typeInfo == null) {
			if(retiredPVTemplate != null) {
				PVTypeInfo templateTypeInfo = PVNames.determineAppropriatePVTypeInfo(retiredPVTemplate, configService);
				if(templateTypeInfo != null) { 
					typeInfo = new PVTypeInfo(pvName, templateTypeInfo);
					typeInfo.setPaused(true);
					typeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
					// Somehow tell the code downstream that this is a fake typeInfo.
					typeInfo.setSamplingMethod(SamplingMethod.DONT_ARCHIVE);
					logger.debug("Using a template PV for " + pvName + " Need to determine the actual DBR type.");
					setActualDBRTypeFromData(pvName, typeInfo, configService);
				}
			}
		}
		
		if(typeInfo == null) { 
			logger.error("Unable to find typeinfo for pv " + pvName);
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

		if(postProcessor == null) {
			if(useReduced) {
				String defaultPPClassName = configService.getInstallationProperties().getProperty("org.epics.archiverappliance.retrieval.DefaultUseReducedPostProcessor", FirstSamplePP.class.getName());
				logger.debug("Using the default usereduced preprocessor " + defaultPPClassName);
				try {
					postProcessor = (PostProcessor) Class.forName(defaultPPClassName).newInstance();
				} catch(Exception ex) {
					logger.error("Exception constructing new instance of post processor " + defaultPPClassName, ex);
					postProcessor = null;
				}
			}
		}

		if(postProcessor == null) {
			logger.debug("Using the default raw preprocessor");
			postProcessor = new DefaultRawPostProcessor();
		}
		
		ApplianceInfo applianceForPV = configService.getApplianceForPV(pvName);
		if(applianceForPV == null) {
			// TypeInfo cannot be null here...
			assert(typeInfo != null);
			applianceForPV = configService.getAppliance(typeInfo.getApplianceIdentity());
		}
		
		if(!applianceForPV.equals(configService.getMyApplianceInfo())) {
			// Data for pv is elsewhere. Proxy/redirect and return.
			proxyRetrievalRequest(req, resp, pvName, useChunkedEncoding, applianceForPV.getRetrievalURL()  + "/../data" );
			return;
		}

		pmansProfiler.mark("After Appliance Info");
		
		String pvNameFromRequest = pvName;
		
		String fieldName = PVNames.getFieldName(pvName);
		if(fieldName != null && !fieldName.equals("") && typeInfo.checkIfFieldAlreadySepcified(fieldName)) {
			logger.debug("We reset the pvName " + pvName + " to one from the typeinfo " + typeInfo.getPvName() + " as that determines the name of the stream. Also using ExtraFieldsPostProcessor");
			pvName = typeInfo.getPvName();
			postProcessor = new ExtraFieldsPostProcessor(fieldName);
		}
		
		try {
			// Postprocessors get their mandatory arguments from the request.
			// If user does not pass in the expected request, throw an exception.
			postProcessor.initialize(postProcessorUserArg, pvName);
		} catch (Exception ex) {
			logger.error("Postprocessor threw an exception during initialization for " + pvName, ex);
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		try(BasicContext retrievalContext = new BasicContext(typeInfo.getDBRType(), pvNameFromRequest); 
				MergeDedupConsumer mergeDedupCountingConsumer = createMergeDedupConsumer(resp, extension, useChunkedEncoding);
				RetrievalExecutorResult executorResult = determineExecutorForPostProcessing(pvName, typeInfo, requestTimes, req, postProcessor)
				) {
			HashMap<String, String> engineMetadata = null;
			if(fetchLatestMetadata  && typeInfo.getSamplingMethod() != SamplingMethod.DONT_ARCHIVE) { 
				// Make a call to the engine to fetch the latest metadata; skip external servers, template PVs and the like by checking the sampling method.
				engineMetadata = fetchLatestMedataFromEngine(pvName, applianceForPV);
			}
			

			LinkedList<Future<RetrievalResult>> retrievalResultFutures = resolveAllDataSources(pvName, typeInfo, postProcessor, applianceForPV, retrievalContext, executorResult, req, resp);
			pmansProfiler.mark("After data source resolution");
			

			long s1 = System.currentTimeMillis();
			String currentlyProcessingPV = null;

			List<Future<EventStream>> eventStreamFutures = getEventStreamFuturesFromRetrievalResults(executorResult, retrievalResultFutures);

			logger.debug("Done with the RetrievalResult's; moving onto the individual event stream from each source for " + pvName);
			pmansProfiler.mark("After retrieval results");

			for(Future<EventStream> future : eventStreamFutures) {
				EventStreamDesc sourceDesc = null;
				try(EventStream eventStream = future.get()) {
					sourceDesc = null; // Reset it for each loop iteration.
					sourceDesc = eventStream.getDescription();
					if(sourceDesc == null) {
						logger.warn("Skipping event stream without a desc for pv " + pvName);
						continue;
					}

					logger.debug("Processing event stream for pv " + pvName + " from source " + ((eventStream.getDescription() != null) ? eventStream.getDescription().getSource() : " unknown"));


					try {
						mergeTypeInfo(typeInfo, sourceDesc, engineMetadata);
					} catch(MismatchedDBRTypeException mex) {
						logger.error(mex.getMessage(), mex);
						continue;
					} 

					if(currentlyProcessingPV == null || !currentlyProcessingPV.equals(pvName)) {
						logger.debug("Switching to new PV " + pvName + " In some mime responses we insert special headers at the beginning of the response. Calling the hook for that");
						currentlyProcessingPV = pvName;
						mergeDedupCountingConsumer.processingPV(currentlyProcessingPV, start, end, (eventStream != null) ? sourceDesc : null);
					}


					try {
						// If the postProcessor does not have a consolidated event stream, we send each eventstream across as we encounter it.
						// Else we send the consolidatedEventStream down below.
						if(!(postProcessor instanceof PostProcessorWithConsolidatedEventStream)) { 
							mergeDedupCountingConsumer.consumeEventStream(eventStream);
							resp.flushBuffer();
						}
					} catch(Exception ex) {
						if(ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
							// We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
							logger.debug("Exception when consuming and flushing data from " + sourceDesc.getSource(), ex);
						} else { 
							logger.error("Exception when consuming and flushing data from " + sourceDesc.getSource() + "-->" + ex.toString(), ex);
						}
					}
					pmansProfiler.mark("After event stream " + eventStream.getDescription().getSource());
				} catch(Exception ex) { 
					if(ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
						// We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
						logger.debug("Exception when consuming and flushing data from " + (sourceDesc != null ? sourceDesc.getSource() : "N/A"), ex);
					} else { 
						logger.error("Exception when consuming and flushing data from " + (sourceDesc != null ? sourceDesc.getSource() : "N/A") + "-->" + ex.toString(), ex);
					}
				}
			}
			
			if(postProcessor instanceof PostProcessorWithConsolidatedEventStream) { 
				try(EventStream eventStream = ((PostProcessorWithConsolidatedEventStream) postProcessor).getConsolidatedEventStream()) {
					EventStreamDesc sourceDesc = eventStream.getDescription();
					if(sourceDesc == null) {
						logger.error("Skipping event stream without a desc for pv " + pvName + " and post processor " + postProcessor.getExtension());
					} else { 
						mergeDedupCountingConsumer.consumeEventStream(eventStream);
						resp.flushBuffer();
					}
				}
			}

			// If the postProcessor needs to send final data across, give it a chance now...
			if(postProcessor instanceof AfterAllStreams) {
				EventStream finalEventStream = ((AfterAllStreams)postProcessor).anyFinalData();
				if(finalEventStream != null) { 
					mergeDedupCountingConsumer.consumeEventStream(finalEventStream);
					resp.flushBuffer();
				}
			}
			
			pmansProfiler.mark("After writing all eventstreams to response");

			long s2 = System.currentTimeMillis();
			logger.info("For the complete request, found a total of " + mergeDedupCountingConsumer.totalEventsForAllPVs + " in " + (s2-s1) + "(ms)" 
					+ " skipping " + mergeDedupCountingConsumer.skippedEventsForAllPVs + " events"
					+ " deduping involved " + mergeDedupCountingConsumer.comparedEventsForAllPVs + " compares.");
		} catch(Exception ex) {
			if(ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
				// We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
				logger.debug("Exception when retrieving data ", ex);
			} else { 
				logger.error("Exception when retrieving data " + "-->" + ex.toString(), ex);
			}
		}
		pmansProfiler.mark("After all closes and flushing all buffers");
		
		// Till we determine all the if conditions where we log this, we log sparingly..
		if(pmansProfiler.totalTimeMS() > 5000) { 
			logger.error("Retrieval time for " + pvName + " from " + startTimeStr + " to " + endTimeStr + pmansProfiler.toString());
		}
	}
	
	private void doGetMultiPV(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
		PoorMansProfiler pmansProfiler = new PoorMansProfiler();
		
		// Gets the list of PVs specified by the `pv` parameter
		// String arrays might be inefficient for retrieval. In any case, they are sorted, which is essential later on.
		List<String> pvNames = null;
		if(req.getMethod().equals("POST")) { 
			pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req, configService);
		} else { 
			pvNames = Arrays.asList(req.getParameterValues("pv"));
		}
	
		// Ensuring that the AA has finished starting up before requests are accepted.
		if(configService.getStartupState() != STARTUP_SEQUENCE.STARTUP_COMPLETE) { 
			String msg = "Cannot process data retrieval requests for specified PVs (" + StringUtils.join(pvNames, ", ") 
					+ ") until the appliance has completely started up.";
			logger.error(msg);
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, msg);
			return;
		}
		
		// Getting various fields from arguments
		String startTimeStr = req.getParameter("from"); 
		String endTimeStr = req.getParameter("to");
		boolean useReduced = false;
		String useReducedStr = req.getParameter("usereduced");
		if(useReducedStr != null && !useReducedStr.equals("")) {
			try {
				useReduced = Boolean.parseBoolean(useReducedStr);
			} catch(Exception ex) {
				logger.error("Exception parsing usereduced", ex);
				useReduced = false;
			}
		}
		
		// Getting MIME type
		String extension = req.getPathInfo().split("\\.")[1];
		logger.info("Mime is " + extension);
		
		if (!extension.equals("json") && !extension.equals("raw") && !extension.equals("jplot") && !extension.equals("qw")) {
			String msg = "Mime type " + extension + " is not supported. Please use \"json\", \"jplot\" or \"raw\".";
			resp.setHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
			return;
		}
		
		boolean useChunkedEncoding = true;
		String doNotChunkStr = req.getParameter("donotchunk");
		if(doNotChunkStr != null && !doNotChunkStr.equals("false")) { 
			logger.info("Turning off HTTP chunked encoding");
			useChunkedEncoding = false;
		}
		
		boolean fetchLatestMetadata = false;
		String fetchLatestMetadataStr = req.getParameter("fetchLatestMetadata");
		if(fetchLatestMetadataStr != null && fetchLatestMetadataStr.equals("true")) { 
			logger.info("Adding a call to the engine to fetch the latest metadata");
			fetchLatestMetadata = true;
		}

		// For data retrieval we need a PV info. However, in case of PV's that have long since retired, we may not want to have PVTypeInfo's in the system.
		// So, we support a template PV that lays out the data sources.
		// During retrieval, you can pass in the PV as a template and we'll clone this and make a temporary copy.
		String retiredPVTemplate = req.getParameter("retiredPVTemplate");
		
		// Goes through given PVs and returns bad request error.
		int nullPVs = 0;
		for (String pvName : pvNames) {
			if(pvName == null) {
				nullPVs++;
			}
			if (nullPVs > 0) {
				logger.warn("Some PVs are null in the request.");
				resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
				return;
			}
		}
		
		if(pvNames.toString().matches("^.*" + ARCH_APPL_PING_PV + ".*$")) {
			logger.debug("Processing ping PV - this is used to validate the connection with the client.");
			processPingPV(req, resp);
			return;
		}
		
		for (String pvName : pvNames) if (pvName.endsWith(".VAL")) { 
			int len = pvName.length();
			pvName = pvName.substring(0, len-4);
			logger.info("Removing .VAL from pvName for request giving " + pvName);
		}

		// ISO datetimes are of the form "2011-02-02T08:00:00.000Z"
		Timestamp end = TimeUtils.plusHours(TimeUtils.now(), 1);
		if(endTimeStr != null) {
			try { 
				end = TimeUtils.convertFromISO8601String(endTimeStr);
			} catch(IllegalArgumentException ex) {
				try { 
					end = TimeUtils.convertFromDateTimeStringWithOffset(endTimeStr);
				} catch(IllegalArgumentException ex2) { 
					String msg = "Cannot parse time " + endTimeStr;
					logger.warn(msg, ex2);
					resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
					resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
					return;
				}
			}
		}
		
		// We get one day by default
		Timestamp start = TimeUtils.minusDays(end, 1);
		if(startTimeStr != null) {
			try { 
				start = TimeUtils.convertFromISO8601String(startTimeStr);
			} catch(IllegalArgumentException ex) {
				try { 
					start = TimeUtils.convertFromDateTimeStringWithOffset(startTimeStr);
				} catch(IllegalArgumentException ex2) { 
					String msg = "Cannot parse time " + startTimeStr;
					logger.warn(msg, ex2);
					resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
					resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
					return;
				}
			}
		}
		
		if(end.before(start)) {
			String msg = "For request, end " + end.toString() + " is before start " + start.toString() 
					+ " for pvs " + StringUtils.join(pvNames, ", ");
			logger.error(msg);
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
			return;
		}
		
		LinkedList<TimeSpan> requestTimes = new LinkedList<TimeSpan>();
		
		// We can specify a list of time stamp pairs using the optional timeranges parameter
		String timeRangesStr = req.getParameter("timeranges");
		if(timeRangesStr != null) { 
			boolean continueWithRequest = parseTimeRanges(resp, "[" + StringUtils.join(pvNames, ", ") + "]", requestTimes, timeRangesStr);
			if(!continueWithRequest) {
				// Cannot parse the time ranges properly; we so abort the request.
				String msg = "The specified time ranges could not be processed appropriately. Aborting.";
				logger.info(msg);
				resp.setHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
				return;
			}
			
			// Override the start and the end so that the mergededup consumer works correctly.
			start = requestTimes.getFirst().getStartTime();
			end = requestTimes.getLast().getEndTime();
			
		} else { 
			requestTimes.add(new TimeSpan(start, end));			
		}

		assert(requestTimes.size() > 0);
		
		// Get a post processor for each PV specified in pvNames
		// If PV in the form <pp>(<pv>), process it
		String postProcessorUserArg = req.getParameter("pp");
		List<String> postProcessorUserArgs = new ArrayList<>(pvNames.size());
		List<PostProcessor> postProcessors = new ArrayList<>(pvNames.size());
		for (int i = 0; i < pvNames.size(); i++) {
			postProcessorUserArgs.add(postProcessorUserArg);
			
			if (pvNames.get(i).contains("(")) {
				if(!pvNames.get(i).contains(")")) {
					String msg = "Unbalanced paren " + pvNames.get(i);
					logger.error(msg);
					resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
					resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
					return;
				}
				String[] components = pvNames.get(i).split("[(,)]");
				postProcessorUserArg = components[0];
				postProcessorUserArgs.set(i, postProcessorUserArg);
				pvNames.set(i, components[1]);
				if(components.length > 2) {
					for(int j = 2; j < components.length; j++) {
						postProcessorUserArgs.set(i, postProcessorUserArgs.get(i) + "_" + components[j]);
					}
				}
				logger.info("After parsing the function call syntax pvName is " + pvNames.get(i) + " and postProcessorUserArg is " + postProcessorUserArg);
			}
			postProcessors.add(PostProcessors.findPostProcessor(postProcessorUserArg));
		}
		
		List<PVTypeInfo> typeInfos  = new ArrayList<PVTypeInfo>(pvNames.size());
		for (int i = 0; i < pvNames.size(); i++) {
			typeInfos.add(PVNames.determineAppropriatePVTypeInfo(pvNames.get(i), configService));
		}
		pmansProfiler.mark("After PVTypeInfo");
		
		for (int i = 0; i < pvNames.size(); i++)
			if(typeInfos.get(i) == null && RetrievalState.includeExternalServers(req)) {
			logger.debug("Checking to see if pv " + pvNames.get(i) + " is served by a external Archiver Server");
			typeInfos.set(i, checkIfPVisServedByExternalServer(pvNames.get(i), start, end, req, resp, useChunkedEncoding));
		}
		
		for (int i = 0; i < pvNames.size(); i++) {
			if(typeInfos.get(i) == null) {
				if(retiredPVTemplate != null) {
					PVTypeInfo templateTypeInfo = PVNames.determineAppropriatePVTypeInfo(retiredPVTemplate, configService);
					if(templateTypeInfo != null) { 
						typeInfos.set(i, new PVTypeInfo(pvNames.get(i), templateTypeInfo));
						typeInfos.get(i).setPaused(true);
						typeInfos.get(i).setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
						// Somehow tell the code downstream that this is a fake typeInfos.
						typeInfos.get(i).setSamplingMethod(SamplingMethod.DONT_ARCHIVE);
						logger.debug("Using a template PV for " + pvNames.get(i) + " Need to determine the actual DBR type.");
						setActualDBRTypeFromData(pvNames.get(i), typeInfos.get(i), configService);
					}
				}
			}
				
			if (typeInfos.get(i) == null) {
				String msg = "Unable to find typeinfo for pv " + pvNames.get(i);
				logger.error(msg);
				resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
				resp.sendError(HttpServletResponse.SC_NOT_FOUND, msg);
				return;
			}
			
			if (postProcessors.get(i) == null) {
				if(useReduced) {
					String defaultPPClassName = configService.getInstallationProperties()
							.getProperty("org.epics.archiverappliance.retrieval.DefaultUseReducedPostProcessor",
									FirstSamplePP.class.getName());
					logger.debug("Using the default usereduced preprocessor " + defaultPPClassName);
					try {
						postProcessors.set(i, (PostProcessor) Class.forName(defaultPPClassName).newInstance());
					} catch(Exception ex) {
						logger.error("Exception constructing new instance of post processor " + defaultPPClassName, ex);
						postProcessors.set(i, null);
					}
				}
			}
	
			if (postProcessors.get(i) == null) {
				logger.debug("Using the default raw preprocessor");
				postProcessors.set(i, new DefaultRawPostProcessor());
			}
		}
		
		// Get the appliances for each of the PVs
		List<ApplianceInfo> applianceForPVs = new ArrayList<ApplianceInfo>(pvNames.size());
		for (int i = 0; i < pvNames.size(); i++) {
			applianceForPVs.add(configService.getApplianceForPV(pvNames.get(i)));
			if(applianceForPVs.get(i) == null) {
				// TypeInfo cannot be null here...
				assert(typeInfos.get(i) != null);
				applianceForPVs.set(i, configService.getAppliance(typeInfos.get(i).getApplianceIdentity()));
			}
		}
		
		/*
		 * Retrieving the external appliances if the current appliance has not got the PV assigned to it, and
		 * storing the associated information of the PVs in that appliance.
		 */
		Map<String, ArrayList<PVInfoForClusterRetrieval>> applianceToPVs = new HashMap<String, ArrayList<PVInfoForClusterRetrieval>>();
		for (int i = 0; i < pvNames.size(); i++) {
			if (!applianceForPVs.get(i).equals(configService.getMyApplianceInfo())) {
				
				ArrayList<PVInfoForClusterRetrieval> appliancePVs = 
						applianceToPVs.get(applianceForPVs.get(i).getMgmtURL());
				appliancePVs = (appliancePVs == null) ? new ArrayList<>() : appliancePVs;
				PVInfoForClusterRetrieval pvInfoForRetrieval = new PVInfoForClusterRetrieval(pvNames.get(i), typeInfos.get(i), 
						postProcessors.get(i), applianceForPVs.get(i));
				appliancePVs.add(pvInfoForRetrieval);
				applianceToPVs.put(applianceForPVs.get(i).getRetrievalURL(), appliancePVs);
			}
		}

		List<List<Future<EventStream>>> listOfEventStreamFuturesLists = new ArrayList<List<Future<EventStream>>>();
		Set<String> retrievalURLs = applianceToPVs.keySet();
		if (retrievalURLs.size() > 0) {
			// Get list of PVs and redirect them to appropriate appliance to be retrieved.
			String retrievalURL;
			ArrayList<PVInfoForClusterRetrieval> pvInfos;
			while (!((retrievalURL = retrievalURLs.iterator().next()) != null)) {
				// Get array list of PVs for appliance
				pvInfos = applianceToPVs.get(retrievalURL);
				try {
					List<List<Future<EventStream>>> resultFromForeignAppliances
						= retrieveEventStreamFromForeignAppliance(req, resp, pvInfos, requestTimes, 
								useChunkedEncoding, retrievalURL + "/../data/getDataForPVs.raw", start, end);
					listOfEventStreamFuturesLists.addAll(resultFromForeignAppliances);
				} catch (Exception ex) {
					logger.error("Failed to retrieve " + StringUtils.join(pvNames, ", ") + " from " + retrievalURL + ".");
					return;
				}
			}
		}

		pmansProfiler.mark("After Appliance Info");
		
		// Setting post processor for PVs, taking into account whether there is a field in the PV name
		List<String> pvNamesFromRequests = new ArrayList<String>(pvNames.size());
		for (int i = 0; i < pvNames.size(); i++) {
			String pvName = pvNames.get(i);
			pvNamesFromRequests.add(pvName);
			PVTypeInfo typeInfo = typeInfos.get(i);
			postProcessorUserArg = postProcessorUserArgs.get(i);
			
			// If a field is specified in a PV name, it will create a post processor for that
			String fieldName = PVNames.getFieldName(pvName);
			if(fieldName != null && !fieldName.equals("") && !pvName.equals(typeInfo.getPvName())) {
				logger.debug("We reset the pvName " + pvName + " to one from the typeinfo "
						+ typeInfo.getPvName() + " as that determines the name of the stream. "
								+ "Also using ExtraFieldsPostProcessor.");
				pvNames.set(i, typeInfo.getPvName());
				postProcessors.set(i, new ExtraFieldsPostProcessor(fieldName));
			}
		
			try {
				// Postprocessors get their mandatory arguments from the request.
				// If user does not pass in the expected request, throw an exception.
				postProcessors.get(i).initialize(postProcessorUserArg, pvName);
			} catch (Exception ex) {
				String msg = "Postprocessor threw an exception during initialization for " + pvName;
				logger.error(msg, ex);
				resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
				resp.sendError(HttpServletResponse.SC_NOT_FOUND, msg);
				return;
			}
		}
		
		/*
		 * MergeDedupConsumer is what writes PB data in its respective format to the HTML response.
		 * The response, after the MergeDedupConsumer is created, contains the following:
		 * 
		 * 1) The content type for the response.
		 * 2) Any additional headers for the particular MIME response.
		 * 
		 * Additionally, the MergeDedupConsumer instance holds a reference to the output stream
		 * that is used to write to the HTML response. It is stored under the name `os`.
		 */
		MergeDedupConsumer mergeDedupCountingConsumer;
		try {
			mergeDedupCountingConsumer = createMergeDedupConsumer(resp, extension, useChunkedEncoding);
		} catch (ServletException se) {
			String msg = "Exception when retrieving data " + "-->" + se.toString();
			logger.error(msg, se);
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, msg);
			return;
		}

		/* 
		 * BasicContext contains the PV name and the expected return type. Used to access PB files.
		 * RetrievalExecutorResult contains a thread service class and the time spans Presumably, the 
		 * thread service is what retrieves the data, and the BasicContext is the context in which it 
		 * works.
		 */
		List<HashMap<String, String>> engineMetadatas = new ArrayList<HashMap<String, String>>();
		try {
			List<BasicContext> retrievalContexts = new ArrayList<BasicContext>(pvNames.size());
			List<RetrievalExecutorResult> executorResults = new ArrayList<RetrievalExecutorResult>(pvNames.size());
			for (int i = 0; i < pvNames.size(); i++) {
				if(fetchLatestMetadata && typeInfos.get(i).getSamplingMethod() != SamplingMethod.DONT_ARCHIVE) {
					// Make a call to the engine to fetch the latest metadata; skip external servers, template PVs and the like by checking the sampling method.
					engineMetadatas.add(fetchLatestMedataFromEngine(pvNames.get(i), applianceForPVs.get(i)));
				}
				retrievalContexts.add(new BasicContext(typeInfos.get(i).getDBRType(), pvNamesFromRequests.get(i)));
				executorResults.add(determineExecutorForPostProcessing(pvNames.get(i), typeInfos.get(i), requestTimes, req, postProcessors.get(i)));
			}
			
			/*
			 * There are as many Future objects in the eventStreamFutures List as there are periods over 
			 * which to fetch data. Retrieval of data happen here in parallel.
			 */
			List<LinkedList<Future<RetrievalResult>>> listOfRetrievalResultFuturesLists = new ArrayList<LinkedList<Future<RetrievalResult>>>();
			for (int i = 0; i < pvNames.size(); i++) {
				listOfRetrievalResultFuturesLists.add(resolveAllDataSources(pvNames.get(i), typeInfos.get(i), postProcessors.get(i), 
						applianceForPVs.get(i), retrievalContexts.get(i), executorResults.get(i), req, resp));
			}
			pmansProfiler.mark("After data source resolution");

			for (int i = 0; i < pvNames.size(); i++) {
				// Data is retrieved here
				List<Future<EventStream>> eventStreamFutures = getEventStreamFuturesFromRetrievalResults(executorResults.get(i),
						listOfRetrievalResultFuturesLists.get(i));
				listOfEventStreamFuturesLists.add(eventStreamFutures);
			}
			
		} catch(Exception ex) {
			if(ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
				// We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
				logger.debug("Exception when retrieving data ", ex);
			} else { 
				logger.error("Exception when retrieving data " + "-->" + ex.toString(), ex);
			}
		}
		
		long s1 = System.currentTimeMillis();
		String currentlyProcessingPV = null;
		
		/*
		 * The following try bracket goes through each of the streams in the list of event stream futures.
		 * 
		 * It is intended that the process goes through one PV at a time.
		 */
		try {
			for (int i = 0; i < pvNames.size(); i++) {
				List<Future<EventStream>> eventStreamFutures = listOfEventStreamFuturesLists.get(i);
				String pvName = pvNames.get(i);
				PVTypeInfo typeInfo = typeInfos.get(i);
				HashMap<String, String> engineMetadata = fetchLatestMetadata ? engineMetadatas.get(i) : null;
				PostProcessor postProcessor = postProcessors.get(i);
				
				logger.debug("Done with the RetrievalResults; moving onto the individual event stream "
						+ "from each source for " + StringUtils.join(pvNames, ", "));
				pmansProfiler.mark("After retrieval results");
				for(Future<EventStream> future : eventStreamFutures) {
					EventStreamDesc sourceDesc = null;
					
					// Gets the result of a data retrieval
					try (EventStream eventStream = future.get()) {
						sourceDesc = null; // Reset it for each loop iteration.
						sourceDesc = eventStream.getDescription();
						if(sourceDesc == null) {
							logger.warn("Skipping event stream without a desc for pv " + pvName);
							continue;
						}
	
						logger.debug("Processing event stream for pv " + pvName + " from source "
								+ ((eventStream.getDescription() != null) ? eventStream.getDescription().getSource() : " unknown"));
	

						try {
							mergeTypeInfo(typeInfo, sourceDesc, engineMetadata);
						} catch(MismatchedDBRTypeException mex) {
							logger.error(mex.getMessage(), mex);
							continue;
						} 
	
						if(currentlyProcessingPV == null || !currentlyProcessingPV.equals(pvName)) {
							logger.debug("Switching to new PV " + pvName + " In some mime responses we insert "
									+ "special headers at the beginning of the response. Calling the hook for "
									+ "that");
							currentlyProcessingPV = pvName;
							/*
							 * Goes through the PB data stream over a period of time. The relevant MIME response
							 * actually deal with the processing of the PV. `start` and `end` refer to the very
							 * beginning and very end of the time period being retrieved over, regardless of
							 * whether it is divided up or not.
							 */
							mergeDedupCountingConsumer.processingPV(currentlyProcessingPV, start, end, (eventStream != null) ? sourceDesc : null);
						}
	
	
						try {
							// If the postProcessor does not have a consolidated event stream, we send each eventstream across as we encounter it.
							// Else we send the consolidatedEventStream down below.
							if(!(postProcessor instanceof PostProcessorWithConsolidatedEventStream)) {
								/*
								 * The eventStream object contains all the data over the current period.
								 */
								mergeDedupCountingConsumer.consumeEventStream(eventStream);
								resp.flushBuffer();
							}
						} catch(Exception ex) {
							if(ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
								// We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
								logger.debug("Exception when consuming and flushing data from " + sourceDesc.getSource(), ex);
							} else { 
								logger.error("Exception when consuming and flushing data from " + sourceDesc.getSource() + "-->" + ex.toString(), ex);
							}
						}
						pmansProfiler.mark("After event stream " + eventStream.getDescription().getSource());
					} catch(Exception ex) {
						if(ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
							// We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
							logger.debug("Exception when consuming and flushing data from " + (sourceDesc != null ? sourceDesc.getSource() : "N/A"), ex);
						} else { 
							logger.error("Exception when consuming and flushing data from " + (sourceDesc != null ? sourceDesc.getSource() : "N/A") + "-->" + ex.toString(), ex);
						}
					}
				}
				
				// TODO Go through data from other appliances here
					
				if(postProcessor instanceof PostProcessorWithConsolidatedEventStream) { 
					try(EventStream eventStream = ((PostProcessorWithConsolidatedEventStream) postProcessor).getConsolidatedEventStream()) {
						EventStreamDesc sourceDesc = eventStream.getDescription();
						if(sourceDesc == null) {
							logger.error("Skipping event stream without a desc for pv " + pvName + " and post processor " + postProcessor.getExtension());
						} else { 
							mergeDedupCountingConsumer.consumeEventStream(eventStream);
							resp.flushBuffer();
						}
					}
				}
		
				// If the postProcessor needs to send final data across, give it a chance now...
				if(postProcessor instanceof AfterAllStreams) {
					EventStream finalEventStream = ((AfterAllStreams)postProcessor).anyFinalData();
					if(finalEventStream != null) { 
						mergeDedupCountingConsumer.consumeEventStream(finalEventStream);
						resp.flushBuffer();
					}
				}
				
				pmansProfiler.mark("After writing all eventstreams to response");
			}
		} catch(Exception ex) {
			if(ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
				// We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
				logger.debug("Exception when retrieving data ", ex);
			} else { 
				logger.error("Exception when retrieving data " + "-->" + ex.toString(), ex);
			}
		}
		
		long s2 = System.currentTimeMillis();
		logger.info("For the complete request, found a total of " + mergeDedupCountingConsumer.totalEventsForAllPVs + " in " + (s2-s1) + "(ms)" 
				+ " skipping " + mergeDedupCountingConsumer.skippedEventsForAllPVs + " events"
				+ " deduping involved " + mergeDedupCountingConsumer.comparedEventsForAllPVs + " compares.");

		pmansProfiler.mark("After all closes and flushing all buffers");
			
		// Till we determine all the if conditions where we log this, we log sparingly..
		if(pmansProfiler.totalTimeMS()/pvNames.size() > 5000) { 
			logger.error("Retrieval time for " + StringUtils.join(pvNames, ", ") + " from " + startTimeStr + " to " + endTimeStr + ": " + pmansProfiler.toString());
		}
		
		mergeDedupCountingConsumer.close();
	}


	/**
	 * Given a list of retrievalResult futures, we loop thru these; execute them (basically calling the reader getData) and then sumbit the returned callables to the executorResult's executor.
	 * We return a list of eventstream futures.
	 * @param executorResult
	 * @param retrievalResultFutures
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private List<Future<EventStream>> getEventStreamFuturesFromRetrievalResults(RetrievalExecutorResult executorResult, LinkedList<Future<RetrievalResult>> retrievalResultFutures)
			throws InterruptedException, ExecutionException {
		// List containing the result
		List<Future<EventStream>> eventStreamFutures = new LinkedList<Future<EventStream>>();
		
		// Loop thru the retrievalResultFutures one by one in sequence; get all the event streams from the plugins and consolidate them into a sequence of eventStream futures.
		for(Future<RetrievalResult> retrievalResultFuture : retrievalResultFutures) {
			// This call blocks until the future is complete.
			// For now, we use a simple get as opposed to a get with a timeout.
			RetrievalResult retrievalresult = retrievalResultFuture.get();
			if(retrievalresult.hasNoData()) {
				logger.debug("Skipping as we have not data from " + retrievalresult.getRetrievalRequest().getDescription() + " for pv " + retrievalresult.getRetrievalRequest().getPvName());
				continue;
			}
			
			// Process the data retrieval calls.
			List<Callable<EventStream>> callables = retrievalresult.getResultStreams();
			for(Callable<EventStream> wrappedCallable : callables) {
				Future<EventStream> submit = executorResult.executorService.submit(wrappedCallable);
				eventStreamFutures.add(submit);
			}
		}
		return eventStreamFutures;
	}


	/**
	 * Resolve all data sources and submit them to the executor in the executorResult
	 * This returns a list of futures of retrieval results.
	 * @param pvName
	 * @param typeInfo
	 * @param postProcessor
	 * @param applianceForPV
	 * @param retrievalContext
	 * @param executorResult
	 * @param req
	 * @param resp
	 * @return
	 * @throws IOException
	 */
	private LinkedList<Future<RetrievalResult>> resolveAllDataSources(String pvName, PVTypeInfo typeInfo, 
			PostProcessor postProcessor, ApplianceInfo applianceForPV, 
			BasicContext retrievalContext, RetrievalExecutorResult executorResult, 
			HttpServletRequest req, HttpServletResponse resp) throws IOException {
		
		LinkedList<Future<RetrievalResult>> retrievalResultFutures = new LinkedList<Future<RetrievalResult>>();
		
		/*
		 * Gets the object responsible for resolving data sources (e.g., where data is stored
		 * for this appliance.
		 */
		DataSourceResolution datasourceresolver = new DataSourceResolution(configService);
		
		for(TimeSpan timespan : executorResult.requestTimespans) { 
			// Resolve data sources for the given PV and the given time frames
			LinkedList<UnitOfRetrieval> unitsofretrieval = datasourceresolver.resolveDataSources(pvName, timespan.getStartTime(), timespan.getEndTime(), typeInfo, retrievalContext, postProcessor, req, resp, applianceForPV);
			// Submit the units of retrieval to the executor service. This will give us a bunch of Futures.
			for(UnitOfRetrieval unitofretrieval : unitsofretrieval) {
				// unitofretrieval implements a call() method as it extends Callable<?>
				retrievalResultFutures.add(executorResult.executorService.submit(unitofretrieval));
			}
		}
		return retrievalResultFutures;
	}


	/**
	 * Create a merge dedup consumer that will merge/dedup multiple event streams.
	 * This basically makes sure that we are serving up events in monotonically increasing timestamp order.
	 * @param resp
	 * @param extension
	 * @param useChunkedEncoding
	 * @return
	 * @throws ServletException
	 */
	private MergeDedupConsumer createMergeDedupConsumer(HttpServletResponse resp, String extension, boolean useChunkedEncoding) throws ServletException {
		MergeDedupConsumer mergeDedupCountingConsumer = null;
		MimeMappingInfo mimemappinginfo = mimeresponses.get(extension);
		if(mimemappinginfo == null) {
			StringWriter supportedextensions = new StringWriter();
			for(String supportedextension : mimeresponses.keySet()) { supportedextensions.append(supportedextension).append(" "); }
			throw new ServletException("Cannot generate response of mime-type " + extension + ". Supported extensions are " + supportedextensions.toString());
		} else {
			try {
				String ctype = mimeresponses.get(extension).contentType;
				resp.setContentType(ctype);
//				if(useChunkedEncoding) { 
//					resp.addHeader("Transfer-Encoding", "chunked");
//				}
				logger.info("Using " + mimemappinginfo.mimeresponseClass.getName() + " as the mime response sending " + ctype);
				MimeResponse mimeresponse = (MimeResponse) mimemappinginfo.mimeresponseClass.newInstance();
				HashMap<String, String> extraHeaders = mimeresponse.getExtraHeaders();
				if(extraHeaders != null) { 
					for(Entry<String, String> kv : extraHeaders.entrySet()) { 
						resp.addHeader(kv.getKey(), kv.getValue());
					}
				}
				OutputStream os = resp.getOutputStream();
				mergeDedupCountingConsumer = new MergeDedupConsumer(mimeresponse, os);
			} catch(Exception ex) {
				throw new ServletException(ex);
			}
		}
		return mergeDedupCountingConsumer;
	}


	/**
	 * Check to see if the PV is served up by an external server. 
	 * If it is, make a typeInfo up and set the appliance as this appliance.
	 * We need the start time of the request as the ChannelArchiver does not serve up data if the starttime is much later than the last event in the dataset.
	 * For external EPICS Archiver Appliances, we simply proxy the data right away. Use the response isCommited to see if we have already processed the request
	 * @param pvName
	 * @param start
	 * @param req
	 * @param resp
	 * @param useChunkedEncoding
	 * @return
	 * @throws IOException
	 */
	private PVTypeInfo checkIfPVisServedByExternalServer(String pvName, Timestamp start, Timestamp end, HttpServletRequest req, HttpServletResponse resp, boolean useChunkedEncoding) throws IOException {
		PVTypeInfo typeInfo = null;
		// See if external EPICS archiver appliances have this PV.
		Map<String, String> externalServers = configService.getExternalArchiverDataServers();
		if(externalServers != null) { 
			for(String serverUrl : externalServers.keySet()) { 
				String index = externalServers.get(serverUrl);
				if(index.equals("pbraw")) { 
					logger.debug("Asking external EPICS Archiver Appliance " + serverUrl + " if it has data for pv " + pvName);
					JSONObject areWeArchivingPVObj = GetUrlContent.getURLContentAsJSONObject(serverUrl + "/bpl/areWeArchivingPV?pv=" + URLEncoder.encode(pvName, "UTF-8"), false);
					if(areWeArchivingPVObj != null) {
						@SuppressWarnings("unchecked")
						Map<String, String> areWeArchivingPV = (Map<String, String>) areWeArchivingPVObj;
						if(areWeArchivingPV.containsKey("status") && Boolean.parseBoolean(areWeArchivingPV.get("status"))) {
							logger.info("Proxying data retrieval for pv " + pvName + " to " + serverUrl);
							try(BasicContext context = new BasicContext()) {
								StoragePlugin hplg = StoragePluginURLParser.parseStoragePlugin("pbraw://localhost?rawURL=" + serverUrl + "/data/getData.raw&name=ext", configService);
								logger.debug(hplg.getDescription());
								List<Callable<EventStream>> callables = hplg.getDataForPV(context, pvName, TimeUtils.minusHours(end, 1), end, null);
								if(callables == null || callables.isEmpty()) {
									logger.info("No data from remote server " + serverUrl + " for pv " + pvName);
								} else {
									for(Callable<EventStream> callable : callables){
										try(EventStream strm = callable.call()) {
											if(strm != null) {
												Iterator<Event> it = strm.iterator();
												if(it.hasNext()) {
													Event e = it.next();
													ArchDBRTypes dbrType = strm.getDescription().getArchDBRType();
													typeInfo = new PVTypeInfo(pvName, dbrType, !dbrType.isWaveForm(), e.getSampleValue().getElementCount());
													typeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
													// Somehow tell the code downstream that this is a fake typeInfo.
													typeInfo.setSamplingMethod(SamplingMethod.DONT_ARCHIVE);
													typeInfo.setDataStores(new String[] {"pbraw://localhost?rawURL=" + serverUrl + "/data/getData.raw"} );
													logger.debug("Done creating a temporary typeinfo for pv " + pvName);
													return typeInfo;
												}
											} else {
												logger.info("Empty stream from remote server" + serverUrl + " for pv " + pvName);
											}
										} catch(Exception ex) {
											logger.error("Exception trying to determine typeinfo for pv " + pvName + " from external server " + serverUrl, ex);
											typeInfo = null;
										}
									}
								}
							} catch(Exception ex) {
								logger.error("Exception trying to determine typeinfo for pv " + pvName + " from external server " + serverUrl, ex);
								typeInfo = null;
							}
						}
						return null;
					}
				}
			}
		}

		
		List<ChannelArchiverDataServerPVInfo> caServers = configService.getChannelArchiverDataServers(pvName);
		if(caServers != null && !caServers.isEmpty()) {
			try(BasicContext context = new BasicContext()) {
				for(ChannelArchiverDataServerPVInfo caServer : caServers) { 
					logger.debug(pvName + " is being server by " + caServer.toString() + " and typeinfo is null. Trying to make a typeinfo up...");
					List<Callable<EventStream>> callables = caServer.getServerInfo().getPlugin().getDataForPV(context, pvName, TimeUtils.minusHours(start, 1), start, null);
					if(callables != null && !callables.isEmpty()) {
						try(EventStream strm = callables.get(0).call()) {
							if(strm != null) {
								Event e = strm.iterator().next();
								if(e != null) {
									ArchDBRTypes dbrType = strm.getDescription().getArchDBRType();
									typeInfo = new PVTypeInfo(pvName, dbrType, !dbrType.isWaveForm(), e.getSampleValue().getElementCount());
									typeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
									// Somehow tell the code downstream that this is a fake typeInfo.
									typeInfo.setSamplingMethod(SamplingMethod.DONT_ARCHIVE);
									logger.debug("Done creating a temporary typeinfo for pv " + pvName);
									return typeInfo;
								}
							}
						} catch(Exception ex) {
							logger.error("Exception trying to determine typeinfo for pv " + pvName + " from CA " + caServer.toString(), ex);
							typeInfo = null;
						}
					}
				}
			}
			logger.warn("Unable to determine typeinfo from CA for pv " + pvName);
			return typeInfo;
		}
		
		logger.debug("Cannot find the PV anywhere " + pvName);
		return null;
	}


	/**
	 * Merges info from pvTypeTnfo that comes from the config database into the remote description that gets sent over the wire.
	 * @param typeInfo
	 * @param eventDesc
	 * @param engineMetaData - Latest from the engine - could be null
	 * @return
	 * @throws IOException
	 */
	private void mergeTypeInfo(PVTypeInfo typeInfo, EventStreamDesc eventDesc, HashMap<String, String> engineMetaData) throws IOException {
		if(typeInfo != null && eventDesc != null && eventDesc instanceof RemotableEventStreamDesc) {
			logger.debug("Merging typeinfo into remote desc for pv " + eventDesc.getPvName() + " into source " + eventDesc.getSource());
			RemotableEventStreamDesc remoteDesc = (RemotableEventStreamDesc) eventDesc;
			remoteDesc.mergeFrom(typeInfo, engineMetaData);
		}
	}
	
	
	private static void processPingPV(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// resp.addHeader("Transfer-Encoding", "chunked");
		final OutputStream os = resp.getOutputStream();
		try {
			short currYear = TimeUtils.getCurrentYear();
			Builder builder = PayloadInfo.newBuilder()
			.setPvname(ARCH_APPL_PING_PV)
			.setType(ArchDBRTypes.DBR_SCALAR_DOUBLE.getPBPayloadType())
			.setYear(currYear);
			byte[] headerBytes = LineEscaper.escapeNewLines(builder.build().toByteArray());
			os.write(headerBytes);
			os.write(LineEscaper.NEWLINE_CHAR);
			
			for(int i = 0; i < 10; i++) {
				ByteArray val = new SimulationEvent(0, currYear, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.1*i)).getRawForm();
				os.write(val.data, val.off, val.len);
				os.write(LineEscaper.NEWLINE_CHAR);
			}
		} finally {
			try { os.flush(); os.close(); } catch(Throwable t) {}
		}
	}


	@Override
	public void init() throws ServletException {
		this.configService = (ConfigService) this.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
	}
	
	
	
	/**
	 * Based on the post processor, we make a call on where we can process the request in parallel
	 * Either way, we return the result of this decision as two components
	 * One is an executor to use
	 * The other is a list of timespans that we have broken  the request into - the timespans will most likely be the time spans of the individual bins in the request.
	 * @author mshankar
	 *
	 */
	private static class RetrievalExecutorResult implements AutoCloseable { 
		ExecutorService executorService;
		LinkedList<TimeSpan> requestTimespans;
		RetrievalExecutorResult(ExecutorService executorService, LinkedList<TimeSpan> requestTimepans) {
			this.executorService = executorService;
			this.requestTimespans = requestTimepans;
		}

		@Override
		public void close() {
			try { 
				this.executorService.shutdown();
			} catch (Throwable t) { 
				logger.debug("Exception shutting down executor", t);
			}
		}
	}
	
	
	/**
	 * Determine the thread pool to be used for post processing based on some characteristics of the request
	 * The plugins will yield a list of callables that could potentially be evaluated in parallel 
	 * Whether we evaluate in parallel is made here.
	 * @param pvName
	 * @param postProcessor
	 * @return
	 */
	private static RetrievalExecutorResult determineExecutorForPostProcessing(String pvName, PVTypeInfo typeInfo, LinkedList<TimeSpan> requestTimes, HttpServletRequest req, PostProcessor postProcessor) {
		long memoryConsumption = postProcessor.estimateMemoryConsumption(pvName, typeInfo, requestTimes.getFirst().getStartTime(), requestTimes.getLast().getEndTime(), req);
		double memoryConsumptionInMB = (double)memoryConsumption/(1024*1024);
		DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
		logger.debug("Memory consumption estimate from postprocessor for pv " + pvName + " is " + memoryConsumption + "(bytes) ~= " + twoSignificantDigits.format(memoryConsumptionInMB) + "(MB)");
		
		// For now, we only use the current thread to execute in serial.
		// Once we get the unit tests for the post processors in a more rigorous shape, we can start using the ForkJoinPool.
		// There are some complexities in using the ForkJoinPool - in this case, we need to convert to using synchronized versions of the SummaryStatistics and DescriptiveStatistics
		// We also still have the issue where we can add a sample twice because of the non-transactional nature of ETL.
		// However, there is a lot of work done by the PostProcessors in estimateMemoryConsumption so leave this call in place.
		return new RetrievalExecutorResult(new CurrentThreadExecutorService(), requestTimes);
	}
	
	
	/**
	 * Make a call to the engine to fetch the latest metadata and then add it to the mergeConsumer
	 * @param pvName
	 * @param applianceForPV
	 */
	@SuppressWarnings("unchecked")
	private HashMap<String, String> fetchLatestMedataFromEngine(String pvName, ApplianceInfo applianceForPV) {
		try { 
			String metadataURL = applianceForPV.getEngineURL() + "/getMetadata?pv=" + URLEncoder.encode(pvName, "UTF-8");
			logger.debug("Getting metadata from the engine using " + metadataURL);
			JSONObject metadata = GetUrlContent.getURLContentAsJSONObject(metadataURL);
			return (HashMap<String, String>) metadata;
		} catch(Exception ex) { 
			logger.warn("Exception fetching latest metadata for pv " + pvName, ex);
		}
		return null;
	}


	/**
	 * If the pv is hosted on another appliance, proxy retrieval requests from that appliance
	 * We expect to return immediately after this method. 
	 * @param req
	 * @param resp
	 * @param pvName
	 * @param useChunkedEncoding
	 * @param dataRetrievalURLForPV
	 * @throws IOException
	 */
	private void proxyRetrievalRequest(HttpServletRequest req, HttpServletResponse resp, String pvName, boolean useChunkedEncoding, String dataRetrievalURLForPV) throws IOException {
		try {
			// TODO add some intelligent business logic to determine if redirect/proxy. 
			// It may be beneficial to support both and choose based on where the client in calling from or perhaps from a header?
			boolean redirect = false;
			if(redirect) { 
				logger.debug("Data for pv " + pvName + "is elsewhere. Redirecting to appliance " + dataRetrievalURLForPV);
				URI redirectURI = new URI(dataRetrievalURLForPV + "/" + req.getPathInfo());
				String redirectURIStr = redirectURI.normalize().toString() +  "?" + req.getQueryString();
				logger.debug("URI for redirect is " + redirectURIStr);
				resp.sendRedirect(redirectURIStr);
				return;
			} else { 
				logger.debug("Data for pv " + pvName + "is elsewhere. Proxying appliance " + dataRetrievalURLForPV);
				URI redirectURI = new URI(dataRetrievalURLForPV + "/" + req.getPathInfo());
				String redirectURIStr = redirectURI.normalize().toString() +  "?" + req.getQueryString();
				logger.debug("URI for proxying is " + redirectURIStr);

//				if(useChunkedEncoding) { 
//					resp.addHeader("Transfer-Encoding", "chunked");
//				}

				CloseableHttpClient httpclient = HttpClients.createDefault();
				HttpGet getMethod = new HttpGet(redirectURIStr);
				getMethod.addHeader("Connection", "close"); // https://www.nuxeo.com/blog/using-httpclient-properly-avoid-closewait-tcp-connections/
				try(CloseableHttpResponse response = httpclient.execute(getMethod)) {
					if(response.getStatusLine().getStatusCode() == 200) {
						HttpEntity entity = response.getEntity();
						HashSet<String> proxiedHeaders = new HashSet<String>();
						proxiedHeaders.addAll(Arrays.asList(MimeResponse.PROXIED_HEADERS));
						Header[] headers = response.getAllHeaders();
						for(Header header : headers) {
							if(proxiedHeaders.contains(header.getName())) {
								logger.debug("Adding headerName " + header.getName() + " and value " + header.getValue() + " when proxying request");
								resp.addHeader(header.getName(), header.getValue());
							}
						}
	
						if (entity != null) {
							logger.debug("Obtained a HTTP entity of length " + entity.getContentLength());
							try(OutputStream os = resp.getOutputStream(); InputStream is = new BufferedInputStream(entity.getContent())) {
								byte buf[] = new byte[10*1024];
								int bytesRead = is.read(buf);
								while(bytesRead > 0) {
									os.write(buf, 0, bytesRead);
									resp.flushBuffer();
									bytesRead = is.read(buf);
								}
							}
						} else {
							throw new IOException("HTTP response did not have an entity associated with it");
						}
					} else {
						logger.error("Invalid status code " + response.getStatusLine().getStatusCode() + " when connecting to URL " + redirectURIStr + ". Sending the errorstream across");
						try (ByteArrayOutputStream os = new ByteArrayOutputStream()) { 
							try(InputStream is = new BufferedInputStream(response.getEntity().getContent())) {
								byte buf[] = new byte[10*1024];
								int bytesRead = is.read(buf);
								while(bytesRead > 0) {
									os.write(buf, 0, bytesRead);
									bytesRead = is.read(buf);
								}
							}
							resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
							resp.sendError(response.getStatusLine().getStatusCode(), new String(os.toByteArray()));
						}
					}
				}
			}
			return;
		} catch(URISyntaxException ex) {
			throw new IOException(ex);
		}
	}
	
	/**
	 * If multiple pvs are hosted on another appliance, a retrieval request is made to that appliance and
	 * the event stream is returned.
	 * @param req
	 * @param resp
	 * @param requestTimes 
	 * @param pvInfo
	 * @param useChunkedEncoding
	 * @param dataRetrievalURLForPV
	 * @param start
	 * @param end
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private List<List<Future<EventStream>>> retrieveEventStreamFromForeignAppliance(
			HttpServletRequest req, HttpServletResponse resp,
			ArrayList<PVInfoForClusterRetrieval> pvInfos, LinkedList<TimeSpan> requestTimes,
			boolean useChunkedEncoding, String dataRetrievalURLForPV,
			Timestamp start, Timestamp end) 
					throws IOException, InterruptedException, ExecutionException {
		
		// Get the executors for the PVs in other clusters
		List<RetrievalExecutorResult> executorResults = new ArrayList<RetrievalExecutorResult>(pvInfos.size());
		for (int i = 0; i < pvInfos.size(); i++) {
			PVInfoForClusterRetrieval pvInfo = pvInfos.get(i);
			executorResults.add(determineExecutorForPostProcessing(pvInfo.getPVName(), 
					pvInfo.getTypeInfo(), requestTimes, req, pvInfo.getPostProcessor()));
		}
		
		// Get list of lists of futures of retrieval results. Basically, this is setting up the data sources for retrieval.
		List<LinkedList<Future<RetrievalResult>>> listOfRetrievalResultsFutures = new ArrayList<LinkedList<Future<RetrievalResult>>>();
		for (int i = 0; i < pvInfos.size(); i++) {
			PVInfoForClusterRetrieval pvInfo = pvInfos.get(i);
			listOfRetrievalResultsFutures.add(resolveAllDataSources(pvInfo.getPVName(), pvInfo.getTypeInfo(), pvInfo.getPostProcessor(), 
					pvInfo.getApplianceInfo(), new BasicContext(), executorResults.get(i), req, resp));
		}
		
		// Now the data is being retrieved, producing a list of lists of futures of event streams.
		List<List<Future<EventStream>>> listOfEventStreamFutures = new ArrayList<List<Future<EventStream>>>();
		for (int i = 0; i < pvInfos.size(); i++) {
			listOfEventStreamFutures.add(getEventStreamFuturesFromRetrievalResults(executorResults.get(i), listOfRetrievalResultsFutures.get(i)));
		}
		
		return listOfEventStreamFutures;
	}

	/**
	 * Parse the timeranges parameter and generate a list of TimeSpans.
	 * @param resp
	 * @param pvName
	 * @param requestTimes - list of timespans that we add the valid times to.
	 * @param timeRangesStr
	 * @return
	 * @throws IOException
	 */
	private boolean parseTimeRanges(HttpServletResponse resp, String pvName, LinkedList<TimeSpan> requestTimes, String timeRangesStr) throws IOException {
		String[] timeRangesStrList =  timeRangesStr.split(",");
		if(timeRangesStrList.length%2 != 0) { 
			String msg = "Need to specify an even number of times in timeranges for pv " + pvName + ". We have " + timeRangesStrList.length + " times";
			logger.error(msg);
			resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
			return false;
		}
		
		LinkedList<Timestamp> timeRangesList = new LinkedList<Timestamp>();
		for(String timeRangesStrItem : timeRangesStrList) { 
			try { 
				Timestamp ts = TimeUtils.convertFromISO8601String(timeRangesStrItem);
				timeRangesList.add(ts);
			} catch(IllegalArgumentException ex) {
				try { 
					Timestamp ts = TimeUtils.convertFromDateTimeStringWithOffset(timeRangesStrItem);
					timeRangesList.add(ts);
				} catch(IllegalArgumentException ex2) { 
					String msg = "Cannot parse time " + timeRangesStrItem;
					logger.warn(msg, ex2);
					resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
					resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
					return false;
				}
			}
		}
		
		assert(timeRangesList.size()%2 == 0);
		Timestamp prevEnd = null;
		while(!timeRangesList.isEmpty()) { 
			Timestamp t0 = timeRangesList.pop();
			Timestamp t1 = timeRangesList.pop();

			if(t1.before(t0)) {
				String msg = "For request, end " + t1.toString() + " is before start " + t0.toString() + " for pv " + pvName;
				logger.error(msg);
				resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
				return false;
			}
			
			if(prevEnd != null) { 
				if(t0.before(prevEnd)) { 
					String msg = "For request, start time " + t0.toString() + " is before previous end time " + prevEnd.toString() + " for pv " + pvName;
					logger.error(msg);
					resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
					resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
					return false ;
				}
			}
			prevEnd = t1;
			requestTimes.add(new TimeSpan(t0, t1));	
		}
		return true;
	}
	
	/**
	 * Used when we are constructing a TypeInfo from a template. We want to look at the actual data and see if we can set the DBR type correctly.
	 * Return true if we are able to do this.
	 * @param typeInfo
	 * @return
	 * @throws IOException
	 */
	private boolean setActualDBRTypeFromData(String pvName, PVTypeInfo typeInfo, ConfigService configService) throws IOException {
		String[] dataStores = typeInfo.getDataStores();
		for(String dataStore : dataStores) { 
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(dataStore, configService);
			if(plugin instanceof ETLDest) { 
				ETLDest etlDest = (ETLDest) plugin;
				try(BasicContext context = new BasicContext()) { 
					Event e = etlDest.getLastKnownEvent(context, pvName);
					if(e != null) { 
						typeInfo.setDBRType(e.getDBRType());
						return true;
					}
				}
			}
		}
		
		return false;
	}
	
    /** 
     * <p> 
     * This class should be used to store the PV name and type info of data that will be 
     * retrieved from neighbouring nodes in a cluster, to be returned in a response from 
     * the source cluster. 
     * </p> 
     * <p> 
     * PVTypeInfo maintains a PV name field, too. At first the PV name field in this object 
     * seems superfluous. But the field is necessary, as it contains the unprocessed PV 
     * name as opposed to the PV name stored by the PVTypeInfo object, which has been 
     * processed. 
     * </p> 
     *  
     * @author Michael Kenning 
     * 
     */ 
    private class PVInfoForClusterRetrieval { 
             
            private String pvName; 
            private PVTypeInfo typeInfo; 
            private PostProcessor postProcessor; 
            private ApplianceInfo applianceInfo; 
             
            private PVInfoForClusterRetrieval(String pvName, PVTypeInfo typeInfo,  
                            PostProcessor postProcessor, ApplianceInfo applianceInfo) { 
                    this.pvName = pvName; 
                    this.typeInfo = typeInfo; 
                    this.postProcessor = postProcessor; 
                    this.applianceInfo = applianceInfo; 
                     
                    assert(this.pvName != null); 
                    assert(this.typeInfo != null); 
                    assert(this.postProcessor != null); 
                    assert(this.applianceInfo != null); 
            } 
             
            public String getPVName() { 
                    return pvName; 
            } 
             
            public PVTypeInfo getTypeInfo() { 
                    return typeInfo; 
            } 
             
            public PostProcessor getPostProcessor() { 
                    return postProcessor; 
            } 
             
            public ApplianceInfo getApplianceInfo() { 
                    return applianceInfo; 
            } 

    }
}
