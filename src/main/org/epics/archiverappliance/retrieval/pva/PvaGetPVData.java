package org.epics.archiverappliance.retrieval.pva;

import java.io.IOException;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PoorMansProfiler;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigService.STARTUP_SEQUENCE;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.mgmt.pva.actions.PvaAction;
import org.epics.archiverappliance.retrieval.DataSourceResolution;
import org.epics.archiverappliance.retrieval.MismatchedDBRTypeException;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RetrievalResult;
import org.epics.archiverappliance.retrieval.UnitOfRetrieval;
import org.epics.archiverappliance.retrieval.postprocessors.AfterAllStreams;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.ExtraFieldsPostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.FirstSamplePP;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessors;
import org.epics.archiverappliance.retrieval.workers.CurrentThreadExecutorService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.nt.NTURI;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status.StatusType;
import org.json.simple.JSONObject;

public class PvaGetPVData implements PvaAction {

	private static final Logger logger = Logger.getLogger(PvaGetPVData.class.getName());
	public static final String NAME = "getPVsData";

	ConfigService configService;

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public void request(PVStructure args, RPCResponseCallback callback, ConfigService configService) {
		this.configService = configService;
		try {
			doGetSinglePV(extractQuery(args), callback, configService);
		} catch (ServletException | IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Extract the query parameters into a {@link HashMap}
	 * @param args request PVStructure - must be of type {@link NTURI}
	 * @return map of the query parameters
	 */
	private Map<String, String> extractQuery(PVStructure args) {
		Map<String, String> reqParameters = new HashMap<>();
		NTURI uri = NTURI.wrap(args);
		reqParameters = Arrays.asList(uri.getQueryNames()).stream().collect(Collectors.toMap(
				queryName -> {
					return queryName;
					},
				queryName -> {
					return uri.getQueryField(PVString.class, queryName).get();
			}));
		return reqParameters;
	}

	private void doGetSinglePV(Map<String, String> reqParameters, RPCResponseCallback resp, ConfigService configService)
			throws ServletException, IOException {

		PoorMansProfiler pmansProfiler = new PoorMansProfiler();
		String pvName = reqParameters.get("pv");

		if (configService.getStartupState() != STARTUP_SEQUENCE.STARTUP_COMPLETE) {
			String msg = "Cannot process data retrieval requests for PV " + pvName
					+ " until the appliance has completely started up.";
			logger.error(msg);
			sendError(resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE, msg, Optional.empty());
			return;
		}

		String startTimeStr = reqParameters.get("from");
		String endTimeStr = reqParameters.get("to");
		boolean useReduced = false;
		String useReducedStr = reqParameters.get("usereduced");
		if (useReducedStr != null && !useReducedStr.equals("")) {
			try {
				useReduced = Boolean.parseBoolean(useReducedStr);
			} catch (Exception ex) {
				logger.error("Exception parsing usereduced", ex);
				useReduced = false;
			}
		}

		boolean useChunkedEncoding = true;
		String doNotChunkStr = reqParameters.get("donotchunk");
		if (doNotChunkStr != null && !doNotChunkStr.equals("false")) {
			logger.info("Turning off HTTP chunked encoding");
			useChunkedEncoding = false;
		}

		boolean fetchLatestMetadata = false;
		String fetchLatestMetadataStr = reqParameters.get("fetchLatestMetadata");
		if (fetchLatestMetadataStr != null && fetchLatestMetadataStr.equals("true")) {
			logger.info("Adding a call to the engine to fetch the latest metadata");
			fetchLatestMetadata = true;
		}

		// For data retrieval we need a PV info. However, in case of PV's that have long
		// since retired, we may not want to have PVTypeInfo's in the system.
		// So, we support a template PV that lays out the data sources.
		// During retrieval, you can pass in the PV as a template and we'll clone this
		// and make a temporary copy.
		String retiredPVTemplate = reqParameters.get("retiredPVTemplate");

		if (pvName == null) {
			String msg = "PV name is null.";
			sendError(resp, HttpServletResponse.SC_BAD_REQUEST, msg, Optional.empty());
			return;
		}

		if (pvName.endsWith(".VAL")) {
			int len = pvName.length();
			pvName = pvName.substring(0, len - 4);
			logger.info("Removing .VAL from pvName for request giving " + pvName);
		}

		// ISO datetimes are of the form "2011-02-02T08:00:00.000Z"
		Timestamp end = TimeUtils.plusHours(TimeUtils.now(), 1);
		if (endTimeStr != null) {
			try {
				end = TimeUtils.convertFromISO8601String(endTimeStr);
			} catch (IllegalArgumentException ex) {
				try {
					end = TimeUtils.convertFromDateTimeStringWithOffset(endTimeStr);
				} catch (IllegalArgumentException ex2) {
					String msg = "Cannot parse time" + endTimeStr;
					sendError(resp, HttpServletResponse.SC_BAD_REQUEST, msg, Optional.of(ex2));
					return;
				}
			}
		}

		// We get one day by default
		Timestamp start = TimeUtils.minusDays(end, 1);
		if (startTimeStr != null) {
			try {
				start = TimeUtils.convertFromISO8601String(startTimeStr);
			} catch (IllegalArgumentException ex) {
				try {
					start = TimeUtils.convertFromDateTimeStringWithOffset(startTimeStr);
				} catch (IllegalArgumentException ex2) {
					String msg = "Cannot parse time " + startTimeStr;
					logger.warn(msg, ex2);
					sendError(resp, HttpServletResponse.SC_BAD_REQUEST, msg, Optional.of(ex2));
					return;
				}
			}
		}

		if (end.before(start)) {
			String msg = "For request, end " + end.toString() + " is before start " + start.toString() + " for pv "
					+ pvName;
			logger.error(msg);
			sendError(resp, HttpServletResponse.SC_BAD_REQUEST, msg, Optional.empty());
			return;
		}

		LinkedList<TimeSpan> requestTimes = new LinkedList<TimeSpan>();

		// We can specify a list of time stamp pairs using the optional timeranges
		// parameter
		String timeRangesStr = reqParameters.get("timeranges");
		if (timeRangesStr != null) {
			boolean continueWithRequest = parseTimeRanges(resp, pvName, requestTimes, timeRangesStr);
			if (!continueWithRequest) {
				// Cannot parse the time ranges properly; we so abort the request.
				return;
			}

			// Override the start and the end so that the mergededup consumer works
			// correctly.
			start = requestTimes.getFirst().getStartTime();
			end = requestTimes.getLast().getEndTime();

		} else {
			requestTimes.add(new TimeSpan(start, end));
		}

		assert (requestTimes.size() > 0);

		String postProcessorUserArg = reqParameters.get("pp");
		if (pvName.contains("(")) {
			if (!pvName.contains(")")) {
				logger.error("Unbalanced paran " + pvName);
				sendError(resp, HttpServletResponse.SC_BAD_REQUEST, "Unbalanced paran " + pvName, Optional.empty());
				return;
			}
			String[] components = pvName.split("[(,)]");
			postProcessorUserArg = components[0];
			pvName = components[1];
			if (components.length > 2) {
				for (int i = 2; i < components.length; i++) {
					postProcessorUserArg = postProcessorUserArg + "_" + components[i];
				}
			}
			logger.info("After parsing the function call syntax pvName is " + pvName + " and postProcessorUserArg is "
					+ postProcessorUserArg);
		}
		PostProcessor postProcessor = PostProcessors.findPostProcessor(postProcessorUserArg);
		PVTypeInfo typeInfo = PVNames.determineAppropriatePVTypeInfo(pvName, configService);
		pmansProfiler.mark("After PVTypeInfo");

		if (typeInfo == null) {
			if (retiredPVTemplate != null) {
				PVTypeInfo templateTypeInfo = PVNames.determineAppropriatePVTypeInfo(retiredPVTemplate, configService);
				if (templateTypeInfo != null) {
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

		if (typeInfo == null) {
			logger.error("Unable to find typeinfo for pv " + pvName);
			sendError(resp, HttpServletResponse.SC_NOT_FOUND, "Unable to find typeinfo for pv " + pvName,
					Optional.empty());
			return;
		}

		if (postProcessor == null) {
			if (useReduced) {
				String defaultPPClassName = configService.getInstallationProperties().getProperty(
						"org.epics.archiverappliance.retrieval.DefaultUseReducedPostProcessor",
						FirstSamplePP.class.getName());
				logger.debug("Using the default usereduced preprocessor " + defaultPPClassName);
				try {
					postProcessor = (PostProcessor) Class.forName(defaultPPClassName).newInstance();
				} catch (Exception ex) {
					logger.error("Exception constructing new instance of post processor " + defaultPPClassName, ex);
					postProcessor = null;
				}
			}
		}

		if (postProcessor == null) {
			logger.debug("Using the default raw preprocessor");
			postProcessor = new DefaultRawPostProcessor();
		}

		ApplianceInfo applianceForPV = configService.getApplianceForPV(pvName);
		if (applianceForPV == null) {
			// TypeInfo cannot be null here...
			assert (typeInfo != null);
			applianceForPV = configService.getAppliance(typeInfo.getApplianceIdentity());
		}

		// if(!applianceForPV.equals(configService.getMyApplianceInfo())) {
		// // Data for pv is elsewhere. Proxy/redirect and return.
		// proxyRetrievalRequest(req, resp, pvName, useChunkedEncoding,
		// applianceForPV.getRetrievalURL() + "/../data" );
		// return;
		// }

		pmansProfiler.mark("After Appliance Info");

		String pvNameFromRequest = pvName;

		String fieldName = PVNames.getFieldName(pvName);
		if (fieldName != null && !fieldName.equals("") && typeInfo.checkIfFieldAlreadySepcified(fieldName)) {
			logger.debug("We reset the pvName " + pvName + " to one from the typeinfo " + typeInfo.getPvName()
					+ " as that determines the name of the stream. Also using ExtraFieldsPostProcessor");
			pvName = typeInfo.getPvName();
			postProcessor = new ExtraFieldsPostProcessor(fieldName);
		}

		try {
			// Postprocessors get their mandatory arguments from the request.
			// If user does not pass in the expected request, throw an exception.
			postProcessor.initialize(postProcessorUserArg, pvName);
		} catch (Exception ex) {
			logger.error("Postprocessor threw an exception during initialization for " + pvName, ex);
			sendError(resp, HttpServletResponse.SC_NOT_FOUND,
					"Postprocessor threw an exception during initialization for " + pvName, Optional.of(ex));
			return;
		}

		try (BasicContext retrievalContext = new BasicContext(typeInfo.getDBRType(), pvNameFromRequest);
				PvaMergeDedupConsumer mergeDedupCountingConsumer = createMergeDedupConsumer(resp, useChunkedEncoding, typeInfo);
				RetrievalExecutorResult executorResult = determineExecutorForPostProcessing(pvName, typeInfo, requestTimes, postProcessor)) {
			HashMap<String, String> engineMetadata = null;
			if (fetchLatestMetadata) {
				// Make a call to the engine to fetch the latest metadata.
				engineMetadata = fetchLatestMedataFromEngine(pvName, applianceForPV);
			}

			LinkedList<Future<RetrievalResult>> retrievalResultFutures = resolveAllDataSources(pvName, typeInfo,
					postProcessor, applianceForPV, retrievalContext, executorResult);
			pmansProfiler.mark("After data source resolution");

			long s1 = System.currentTimeMillis();
			String currentlyProcessingPV = null;

			List<Future<EventStream>> eventStreamFutures = getEventStreamFuturesFromRetrievalResults(executorResult,
					retrievalResultFutures);

			logger.info("Done with the RetrievalResult's; moving onto the individual event stream from each source for "
					+ pvName);
			pmansProfiler.mark("After retrieval results");

			for (Future<EventStream> future : eventStreamFutures) {
				EventStreamDesc sourceDesc = null;
				try (EventStream eventStream = future.get()) {
					sourceDesc = null; // Reset it for each loop iteration.
					sourceDesc = eventStream.getDescription();
					if (sourceDesc == null) {
						logger.warn("Skipping event stream without a desc for pv " + pvName);
						continue;
					}

					logger.info("Processing event stream for pv " + pvName + " from source "
							+ ((eventStream.getDescription() != null) ? eventStream.getDescription().getSource()
									: " unknown"));

					try {
						mergeTypeInfo(typeInfo, sourceDesc, engineMetadata);
					} catch (MismatchedDBRTypeException mex) {
						logger.error(mex.getMessage(), mex);
						continue;
					}

					if (currentlyProcessingPV == null || !currentlyProcessingPV.equals(pvName)) {
						logger.info("Switching to new PV " + pvName
								+ " In some mime responses we insert special headers at the beginning of the response. Calling the hook for that");
						currentlyProcessingPV = pvName;
						mergeDedupCountingConsumer.processingPV(currentlyProcessingPV, start, end, (eventStream != null) ? sourceDesc : null);
					}

					try {
						// If the postProcessor does not have a consolidated event stream, we send each
						// eventstream across as we encounter it.
						// Else we send the consolidatedEventStream down below.
						if (!(postProcessor instanceof PostProcessorWithConsolidatedEventStream)) {
							mergeDedupCountingConsumer.consumeEventStream(eventStream);
							// resp.flushBuffer();
						}
					} catch (Exception ex) {
						if (ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
							// We check for ClientAbortException etc this way to avoid including tomcat jars
							// in the build path.
							logger.info("Exception when consuming and flushing data from " + sourceDesc.getSource(), ex);
						} else {
							logger.error("Exception when consuming and flushing data from " + sourceDesc.getSource()
									+ "-->" + ex.toString(), ex);
						}
					}
					pmansProfiler.mark("After event stream " + eventStream.getDescription().getSource());
				} catch (Exception ex) {
					if (ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
						// We check for ClientAbortException etc this way to avoid including tomcat jars
						// in the build path.
						logger.info("Exception when consuming and flushing data from "
								+ (sourceDesc != null ? sourceDesc.getSource() : "N/A"), ex);
					} else {
						logger.error(
								"Exception when consuming and flushing data from "
										+ (sourceDesc != null ? sourceDesc.getSource() : "N/A") + "-->" + ex.toString(), ex);
					}
				}
			}

			if (postProcessor instanceof PostProcessorWithConsolidatedEventStream) {
				try (EventStream eventStream = ((PostProcessorWithConsolidatedEventStream) postProcessor)
						.getConsolidatedEventStream()) {
					EventStreamDesc sourceDesc = eventStream.getDescription();
					if (sourceDesc == null) {
						logger.error("Skipping event stream without a desc for pv " + pvName + " and post processor "
								+ postProcessor.getExtension());
					} else {
						mergeDedupCountingConsumer.consumeEventStream(eventStream);
						// resp.flushBuffer();
					}
				}
			}

			// If the postProcessor needs to send final data across, give it a chance now...
			if (postProcessor instanceof AfterAllStreams) {
				EventStream finalEventStream = ((AfterAllStreams) postProcessor).anyFinalData();
				if (finalEventStream != null) {
					mergeDedupCountingConsumer.consumeEventStream(finalEventStream);			
					// resp.flushBuffer();
				}
			}

			mergeDedupCountingConsumer.send();
			pmansProfiler.mark("After writing all eventstreams to response");

			long s2 = System.currentTimeMillis();
			logger.info("For the complete request, found a total of " + mergeDedupCountingConsumer.totalEventsForAllPVs
					+ " in " + (s2 - s1) + "(ms)" + " skipping " + mergeDedupCountingConsumer.skippedEventsForAllPVs
					+ " events" + " deduping involved " + mergeDedupCountingConsumer.comparedEventsForAllPVs
					+ " compares.");
		} catch (Exception ex) {
			if (ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
				// We check for ClientAbortException etc this way to avoid including tomcat jars
				// in the build path.
				logger.debug("Exception when retrieving data ", ex);
			} else {
				logger.error("Exception when retrieving data " + "-->" + ex.toString(), ex);
			}
		}
		pmansProfiler.mark("After all closes and flushing all buffers");

		// Till we determine all the if conditions where we log this, we log sparingly..
		if (pmansProfiler.totalTimeMS() > 5000) {
			logger.error("Retrieval time for " + pvName + " from " + startTimeStr + " to " + endTimeStr
					+ pmansProfiler.toString());
		}
	}

	/**
	 * Handle error conditionals and send the appropriate response to the requesting
	 * client
	 * 
	 * @param resp
	 * @param scServiceUnavailable
	 * @param msg
	 * @param ex
	 */
	private void sendError(RPCResponseCallback resp, int scServiceUnavailable, String msg, Optional<Exception> ex) {
		if (ex.isPresent()) {
			resp.requestDone(StatusFactory.getStatusCreate().createStatus(StatusType.ERROR, msg, ex.get()), null);
		} else {
			resp.requestDone(StatusFactory.getStatusCreate().createStatus(StatusType.ERROR, msg, null), null);
		}
	}

	/**
	 * Parse the timeranges parameter and generate a list of TimeSpans.
	 * 
	 * @param resp
	 * @param pvName
	 * @param requestTimes
	 *            - list of timespans that we add the valid times to.
	 * @param timeRangesStr
	 * @return
	 * @throws IOException
	 */
	private boolean parseTimeRanges(RPCResponseCallback resp, String pvName, LinkedList<TimeSpan> requestTimes,
			String timeRangesStr) throws IOException {
		String[] timeRangesStrList = timeRangesStr.split(",");

		if (timeRangesStrList.length % 2 != 0) {
			String msg = "Need to specify an even number of times in timeranges for pv " + pvName + ". We have "
					+ timeRangesStrList.length + " times";
			logger.error(msg);
			sendError(resp, HttpServletResponse.SC_BAD_REQUEST, msg, Optional.empty());
			return false;
		}

		LinkedList<Timestamp> timeRangesList = new LinkedList<Timestamp>();
		for (String timeRangesStrItem : timeRangesStrList) {
			try {
				Timestamp ts = TimeUtils.convertFromISO8601String(timeRangesStrItem);
				timeRangesList.add(ts);
			} catch (IllegalArgumentException ex) {
				try {
					Timestamp ts = TimeUtils.convertFromDateTimeStringWithOffset(timeRangesStrItem);
					timeRangesList.add(ts);
				} catch (IllegalArgumentException ex2) {
					String msg = "Cannot parse time " + timeRangesStrItem;
					logger.warn(msg, ex2);
					sendError(resp, HttpServletResponse.SC_BAD_REQUEST, msg, Optional.of(ex2));
					return false;
				}
			}
		}

		assert (timeRangesList.size() % 2 == 0);
		Timestamp prevEnd = null;
		while (!timeRangesList.isEmpty()) {
			Timestamp t0 = timeRangesList.pop();
			Timestamp t1 = timeRangesList.pop();

			if (t1.before(t0)) {
				String msg = "For request, end " + t1.toString() + " is before start " + t0.toString() + " for pv "
						+ pvName;
				logger.error(msg);
				sendError(resp, HttpServletResponse.SC_BAD_REQUEST, msg, Optional.empty());
				return false;
			}

			if (prevEnd != null) {
				if (t0.before(prevEnd)) {
					String msg = "For request, start time " + t0.toString() + " is before previous end time "
							+ prevEnd.toString() + " for pv " + pvName;
					logger.error(msg);
					sendError(resp, HttpServletResponse.SC_BAD_REQUEST, msg, Optional.empty());
					return false;
				}
			}
			prevEnd = t1;
			requestTimes.add(new TimeSpan(t0, t1));
		}
		return true;
	}

	/**
	 * Used when we are constructing a TypeInfo from a template. We want to look at
	 * the actual data and see if we can set the DBR type correctly. Return true if
	 * we are able to do this.
	 * 
	 * @param typeInfo
	 * @return
	 * @throws IOException
	 */
	private boolean setActualDBRTypeFromData(String pvName, PVTypeInfo typeInfo, ConfigService configService)
			throws IOException {
		String[] dataStores = typeInfo.getDataStores();
		for (String dataStore : dataStores) {
			StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(dataStore, configService);
			if (plugin instanceof ETLDest) {
				ETLDest etlDest = (ETLDest) plugin;
				try (BasicContext context = new BasicContext()) {
					Event e = etlDest.getLastKnownEvent(context, pvName);
					if (e != null) {
						typeInfo.setDBRType(e.getDBRType());
						return true;
					}
				}
			}
		}

		return false;
	}

	/**
	 * Create a merge dedup consumer that will merge/dedup multiple event streams.
	 * This basically makes sure that we are serving up events in monotonically
	 * increasing timestamp order.
	 * 
	 * @param resp
	 * @param extension
	 * @param useChunkedEncoding
	 * @return
	 * @throws ServletException
	 */
	private PvaMergeDedupConsumer createMergeDedupConsumer(RPCResponseCallback resp, boolean useChunkedEncoding, PVTypeInfo typeInfo)
			throws ServletException {
		PvaMergeDedupConsumer mergeDedupCountingConsumer = null;
		try {
			PvaMimeResponse mimeresponse = PvaMimeResponse.class.newInstance();
			HashMap<String, String> extraHeaders = mimeresponse.getExtraHeaders();
			logger.info(extraHeaders);
			mergeDedupCountingConsumer = new PvaMergeDedupConsumer(mimeresponse, resp, typeInfo);
		} catch (Exception ex) {
			throw new ServletException(ex);

		}
		return mergeDedupCountingConsumer;
	}

	/**
	 * Make a call to the engine to fetch the latest metadata and then add it to the
	 * mergeConsumer
	 * 
	 * @param pvName
	 * @param applianceForPV
	 */
	@SuppressWarnings("unchecked")
	private HashMap<String, String> fetchLatestMedataFromEngine(String pvName, ApplianceInfo applianceForPV) {
		try {
			String metadataURL = applianceForPV.getEngineURL() + "/getMetadata?pv="
					+ URLEncoder.encode(pvName, "UTF-8");
			logger.debug("Getting metadata from the engine using " + metadataURL);
			JSONObject metadata = GetUrlContent.getURLContentAsJSONObject(metadataURL);
			return (HashMap<String, String>) metadata;
		} catch (Exception ex) {
			logger.warn("Exception fetching latest metadata for pv " + pvName, ex);
		}
		return null;
	}

	/**
	 * Based on the post processor, we make a call on where we can process the
	 * request in parallel Either way, we return the result of this decision as two
	 * components One is an executor to use The other is a list of timespans that we
	 * have broken the request into - the timespans will most likely be the time
	 * spans of the individual bins in the request.
	 * 
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
	 * Determine the thread pool to be used for post processing based on some
	 * characteristics of the request The plugins will yield a list of callables
	 * that could potentially be evaluated in parallel Whether we evaluate in
	 * parallel is made here.
	 * 
	 * @param pvName
	 * @param postProcessor
	 * @return
	 */
	private static RetrievalExecutorResult determineExecutorForPostProcessing(String pvName, PVTypeInfo typeInfo,
			LinkedList<TimeSpan> requestTimes, PostProcessor postProcessor) {
		long memoryConsumption = postProcessor.estimateMemoryConsumption(pvName, typeInfo,
				requestTimes.getFirst().getStartTime(), requestTimes.getLast().getEndTime(), null);
		double memoryConsumptionInMB = (double) memoryConsumption / (1024 * 1024);
		DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
		logger.debug("Memory consumption estimate from postprocessor for pv " + pvName + " is " + memoryConsumption
				+ "(bytes) ~= " + twoSignificantDigits.format(memoryConsumptionInMB) + "(MB)");

		// For now, we only use the current thread to execute in serial.
		// Once we get the unit tests for the post processors in a more rigorous shape,
		// we can start using the ForkJoinPool.
		// There are some complexities in using the ForkJoinPool - in this case, we need
		// to convert to using synchronized versions of the SummaryStatistics and
		// DescriptiveStatistics
		// We also still have the issue where we can add a sample twice because of the
		// non-transactional nature of ETL.
		// However, there is a lot of work done by the PostProcessors in
		// estimateMemoryConsumption so leave this call in place.
		return new RetrievalExecutorResult(new CurrentThreadExecutorService(), requestTimes);
	}

	/**
	 * Given a list of retrievalResult futures, we loop thru these; execute them
	 * (basically calling the reader getData) and then sumbit the returned callables
	 * to the executorResult's executor. We return a list of eventstream futures.
	 * 
	 * @param executorResult
	 * @param retrievalResultFutures
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private List<Future<EventStream>> getEventStreamFuturesFromRetrievalResults(RetrievalExecutorResult executorResult,
			LinkedList<Future<RetrievalResult>> retrievalResultFutures)
			throws InterruptedException, ExecutionException {
		// List containing the result
		List<Future<EventStream>> eventStreamFutures = new LinkedList<Future<EventStream>>();

		// Loop thru the retrievalResultFutures one by one in sequence; get all the
		// event streams from the plugins and consolidate them into a sequence of
		// eventStream futures.
		for (Future<RetrievalResult> retrievalResultFuture : retrievalResultFutures) {
			// This call blocks until the future is complete.
			// For now, we use a simple get as opposed to a get with a timeout.
			RetrievalResult retrievalresult = retrievalResultFuture.get();
			if (retrievalresult.hasNoData()) {
				logger.debug(
						"Skipping as we have not data from " + retrievalresult.getRetrievalRequest().getDescription()
								+ " for pv " + retrievalresult.getRetrievalRequest().getPvName());
				continue;
			}

			// Process the data retrieval calls.
			List<Callable<EventStream>> callables = retrievalresult.getResultStreams();
			for (Callable<EventStream> wrappedCallable : callables) {
				Future<EventStream> submit = executorResult.executorService.submit(wrappedCallable);
				eventStreamFutures.add(submit);
			}
		}
		return eventStreamFutures;
	}

	/**
	 * Merges info from pvTypeTnfo that comes from the config database into the
	 * remote description that gets sent over the wire.
	 * 
	 * @param typeInfo
	 * @param eventDesc
	 * @param engineMetaData
	 *            - Latest from the engine - could be null
	 * @return
	 * @throws IOException
	 */
	private void mergeTypeInfo(PVTypeInfo typeInfo, EventStreamDesc eventDesc, HashMap<String, String> engineMetaData)
			throws IOException {
		if (typeInfo != null && eventDesc != null && eventDesc instanceof RemotableEventStreamDesc) {
			logger.debug("Merging typeinfo into remote desc for pv " + eventDesc.getPvName() + " into source "
					+ eventDesc.getSource());
			RemotableEventStreamDesc remoteDesc = (RemotableEventStreamDesc) eventDesc;
			remoteDesc.mergeFrom(typeInfo, engineMetaData);
		}
	}

	/**
	 * Resolve all data sources and submit them to the executor in the
	 * executorResult This returns a list of futures of retrieval results.
	 * 
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
			PostProcessor postProcessor, ApplianceInfo applianceForPV, BasicContext retrievalContext,
			RetrievalExecutorResult executorResult) throws IOException {

		LinkedList<Future<RetrievalResult>> retrievalResultFutures = new LinkedList<Future<RetrievalResult>>();

		/*
		 * Gets the object responsible for resolving data sources (e.g., where data is
		 * stored for this appliance.
		 */
		DataSourceResolution datasourceresolver = new DataSourceResolution(configService);

		for (TimeSpan timespan : executorResult.requestTimespans) {
			// Resolve data sources for the given PV and the given time frames
			LinkedList<UnitOfRetrieval> unitsofretrieval = datasourceresolver.resolveDataSources(pvName,
					timespan.getStartTime(), timespan.getEndTime(), typeInfo, retrievalContext, postProcessor, null,
					null, applianceForPV);
			// Submit the units of retrieval to the executor service. This will give us a
			// bunch of Futures.
			for (UnitOfRetrieval unitofretrieval : unitsofretrieval) {
				// unitofretrieval implements a call() method as it extends Callable<?>
				retrievalResultFutures.add(executorResult.executorService.submit(unitofretrieval));
			}
		}
		return retrievalResultFutures;
	}
}
