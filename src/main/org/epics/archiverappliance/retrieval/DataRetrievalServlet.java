/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;


import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo.Builder;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.python.core.PyDictionary;
import org.python.core.PyList;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.epics.archiverappliance.retrieval.RetrievalError.check;
import static org.epics.archiverappliance.retrieval.RetrievalError.logAndRespond;

/**
 * Main servlet for retrieval of data.
 * All data retrieval is funneled thru here.
 *
 * @author mshankar
 */
public class DataRetrievalServlet extends HttpServlet {
    private static final String ARCH_APPL_PING_PV = "ArchApplPingPV";
    private static final String SEARCH_STORE_FOR_RETIRED_PV_STR =
            "org.epics.archiverappliance.retrieval.SearchStoreForRetiredPvs";
    private static final Logger logger = LogManager.getLogger(DataRetrievalServlet.class.getName());
    private static final HashMap<String, MimeMappingInfo> mimeresponses = new HashMap<String, MimeMappingInfo>();
    private static ConfigService configService = null;

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

    private static void processPingPV(HttpServletResponse resp) throws IOException {
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

            for (int i = 0; i < 10; i++) {
                ByteArray val = new SimulationEvent(0, currYear, ArchDBRTypes.DBR_SCALAR_DOUBLE, new ScalarValue<Double>(0.1 * i)).getRawForm();
                os.write(val.data, val.off, val.len);
                os.write(LineEscaper.NEWLINE_CHAR);
            }
        } finally {
            try {
                os.flush();
                os.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Determine the thread pool to be used for post processing based on some characteristics of the request
     * The plugins will yield a list of callables that could potentially be evaluated in parallel
     * Whether we evaluate in parallel is made here.
     *
     * @param pvName        Name of pv
     * @param postProcessor post processor
     * @return result
     */
    private static RetrievalExecutorResult determineExecutorForPostProcessing(String pvName, PVTypeInfo typeInfo, LinkedList<TimeSpan> requestTimes, HttpServletRequest req, PostProcessor postProcessor) {
        long memoryConsumption = postProcessor.estimateMemoryConsumption(pvName, typeInfo, requestTimes.getFirst().getStartTime(), requestTimes.getLast().getEndTime(), req);
        double memoryConsumptionInMB = (double) memoryConsumption / (1024 * 1024);
        DecimalFormat twoSignificantDigits = new DecimalFormat("###,###,###,###,###,###.##");
        logger.debug("Memory consumption estimate from postprocessor for pv " + pvName + " is " + memoryConsumption + "(bytes) ~= " + twoSignificantDigits.format(memoryConsumptionInMB) + "(MB)");

        // For now, we only use the current thread to execute in serial.
        // Once we get the unit tests for the post processors in a more rigorous shape, we can start using the ForkJoinPool.
        // There are some complexities in using the ForkJoinPool - in this case, we need to convert to using synchronized versions of the SummaryStatistics and DescriptiveStatistics
        // We also still have the issue where we can add a sample twice because of the non-transactional nature of ETL.
        // However, there is a lot of work done by the PostProcessors in estimateMemoryConsumption so leave this call in place.
        return new RetrievalExecutorResult(new CurrentThreadExecutorService(), requestTimes);
    }

    static Timestamp fromString(String timestampString, Timestamp defaultTime) throws IllegalArgumentException {

        // ISO datetimes are of the form "2011-02-02T08:00:00.000Z"
        Timestamp res = defaultTime;
        if (timestampString != null) {
            try {
                res = TimeUtils.convertFromISO8601String(timestampString);
            } catch (IllegalArgumentException ex) {
                res = TimeUtils.convertFromDateTimeStringWithOffset(timestampString);
            }
        }
        return res;

    }

    private static void consolidateEventStream(HttpServletResponse resp, String pvName, PostProcessor postProcessor, MergeDedupConsumer mergeDedupCountingConsumer) throws Exception {
        if (postProcessor instanceof PostProcessorWithConsolidatedEventStream) {
            try (EventStream eventStream = ((PostProcessorWithConsolidatedEventStream) postProcessor).getConsolidatedEventStream()) {
                EventStreamDesc sourceDesc = eventStream.getDescription();
                if (sourceDesc == null) {
                    logger.error("Skipping event stream without a desc for pv " + pvName + " and post processor " + postProcessor.getExtension());
                } else {
                    mergeDedupCountingConsumer.consumeEventStream(eventStream);
                    resp.flushBuffer();
                }
            }
        }
    }

    private static void consolidateEventStream(HttpServletResponse resp, PostProcessor postProcessor, MergeDedupConsumer mergeDedupCountingConsumer, EventStreamDesc sourceDesc, EventStream eventStream) {
        try {
            // If the postProcessor does not have a consolidated event stream, we send each eventstream across as we encounter it.
            // Else we send the consolidatedEventStream down below.
            if (!(postProcessor instanceof PostProcessorWithConsolidatedEventStream)) {
                mergeDedupCountingConsumer.consumeEventStream(eventStream);
                resp.flushBuffer();
            }
        } catch (Exception ex) {
            if (ex.toString() != null && ex.toString().contains("ClientAbortException")) {
                // We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
                logger.debug("Exception when consuming and flushing data from " + sourceDesc.getSource(), ex);
            } else {
                logger.error("Exception when consuming and flushing data from " + sourceDesc.getSource() + "-->" + ex, ex);
            }
        }
    }

    private static String removeVal(String pvName) {
        if (pvName.endsWith(".VAL")) {
            int len = pvName.length();
            pvName = pvName.substring(0, len - 4);
            logger.debug("Removing .VAL from pvName for request giving " + pvName);
        }
        return pvName;
    }

    private static boolean isFetchLatestMetadata(HttpServletRequest req) {
        boolean fetchLatestMetadata = false;
        String fetchLatestMetadataStr = req.getParameter("fetchLatestMetadata");
        if (fetchLatestMetadataStr != null && fetchLatestMetadataStr.equals("true")) {
            logger.debug("Adding a call to the engine to fetch the latest metadata");
            fetchLatestMetadata = true;
        }
        return fetchLatestMetadata;
    }

    private static boolean useChunkedEncoding(HttpServletRequest req) {
        boolean useChunkedEncoding = true;
        String doNotChunkStr = req.getParameter("donotchunk");
        if (doNotChunkStr != null && !doNotChunkStr.equals("false")) {
            logger.debug("Turning off HTTP chunked encoding");
            useChunkedEncoding = false;
        }
        return useChunkedEncoding;
    }

    private static boolean useReduced(HttpServletRequest req) {
        boolean useReduced = false;
        String useReducedStr = req.getParameter("usereduced");
        if (useReducedStr != null && !useReducedStr.equals("")) {
            try {
                useReduced = Boolean.parseBoolean(useReducedStr);
            } catch (Exception ex) {
                logger.error("Exception parsing usereduced", ex);
            }
        }
        return useReduced;
    }


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        String[] pathnameSplit = req.getPathInfo().split("/");
        String requestName = (pathnameSplit[pathnameSplit.length - 1].split("\\."))[0];

        if (requestName.equals("getData")) {
            logger.debug("User requesting data for single PV");
            doGetSinglePV(req, resp);
        } else if (requestName.equals("getDataForPVs")) {
            logger.debug("User requesting data for multiple PVs");
            doGetMultiPV(req, resp);
        } else {
            String msg = "\"" + requestName + "\" is not a valid API method.";
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String[] pathnameSplit = req.getPathInfo().split("/");
        String requestName = (pathnameSplit[pathnameSplit.length - 1].split("\\."))[0];

        switch (requestName) {
            case "getDataForPVs" -> {
                logger.debug("User requesting data for multiple PVs");
                doGetMultiPV(req, resp);
            }
            case "getDataAtTime" -> {
                try {
                    GetDataAtTime.getDataAtTime(req, resp, configService);
                } catch (ExecutionException | InterruptedException ex) {
                    throw new IOException(ex);
                }
            }
            case "getDataAtTimeForAppliance" -> {
                try {
                    GetDataAtTime.getDataAtTimeForAppliance(req, resp, configService);
                } catch (ExecutionException | InterruptedException ex) {
                    throw new IOException(ex);
                }
            }
            default -> {
                String msg = "\"" + requestName + "\" is not a valid API method.";
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
            }
        }
    }

    private void doGetSinglePV(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        PoorMansProfiler pmansProfiler = new PoorMansProfiler();
        String pvName = req.getParameter("pv");

        if (check(configService.getStartupState() != STARTUP_SEQUENCE.STARTUP_COMPLETE,
                "Cannot process data retrieval requests for PV " + pvName + " until the appliance has completely started up.",
                resp,
                HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            return;

        boolean useReduced = useReduced(req);
        String extension = req.getPathInfo().split("\\.")[1];
        logger.info("Mime is {}", extension);

        boolean useChunkedEncoding = useChunkedEncoding(req);

        boolean fetchLatestMetadata = isFetchLatestMetadata(req);

        // For data retrieval we need a PV info. However, in case of PV's that have long since retired, we may not want to have PVTypeInfo's in the system.
        // So, we support a template PV that lays out the data sources.
        // During retrieval, you can pass in the PV as a template and we'll clone this and make a temporary copy.
        String retiredPVTemplate = req.getParameter("retiredPVTemplate");

        if (check(pvName == null, "PV name is null.", resp, HttpServletResponse.SC_BAD_REQUEST))
            return;


        logger.debug("pvName is {}", pvName);
        assert pvName != null;
        if (pvName.equals(ARCH_APPL_PING_PV)) {
            logger.debug("Processing ping PV - this is used to validate the connection with the client.");
            processPingPV(resp);
            return;
        }

        RequestTimes requestTimesOb = getRequestTimes(req, resp, pvName);

        logger.debug("requestTimesOb is {}", requestTimesOb);
        if (requestTimesOb == null) return;

        assert (!requestTimesOb.requestTimes().isEmpty());
        String pp = req.getParameter("pp");
        StringBuilder postProcessorUserArg = new StringBuilder();
        if (pp != null) {
            postProcessorUserArg = new StringBuilder(pp);
        }
        if (pvName.contains("(")) {
            logger.debug("pv name contains (");
            if (!pvName.contains(")")) {
                logAndRespond("Unbalanced paran " + pvName, null, resp, HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            String[] components = pvName.split("[(,)]");
            postProcessorUserArg = new StringBuilder(components[0]);
            pvName = components[1];
            if (components.length > 2) {
                for (int i = 2; i < components.length; i++) {
                    postProcessorUserArg.append("_").append(components[i]);
                }
            }
            logger.debug("After parsing the function call syntax pvName is " + pvName + " and postProcessorUserArg is " + postProcessorUserArg);
        }


        logger.debug("postProcessorUserArg times is {}", postProcessorUserArg);
        PostProcessor postProcessor = PostProcessors.findPostProcessor(postProcessorUserArg.toString());

        String pvNameFromRequest = pvName;

        pvName = removeVal(pvName);

        PVTypeInfo typeInfo = PVNames.determineAppropriatePVTypeInfo(pvName, configService);
        pmansProfiler.mark("After PVTypeInfo");

        typeInfo = getPvTypeInfo(req, resp, pvName, retiredPVTemplate, requestTimesOb, typeInfo);

        if (typeInfo == null) {
            logAndRespond("Unable to find typeinfo for pv " + pvName, null, resp, HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        postProcessor = reducePostprocessor(useReduced, postProcessor);

        ApplianceInfo applianceForPV = configService.getApplianceForPV(pvName);
        if (applianceForPV == null) {
            // TypeInfo cannot be null here...
            applianceForPV = configService.getAppliance(typeInfo.getApplianceIdentity());
        }

        if (!applianceForPV.equals(configService.getMyApplianceInfo())) {
            // Data for pv is elsewhere. Proxy/redirect and return.
            proxyRetrievalRequest(req, resp, pvName, applianceForPV.getRetrievalURL() + "/../data");
            return;
        }

        pmansProfiler.mark("After Appliance Info");

        String fieldName = PVNames.getFieldName(pvName);
        if (fieldName != null && !fieldName.equals("") && typeInfo.checkIfFieldAlreadySepcified(fieldName)) {
            logger.debug("We reset the pvName " + pvName + " to one from the typeinfo " + typeInfo.getPvName() + " as that determines the name of the stream. Also using ExtraFieldsPostProcessor");
            pvName = typeInfo.getPvName();
            postProcessor = new ExtraFieldsPostProcessor(fieldName);
        }

        try {
            // Postprocessors get their mandatory arguments from the request.
            // If user does not pass in the expected request, throw an exception.
            postProcessor.initialize(postProcessorUserArg.toString(), pvName);
        } catch (Exception ex) {
            logger.error("Postprocessor threw an exception during initialization for " + pvName, ex);
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        try (BasicContext retrievalContext = new BasicContext(typeInfo.getDBRType(), pvNameFromRequest);
             MergeDedupConsumer mergeDedupCountingConsumer = createMergeDedupConsumer(resp, extension);
             RetrievalExecutorResult executorResult = determineExecutorForPostProcessing(pvName, typeInfo, requestTimesOb.requestTimes(), req, postProcessor)
        ) {
            HashMap<String, String> engineMetadata = null;
            if (fetchLatestMetadata && typeInfo.getSamplingMethod() != SamplingMethod.DONT_ARCHIVE) {
                // Make a call to the engine to fetch the latest metadata; skip external servers, template PVs and the like by checking the sampling method.
                engineMetadata = fetchLatestMedataFromEngine(pvName, applianceForPV);
            }


            LinkedList<Future<RetrievalResult>> retrievalResultFutures = resolveAllDataSources(pvName, typeInfo, postProcessor, applianceForPV, retrievalContext, executorResult, req);
            pmansProfiler.mark("After data source resolution");


            long s1 = System.currentTimeMillis();
            String currentlyProcessingPV = null;

            List<Future<EventStream>> eventStreamFutures = getEventStreamFuturesFromRetrievalResults(executorResult, retrievalResultFutures);

            logger.debug("Done with the RetrievalResult's; moving onto the individual event stream from each source for " + pvName);
            pmansProfiler.mark("After retrieval results");

            evaluateEventStreamFutures(resp, pmansProfiler, pvName, requestTimesOb, postProcessor, typeInfo, retrievalContext, mergeDedupCountingConsumer, engineMetadata, currentlyProcessingPV, eventStreamFutures);

            consolidateEventStream(resp, pvName, postProcessor, mergeDedupCountingConsumer);

            // If the postProcessor needs to send final data across, give it a chance now...
            if (postProcessor instanceof AfterAllStreams) {
                EventStream finalEventStream = ((AfterAllStreams) postProcessor).anyFinalData();
                if (finalEventStream != null) {
                    mergeDedupCountingConsumer.consumeEventStream(finalEventStream);
                    resp.flushBuffer();
                }
            }

            pmansProfiler.mark("After writing all eventstreams to response");

            long s2 = System.currentTimeMillis();
            logger.info("For the complete request, found a total of " + mergeDedupCountingConsumer.totalEventsForAllPVs + " in " + (s2 - s1) + "(ms)"
                    + " skipping " + mergeDedupCountingConsumer.skippedEventsForAllPVs + " events"
                    + " deduping involved " + mergeDedupCountingConsumer.comparedEventsForAllPVs + " compares.");
        } catch (Exception ex) {
            logger.error("Exception when retrieving data ", ex);
        }
        pmansProfiler.mark("After all closes and flushing all buffers");

        // Till we determine all the if conditions where we log this, we log sparingly..
        if (pmansProfiler.totalTimeMS() > 5000) {
            logger.error("Retrieval time for " + pvName + " from " + requestTimesOb.start + " to " + requestTimesOb.end + pmansProfiler);
        }
    }

    private void evaluateEventStreamFutures(HttpServletResponse resp, PoorMansProfiler pmansProfiler, String pvName, RequestTimes requestTimesOb, PostProcessor postProcessor, PVTypeInfo typeInfo, BasicContext retrievalContext, MergeDedupConsumer mergeDedupCountingConsumer, HashMap<String, String> engineMetadata, String currentlyProcessingPV, List<Future<EventStream>> eventStreamFutures) {
        for (Future<EventStream> future : eventStreamFutures) {
            EventStreamDesc sourceDesc = null;
            try (EventStream eventStream = future.get()) {
                // Reset it for each loop iteration.
                sourceDesc = eventStream.getDescription();
                if (sourceDesc == null) {
                    logger.warn("Skipping event stream without a desc for pv " + pvName);
                    continue;
                }

                logger.debug("Processing event stream for pv " + pvName + " from source " + ((eventStream.getDescription() != null) ? eventStream.getDescription().getSource() : " unknown"));

                if (mergeTypeInfo(typeInfo, engineMetadata, sourceDesc))
                    continue;

                if (currentlyProcessingPV == null || !currentlyProcessingPV.equals(pvName)) {
                    logger.debug("Switching to new PV " + pvName + " In some mime responses we insert special headers at the beginning of the response. Calling the hook for that");
                    currentlyProcessingPV = pvName;
                    mergeDedupCountingConsumer.processingPV(retrievalContext, currentlyProcessingPV, requestTimesOb.start(), requestTimesOb.end(), (eventStream != null) ? sourceDesc : null);
                }

                consolidateEventStream(resp, postProcessor, mergeDedupCountingConsumer, sourceDesc, eventStream);
                pmansProfiler.mark("After event stream " + eventStream.getDescription().getSource());
            } catch (Exception ex) {
                if (ex.toString() != null && ex.toString().contains("ClientAbortException")) {
                    // We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
                    logger.debug("Exception when consuming and flushing data from " + (sourceDesc != null ? sourceDesc.getSource() : "N/A"), ex);
                } else {
                    logger.error("Exception when consuming and flushing data from " + (sourceDesc != null ? sourceDesc.getSource() : "N/A") + "-->" + ex, ex);
                }
            }
        }
    }

    private boolean mergeTypeInfo(PVTypeInfo typeInfo, HashMap<String, String> engineMetadata, EventStreamDesc sourceDesc) throws IOException {
        try {
            mergeTypeInfo(typeInfo, sourceDesc, engineMetadata);
        } catch (MismatchedDBRTypeException mex) {
            logger.error(mex.getMessage(), mex);
            return true;
        }
        return false;
    }

    private PostProcessor reducePostprocessor(boolean useReduced, PostProcessor postProcessor) {
        if (postProcessor == null && useReduced) {
            String defaultPPClassName = configService.getInstallationProperties().getProperty("org.epics.archiverappliance.retrieval.DefaultUseReducedPostProcessor", FirstSamplePP.class.getName());
            logger.debug("Using the default usereduced preprocessor " + defaultPPClassName);
            try {
                postProcessor = (PostProcessor) Class.forName(defaultPPClassName).getConstructor().newInstance();
            } catch (Exception ex) {
                logger.error("Exception constructing new instance of post processor " + defaultPPClassName, ex);
            }

        }

        if (postProcessor == null) {
            logger.debug("Using the default raw preprocessor");
            postProcessor = new DefaultRawPostProcessor();
        }
        return postProcessor;
    }

    private PVTypeInfo getPvTypeInfo(HttpServletRequest req, HttpServletResponse resp, String pvName, String retiredPVTemplate, RequestTimes requestTimesOb, PVTypeInfo typeInfo) throws IOException {
        typeInfo = getPvTypeInfo(req, pvName, requestTimesOb, typeInfo);

        if (typeInfo == null && (resp.isCommitted())) {
            logger.debug("Proxied the data thru an external server for PV " + pvName);
            return null;
        }

        typeInfo = getPvTypeInfo(pvName, retiredPVTemplate, typeInfo);
        return typeInfo;
    }

    private PVTypeInfo getPvTypeInfo(String pvName, String retiredPVTemplate, PVTypeInfo typeInfo) throws IOException {
        if (typeInfo == null) {
            if (retiredPVTemplate != null) {
                PVTypeInfo templateTypeInfo = PVNames.determineAppropriatePVTypeInfo(retiredPVTemplate, configService);
                if (templateTypeInfo != null) {
                    typeInfo = new PVTypeInfo(pvName, templateTypeInfo);
                    typeInfo.setPaused(true);
                    typeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
                    // Somehow tell the code downstream that this is a fake typeInfo.
                    typeInfo.setSamplingMethod(SamplingMethod.DONT_ARCHIVE);
                    logger.debug("Using a template PV for {}. Need to determine the actual DBR type.", pvName);
                    setActualDBRTypeFromData(pvName, typeInfo, configService);
                }
            } else if (Boolean.TRUE.equals(isSearchStoreForRetiredPvs())) {
                typeInfo = createDefaultPVTypeInfoFromPolicies(pvName);
                boolean foundRetiredPV = setActualDBRTypeFromData(pvName, typeInfo, configService);

                // If the PV does not exist in the dataStores at all then
                // set the typeInfo back to null so it will return no data
                if (!foundRetiredPV) {
                    typeInfo = null;
                }
            }
        }
        return typeInfo;
    }

    private PVTypeInfo getPvTypeInfo(HttpServletRequest req, String pvName, RequestTimes requestTimesOb, PVTypeInfo typeInfo) throws IOException {
        if (typeInfo == null && RetrievalState.includeExternalServers(req)) {
            logger.debug("Checking to see if pv {} is served by a external Archiver Server", pvName);
            typeInfo = checkIfPVisServedByExternalServer(pvName, requestTimesOb.start(), requestTimesOb.end());
        }
        return typeInfo;
    }

    private RequestTimes getRequestTimes(HttpServletRequest req, HttpServletResponse resp, String pvName) throws IOException {
        // ISO datetimes are of the form "2011-02-02T08:00:00.000Z"
        Timestamp end;
        try {
            end = fromString(req.getParameter("to"), TimeUtils.plusHours(TimeUtils.now(), 1));
        } catch (IllegalArgumentException ex) {
            logAndRespond("Cannot parse time" + req.getParameter("to"), ex, resp, HttpServletResponse.SC_BAD_REQUEST);
            return null;
        }
        // ISO datetimes are of the form "2011-02-02T08:00:00.000Z"
        Timestamp start;
        try {
            start = fromString(req.getParameter("from"), TimeUtils.minusDays(end, 1));
        } catch (IllegalArgumentException ex) {
            logAndRespond("Cannot parse time" + req.getParameter("from"), ex, resp, HttpServletResponse.SC_BAD_REQUEST);
            return null;
        }

        if (end.before(start)) {
            logAndRespond("For request, end " + end + " is before start " + start + " for pv " + pvName, null, resp, HttpServletResponse.SC_BAD_REQUEST);
            return null;
        }

        return getRequestTimes(req, resp, pvName, end, start);
    }

    private RequestTimes getRequestTimes(HttpServletRequest req, HttpServletResponse resp, String pvName, Timestamp end, Timestamp start) throws IOException {
        LinkedList<TimeSpan> requestTimes = new LinkedList<>();

        // We can specify a list of time stamp pairs using the optional timeranges parameter
        String timeRangesStr = req.getParameter("timeranges");
        if (timeRangesStr != null) {
            boolean continueWithRequest = parseTimeRanges(resp, pvName, requestTimes, timeRangesStr);
            if (!continueWithRequest) {
                // Cannot parse the time ranges properly; we so abort the request.
                return null;
            }

            // Override the start and the end so that the mergededup consumer works correctly.
            start = requestTimes.getFirst().getStartTime();
            end = requestTimes.getLast().getEndTime();

        } else {
            requestTimes.add(new TimeSpan(start, end));
        }
        return new RequestTimes(end, start, requestTimes);
    }


    private void doGetMultiPV(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        PoorMansProfiler pmansProfiler = new PoorMansProfiler();

        // Gets the list of PVs specified by the `pv` parameter
        // String arrays might be inefficient for retrieval. In any case, they are sorted, which is essential later on.
        List<String> pvNames = null;
        if (req.getMethod().equals("POST")) {
            pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req, configService);
        } else {
            pvNames = Arrays.asList(req.getParameterValues("pv"));
        }

        // Ensuring that the AA has finished starting up before requests are accepted.
        if (check(configService.getStartupState() != STARTUP_SEQUENCE.STARTUP_COMPLETE, "Cannot process data retrieval requests for specified PVs (" + StringUtils.join(pvNames, ", ")
                + ") until the appliance has completely started up.", resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE))
            return;

        // Getting various fields from arguments
        boolean useReduced = useReduced(req);

        // Getting MIME type
        String extension = req.getPathInfo().split("\\.")[1];
        logger.info("Mime is {}", extension);

        if (!extension.equals("json") && !extension.equals("raw") && !extension.equals("jplot") && !extension.equals("qw")) {
            String msg = "Mime type " + extension + " is not supported. Please use \"json\", \"jplot\" or \"raw\".";
            logAndRespond(msg, null, resp, HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        boolean useChunkedEncoding = useChunkedEncoding(req);

        boolean fetchLatestMetadata = isFetchLatestMetadata(req);

        // For data retrieval we need a PV info. However, in case of PV's that have long since retired, we may not want to have PVTypeInfo's in the system.
        // So, we support a template PV that lays out the data sources.
        // During retrieval, you can pass in the PV as a template and we'll clone this and make a temporary copy.
        String retiredPVTemplate = req.getParameter("retiredPVTemplate");

        // Goes through given PVs and returns bad request error.
        int nullPVs = 0;
        for (String pvName : pvNames) {
            if (pvName == null) {
                nullPVs++;
            }
            if (nullPVs > 0) {
                logger.warn("Some PVs are null in the request.");
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
        }

        if (pvNames.toString().matches("^.*" + ARCH_APPL_PING_PV + ".*$")) {
            logger.debug("Processing ping PV - this is used to validate the connection with the client.");
            processPingPV(resp);
            return;
        }

        RequestTimes requestTimesOb = getRequestTimes(req, resp, pvNames.toString());
        assert (!Objects.requireNonNull(requestTimesOb).requestTimes.isEmpty());

        // Get a post processor for each PV specified in pvNames
        // If PV in the form <pp>(<pv>), process it
        String postProcessorUserArg = req.getParameter("pp");
        List<String> postProcessorUserArgs = new ArrayList<>(pvNames.size());
        List<PostProcessor> postProcessors = new ArrayList<>(pvNames.size());
        for (int i = 0; i < pvNames.size(); i++) {
            postProcessorUserArgs.add(postProcessorUserArg);

            if (pvNames.get(i).contains("(")) {
                if (check(!pvNames.get(i).contains(")"), "Unbalanced paren " + pvNames.get(i), resp, HttpServletResponse.SC_BAD_REQUEST))
                    return;
                String[] components = pvNames.get(i).split("[(,)]");
                postProcessorUserArg = components[0];
                postProcessorUserArgs.set(i, postProcessorUserArg);
                pvNames.set(i, components[1]);
                if (components.length > 2) {
                    for (int j = 2; j < components.length; j++) {
                        postProcessorUserArgs.set(i, postProcessorUserArgs.get(i) + "_" + components[j]);
                    }
                }
                logger.info("After parsing the function call syntax pvName is " + pvNames.get(i) + " and postProcessorUserArg is " + postProcessorUserArg);
            }
            postProcessors.add(PostProcessors.findPostProcessor(postProcessorUserArg));
        }

        List<String> pvNamesFromRequests = new ArrayList<String>(pvNames.size());
        pvNamesFromRequests.addAll(pvNames);

        for (int i = 0; i < pvNames.size(); i++) {
            if (pvNames.get(i).endsWith(".VAL")) {
                int len = pvNames.get(i).length();
                pvNames.set(i, pvNames.get(i).substring(0, len - 4));
                logger.info("Removing .VAL from pvName for request giving " + pvNames.get(i));
            }
        }

        List<PVTypeInfo> typeInfos = new ArrayList<PVTypeInfo>(pvNames.size());
        for (String name : pvNames) {
            typeInfos.add(PVNames.determineAppropriatePVTypeInfo(name, configService));
        }
        pmansProfiler.mark("After PVTypeInfo");

        for (int i = 0; i < pvNames.size(); i++)
            if (typeInfos.get(i) == null && RetrievalState.includeExternalServers(req)) {
                logger.debug("Checking to see if pv " + pvNames.get(i) + " is served by a external Archiver Server");
                typeInfos.set(i, checkIfPVisServedByExternalServer(pvNames.get(i), requestTimesOb.start, requestTimesOb.end));
            }

        for (int i = 0; i < pvNames.size(); i++) {
            typeInfos.set(i, getPvTypeInfo(pvNames.get(i), retiredPVTemplate, typeInfos.get(i)));

            if (check(typeInfos.get(i) == null, "Unable to find typeinfo for pv " + pvNames.get(i), resp, HttpServletResponse.SC_NOT_FOUND))
                return;
            postProcessors.set(i, reducePostprocessor(useReduced, postProcessors.get(i)));
        }

        // Get the appliances for each of the PVs
        List<ApplianceInfo> applianceForPVs = new ArrayList<ApplianceInfo>(pvNames.size());
        for (int i = 0; i < pvNames.size(); i++) {
            applianceForPVs.add(configService.getApplianceForPV(pvNames.get(i)));
            if (applianceForPVs.get(i) == null) {
                // TypeInfo cannot be null here...
                assert (typeInfos.get(i) != null);
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
        if (!retrievalURLs.isEmpty()) {
            // Get list of PVs and redirect them to appropriate appliance to be retrieved.
            String retrievalURL;
            ArrayList<PVInfoForClusterRetrieval> pvInfos;
            while ((retrievalURL = retrievalURLs.iterator().next()) == null) {
                // Get array list of PVs for appliance
                pvInfos = applianceToPVs.get(retrievalURL);
                try {
                    List<List<Future<EventStream>>> resultFromForeignAppliances
                            = retrieveEventStreamFromForeignAppliance(req, resp, pvInfos, requestTimesOb.requestTimes
                    );
                    listOfEventStreamFuturesLists.addAll(resultFromForeignAppliances);
                } catch (Exception ex) {
                    logger.error("Failed to retrieve " + StringUtils.join(pvNames, ", ") + " from " + retrievalURL + ".");
                    return;
                }
            }
        }

        pmansProfiler.mark("After Appliance Info");

        // Setting post processor for PVs, taking into account whether there is a field in the PV name
        for (int i = 0; i < pvNames.size(); i++) {
            String pvName = pvNames.get(i);
            PVTypeInfo typeInfo = typeInfos.get(i);
            postProcessorUserArg = postProcessorUserArgs.get(i);

            // If a field is specified in a PV name, it will create a post processor for that
            String fieldName = PVNames.getFieldName(pvName);
            if (fieldName != null && !fieldName.equals("") && !pvName.equals(typeInfo.getPvName())) {
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
            mergeDedupCountingConsumer = createMergeDedupConsumer(resp, extension);
        } catch (ServletException se) {
            String msg = "Exception when retrieving data " + "-->" + se;
            logger.error(msg, se);
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
                if (fetchLatestMetadata && typeInfos.get(i).getSamplingMethod() != SamplingMethod.DONT_ARCHIVE) {
                    // Make a call to the engine to fetch the latest metadata; skip external servers, template PVs and the like by checking the sampling method.
                    engineMetadatas.add(fetchLatestMedataFromEngine(pvNames.get(i), applianceForPVs.get(i)));
                }
                retrievalContexts.add(new BasicContext(typeInfos.get(i).getDBRType(), pvNamesFromRequests.get(i)));
                executorResults.add(determineExecutorForPostProcessing(pvNames.get(i), typeInfos.get(i), requestTimesOb.requestTimes, req, postProcessors.get(i)));
            }

            /*
             * There are as many Future objects in the eventStreamFutures List as there are periods over
             * which to fetch data. Retrieval of data happen here in parallel.
             */
            List<LinkedList<Future<RetrievalResult>>> listOfRetrievalResultFuturesLists = new ArrayList<LinkedList<Future<RetrievalResult>>>();
            for (int i = 0; i < pvNames.size(); i++) {
                listOfRetrievalResultFuturesLists.add(resolveAllDataSources(pvNames.get(i), typeInfos.get(i), postProcessors.get(i),
                        applianceForPVs.get(i), retrievalContexts.get(i), executorResults.get(i), req));
            }
            pmansProfiler.mark("After data source resolution");

            for (int i = 0; i < pvNames.size(); i++) {
                // Data is retrieved here
                List<Future<EventStream>> eventStreamFutures = getEventStreamFuturesFromRetrievalResults(executorResults.get(i),
                        listOfRetrievalResultFuturesLists.get(i));
                listOfEventStreamFuturesLists.add(eventStreamFutures);
            }

        } catch (Exception ex) {
            logger.error("Exception when retrieving data ", ex);
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
                try (BasicContext retrievalContext = new BasicContext(typeInfos.get(i).getDBRType(), pvNamesFromRequests.get(i))) {
                    List<Future<EventStream>> eventStreamFutures = listOfEventStreamFuturesLists.get(i);
                    String pvName = pvNames.get(i);
                    PVTypeInfo typeInfo = typeInfos.get(i);
                    HashMap<String, String> engineMetadata = fetchLatestMetadata ? engineMetadatas.get(i) : null;
                    PostProcessor postProcessor = postProcessors.get(i);

                    logger.debug("Done with the RetrievalResults; moving onto the individual event stream "
                            + "from each source for " + StringUtils.join(pvNames, ", "));
                    pmansProfiler.mark("After retrieval results");
                    evaluateEventStreamFutures(resp, pmansProfiler, pvName, requestTimesOb, postProcessor, typeInfo, retrievalContext, mergeDedupCountingConsumer, engineMetadata, currentlyProcessingPV, eventStreamFutures);

                    consolidateEventStream(resp, pvName, postProcessor, mergeDedupCountingConsumer);

                    // If the postProcessor needs to send final data across, give it a chance now...
                    if (postProcessor instanceof AfterAllStreams) {
                        EventStream finalEventStream = ((AfterAllStreams) postProcessor).anyFinalData();
                        if (finalEventStream != null) {
                            mergeDedupCountingConsumer.consumeEventStream(finalEventStream);
                            resp.flushBuffer();
                        }
                    }

                    pmansProfiler.mark("After writing all eventstreams to response");
                }
            }
        } catch (Exception ex) {
            logger.error("Exception when retrieving data ", ex);
        }

        long s2 = System.currentTimeMillis();
        logger.info("For the complete request, found a total of " + mergeDedupCountingConsumer.totalEventsForAllPVs + " in " + (s2 - s1) + "(ms)"
                + " skipping " + mergeDedupCountingConsumer.skippedEventsForAllPVs + " events"
                + " deduping involved " + mergeDedupCountingConsumer.comparedEventsForAllPVs + " compares.");

        pmansProfiler.mark("After all closes and flushing all buffers");

        // Till we determine all the if conditions where we log this, we log sparingly..
        if (pmansProfiler.totalTimeMS() / pvNames.size() > 5000) {
            logger.error("Retrieval time for " + StringUtils.join(pvNames, ", ") + " from " + requestTimesOb.start + " to " + requestTimesOb.end + ": " + pmansProfiler);
        }

        mergeDedupCountingConsumer.close();
    }

    /**
     * Given a list of retrievalResult futures, we loop thru these; execute them (basically calling the reader getData) and then sumbit the returned callables to the executorResult's executor.
     * We return a list of eventstream futures.
     *
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
        for (Future<RetrievalResult> retrievalResultFuture : retrievalResultFutures) {
            // This call blocks until the future is complete.
            // For now, we use a simple get as opposed to a get with a timeout.
            RetrievalResult retrievalresult = retrievalResultFuture.get();
            if (retrievalresult.hasNoData()) {
                logger.debug("Skipping as we have not data from " + retrievalresult.getRetrievalRequest().getDescription() + " for pv " + retrievalresult.getRetrievalRequest().getPvName());
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
     * Resolve all data sources and submit them to the executor in the executorResult
     * This returns a list of futures of retrieval results.
     *
     * @param pvName
     * @param typeInfo
     * @param postProcessor
     * @param applianceForPV
     * @param retrievalContext
     * @param executorResult
     * @param req
     * @return
     * @throws IOException
     */
    private LinkedList<Future<RetrievalResult>> resolveAllDataSources(String pvName, PVTypeInfo typeInfo,
                                                                      PostProcessor postProcessor, ApplianceInfo applianceForPV,
                                                                      BasicContext retrievalContext, RetrievalExecutorResult executorResult,
                                                                      HttpServletRequest req) throws IOException {

        LinkedList<Future<RetrievalResult>> retrievalResultFutures = new LinkedList<Future<RetrievalResult>>();

        /*
         * Gets the object responsible for resolving data sources (e.g., where data is stored
         * for this appliance.
         */
        DataSourceResolution datasourceresolver = new DataSourceResolution(configService);

        for (TimeSpan timespan : executorResult.requestTimespans) {
            // Resolve data sources for the given PV and the given time frames
            List<UnitOfRetrieval> unitsofretrieval = datasourceresolver.resolveDataSources(pvName, timespan.getStartTime(), timespan.getEndTime(), typeInfo, retrievalContext, postProcessor, req, applianceForPV);
            // Submit the units of retrieval to the executor service. This will give us a bunch of Futures.
            for (UnitOfRetrieval unitofretrieval : unitsofretrieval) {
                // unitofretrieval implements a call() method as it extends Callable<?>
                retrievalResultFutures.add(executorResult.executorService.submit(unitofretrieval));
            }
        }
        return retrievalResultFutures;
    }

    /**
     * Create a merge dedup consumer that will merge/dedup multiple event streams.
     * This basically makes sure that we are serving up events in monotonically increasing timestamp order.
     *
     * @param resp
     * @param extension
     * @return
     * @throws ServletException
     */
    private MergeDedupConsumer createMergeDedupConsumer(HttpServletResponse resp, String extension) throws ServletException {
        MergeDedupConsumer mergeDedupCountingConsumer;
        MimeMappingInfo mimemappinginfo = mimeresponses.get(extension);
        if (mimemappinginfo == null) {
            StringWriter supportedextensions = new StringWriter();
            for (String supportedextension : mimeresponses.keySet()) {
                supportedextensions.append(supportedextension).append(" ");
            }
            throw new ServletException("Cannot generate response of mime-type " + extension + ". Supported extensions are " + supportedextensions);
        } else {
            try {
                String ctype = mimeresponses.get(extension).contentType;
                resp.setContentType(ctype);
                logger.debug("Using {} as the mime response sending {}", mimemappinginfo.mimeresponseClass.getName(), ctype);
                MimeResponse mimeresponse = mimemappinginfo.mimeresponseClass.getConstructor().newInstance();
                OutputStream os = resp.getOutputStream();
                mergeDedupCountingConsumer = new MergeDedupConsumer(mimeresponse, os);
            } catch (Exception ex) {
                throw new ServletException(ex);
            }
        }
        return mergeDedupCountingConsumer;
    }

    /**
     * Check to see if the PV is served up by an external server.
     * If it is, make a typeInfo up and set the appliance as this appliance.
     * We should only call this method if the typeInfo for this PV is null.
     * We need the start time of the request as the ChannelArchiver does not serve up data if the starttime is much later than the last event in the dataset.
     * For external EPICS Archiver Appliances, we simply proxy the data right away. Use the response isCommited to see if we have already processed the request
     *
     * @param pvName
     * @param start
     * @return
     * @throws IOException
     */
    private PVTypeInfo checkIfPVisServedByExternalServer(String pvName, Timestamp start, Timestamp end) throws IOException {
        PVTypeInfo typeInfo = null;
        // See if external EPICS archiver appliances have this PV.
        Map<String, String> externalServers = configService.getExternalArchiverDataServers();
        if (externalServers != null) {
            for (var serverEntry : externalServers.entrySet()) {
                String serverUrl = serverEntry.getKey();
                String index = serverEntry.getValue();
                if (index.equals("pbraw")) {
                    serverUrl = serverUrl.split("\\?")[0];
                    logger.debug("Asking external EPICS Archiver Appliance " + serverUrl + " if it has data for pv " + pvName);
                    JSONObject areWeArchivingPVObj = GetUrlContent.getURLContentAsJSONObject(serverUrl + "/bpl/areWeArchivingPV?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8), false);
                    if (areWeArchivingPVObj != null) {
                        @SuppressWarnings("unchecked")
                        Map<String, String> areWeArchivingPV = areWeArchivingPVObj;
                        if (areWeArchivingPV.containsKey("status") && Boolean.parseBoolean(areWeArchivingPV.get("status"))) {
                            logger.info("Proxying data retrieval for pv " + pvName + " to " + serverUrl);
                            try (BasicContext context = new BasicContext()) {
                                StoragePlugin hplg = StoragePluginURLParser.parseStoragePlugin("pbraw://localhost?rawURL=" + serverUrl + "/data/getData.raw&name=ext", configService);
                                assert hplg != null;
                                logger.debug(hplg.getDescription());
                                List<Callable<EventStream>> callables = hplg.getDataForPV(context, pvName, TimeUtils.minusHours(end, 1), end, null);
                                if (callables == null || callables.isEmpty()) {
                                    logger.info("No data from remote server " + serverUrl + " for pv " + pvName);
                                } else {
                                    for (Callable<EventStream> callable : callables) {
                                        try (EventStream strm = callable.call()) {
                                            if (strm != null) {
                                                Iterator<Event> it = strm.iterator();
                                                if (it.hasNext()) {
                                                    Event e = it.next();
                                                    ArchDBRTypes dbrType = strm.getDescription().getArchDBRType();
                                                    typeInfo = new PVTypeInfo(pvName, dbrType, !dbrType.isWaveForm(), e.getSampleValue().getElementCount());
                                                    typeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
                                                    // Somehow tell the code downstream that this is a fake typeInfo.
                                                    typeInfo.setSamplingMethod(SamplingMethod.DONT_ARCHIVE);
                                                    typeInfo.setDataStores(new String[]{"pbraw://localhost?rawURL=" + serverUrl + "/data/getData.raw"});
                                                    logger.debug("Done creating a temporary typeinfo for pv " + pvName);
                                                    return typeInfo;
                                                }
                                            } else {
                                                logger.info("Empty stream from remote server" + serverUrl + " for pv " + pvName);
                                            }
                                        } catch (Exception ex) {
                                            logger.error("Exception trying to determine typeinfo for pv " + pvName + " from external server " + serverUrl, ex);
                                            typeInfo = null;
                                        }
                                    }
                                }
                            } catch (Exception ex) {
                                logger.error("Exception trying to determine typeinfo for pv " + pvName + " from external server " + serverUrl, ex);
                                typeInfo = null;
                            }
                        }
                    }
                }
            }
        }


        List<ChannelArchiverDataServerPVInfo> caServers = configService.getChannelArchiverDataServers(pvName);
        if (caServers != null && !caServers.isEmpty()) {
            try (BasicContext context = new BasicContext()) {
                for (ChannelArchiverDataServerPVInfo caServer : caServers) {
                    logger.debug(pvName + " is being server by " + caServer.toString() + " and typeinfo is null. Trying to make a typeinfo up...");
                    List<Callable<EventStream>> callables = caServer.getServerInfo().getPlugin().getDataForPV(context, pvName, TimeUtils.minusHours(start, 1), start, null);
                    if (callables != null && !callables.isEmpty()) {
                        try (EventStream strm = callables.get(0).call()) {
                            if (strm != null) {
                                Event e = strm.iterator().next();
                                if (e != null) {
                                    ArchDBRTypes dbrType = strm.getDescription().getArchDBRType();
                                    typeInfo = new PVTypeInfo(pvName, dbrType, !dbrType.isWaveForm(), e.getSampleValue().getElementCount());
                                    typeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
                                    // Somehow tell the code downstream that this is a fake typeInfo.
                                    typeInfo.setSamplingMethod(SamplingMethod.DONT_ARCHIVE);
                                    logger.debug("Done creating a temporary typeinfo for pv " + pvName);
                                    return typeInfo;
                                }
                            }
                        } catch (Exception ex) {
                            logger.error("Exception trying to determine typeinfo for pv " + pvName + " from CA " + caServer, ex);
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
     *
     * @param typeInfo
     * @param eventDesc
     * @param engineMetaData - Latest from the engine - could be null
     * @return
     * @throws IOException
     */
    private void mergeTypeInfo(PVTypeInfo typeInfo, EventStreamDesc eventDesc, HashMap<String, String> engineMetaData) throws IOException {
        if (typeInfo != null && eventDesc instanceof RemotableEventStreamDesc remoteDesc) {
            logger.debug("Merging typeinfo into remote desc for pv " + eventDesc.getPvName() + " into source " + eventDesc.getSource());
            remoteDesc.mergeFrom(typeInfo, engineMetaData);
        }
    }

    @Override
    public void init() throws ServletException {
        configService = (ConfigService) this.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
    }

    /**
     * Make a call to the engine to fetch the latest metadata and then add it to the mergeConsumer
     *
     * @param pvName
     * @param applianceForPV
     */
    @SuppressWarnings("unchecked")
    private HashMap<String, String> fetchLatestMedataFromEngine(String pvName, ApplianceInfo applianceForPV) {
        try {
            String metadataURL = applianceForPV.getEngineURL() + "/getMetadata?pv=" + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
            logger.debug("Getting metadata from the engine using " + metadataURL);
            return GetUrlContent.getURLContentAsJSONObject(metadataURL);
        } catch (Exception ex) {
            logger.warn("Exception fetching latest metadata for pv " + pvName, ex);
        }
        return null;
    }

    /**
     * If the pv is hosted on another appliance, proxy retrieval requests from that appliance
     * We expect to return immediately after this method.
     *
     * @param req
     * @param resp
     * @param pvName
     * @param dataRetrievalURLForPV
     * @throws IOException
     */
    private void proxyRetrievalRequest(HttpServletRequest req, HttpServletResponse resp, String pvName, String dataRetrievalURLForPV) throws IOException {
        try {
            // It may be beneficial to support both and choose based on where the client in calling from or perhaps from a header?
            boolean redirect = !Objects.equals(req.getHeader("redirect"), "false");
            if (redirect) {
                logger.info("Data for pv " + pvName + "is elsewhere. Redirecting to appliance " + dataRetrievalURLForPV);
                URI redirectURI = new URI(dataRetrievalURLForPV + "/" + req.getPathInfo());
                String redirectURIStr = redirectURI.normalize() + "?" + req.getQueryString();
                logger.debug("URI for redirect is " + redirectURIStr);
                resp.sendRedirect(redirectURIStr);
            } else {
                logger.info("Data for pv " + pvName + "is elsewhere. Proxying appliance " + dataRetrievalURLForPV);
                URI redirectURI = new URI(dataRetrievalURLForPV + "/" + req.getPathInfo());
                String redirectURIStr = redirectURI.normalize() + "?" + req.getQueryString();
                logger.debug("URI for proxying is " + redirectURIStr);

                try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
                    HttpGet getMethod = new HttpGet(redirectURIStr);
                    getMethod.addHeader("Connection", "close"); // https://www.nuxeo.com/blog/using-httpclient-properly-avoid-closewait-tcp-connections/
                    try (CloseableHttpResponse response = httpclient.execute(getMethod)) {
                        if (response.getStatusLine().getStatusCode() == 200) {
                            HttpEntity entity = response.getEntity();
                            HashSet<String> proxiedHeaders = new HashSet<String>(Arrays.asList(MimeResponse.PROXIED_HEADERS));
                            Header[] headers = response.getAllHeaders();
                            for (Header header : headers) {
                                if (proxiedHeaders.contains(header.getName())) {
                                    logger.debug("Adding headerName " + header.getName() + " and value " + header.getValue() + " when proxying request");
                                    resp.addHeader(header.getName(), header.getValue());
                                }
                            }

                            if (entity != null) {
                                logger.debug("Obtained a HTTP entity of length " + entity.getContentLength());
                                try (OutputStream os = resp.getOutputStream(); InputStream is = new BufferedInputStream(entity.getContent())) {
                                    byte[] buf = new byte[10 * 1024];
                                    int bytesRead = is.read(buf);
                                    while (bytesRead > 0) {
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
                                try (InputStream is = new BufferedInputStream(response.getEntity().getContent())) {
                                    byte[] buf = new byte[10 * 1024];
                                    int bytesRead = is.read(buf);
                                    while (bytesRead > 0) {
                                        os.write(buf, 0, bytesRead);
                                        bytesRead = is.read(buf);
                                    }
                                }
                                resp.sendError(response.getStatusLine().getStatusCode(), new String(os.toByteArray()));
                            }
                        }
                    }
                }
            }
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * If multiple pvs are hosted on another appliance, a retrieval request is made to that appliance and
     * the event stream is returned.
     *
     * @param req
     * @param resp
     * @param requestTimes
     * @param pvInfos
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private List<List<Future<EventStream>>> retrieveEventStreamFromForeignAppliance(
            HttpServletRequest req, HttpServletResponse resp,
            ArrayList<PVInfoForClusterRetrieval> pvInfos, LinkedList<TimeSpan> requestTimes)
            throws IOException, InterruptedException, ExecutionException {

        // Get the executors for the PVs in other clusters
        List<RetrievalExecutorResult> executorResults = new ArrayList<RetrievalExecutorResult>(pvInfos.size());
        for (PVInfoForClusterRetrieval pvInfo : pvInfos) {
            executorResults.add(determineExecutorForPostProcessing(pvInfo.getPVName(),
                    pvInfo.getTypeInfo(), requestTimes, req, pvInfo.getPostProcessor()));
        }

        // Get list of lists of futures of retrieval results. Basically, this is setting up the data sources for retrieval.
        List<LinkedList<Future<RetrievalResult>>> listOfRetrievalResultsFutures = new ArrayList<LinkedList<Future<RetrievalResult>>>();
        for (int i = 0; i < pvInfos.size(); i++) {
            PVInfoForClusterRetrieval pvInfo = pvInfos.get(i);
            listOfRetrievalResultsFutures.add(resolveAllDataSources(pvInfo.getPVName(), pvInfo.getTypeInfo(), pvInfo.getPostProcessor(),
                    pvInfo.getApplianceInfo(), new BasicContext(), executorResults.get(i), req));
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
     *
     * @param resp
     * @param pvName
     * @param requestTimes  - list of timespans that we add the valid times to.
     * @param timeRangesStr
     * @return
     * @throws IOException
     */
    private boolean parseTimeRanges(HttpServletResponse resp, String pvName, LinkedList<TimeSpan> requestTimes, String timeRangesStr) throws IOException {
        String[] timeRangesStrList = timeRangesStr.split(",");
        if (timeRangesStrList.length % 2 != 0) {
            String msg = "Need to specify an even number of times in timeranges for pv " + pvName + ". We have " + timeRangesStrList.length + " times";
            logger.error(msg);
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
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
                    resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
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
                String msg = "For request, end " + t1 + " is before start " + t0.toString() + " for pv " + pvName;
                logger.error(msg);
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
                return false;
            }

            if (prevEnd != null) {
                if (t0.before(prevEnd)) {
                    String msg = "For request, start time " + t0 + " is before previous end time " + prevEnd + " for pv " + pvName;
                    logger.error(msg);
                    resp.sendError(HttpServletResponse.SC_BAD_REQUEST, msg);
                    return false;
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
     *
     * @param typeInfo
     * @return
     * @throws IOException
     */
    private boolean setActualDBRTypeFromData(String pvName, PVTypeInfo typeInfo, ConfigService configService) throws IOException {
        String[] dataStores = typeInfo.getDataStores();
        for (String dataStore : dataStores) {
            StoragePlugin plugin = StoragePluginURLParser.parseStoragePlugin(dataStore, configService);
            if (plugin instanceof ETLDest etlDest) {
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
     * Create a PVTypeInfo object based on a pv name and determine the datastore
     * locations from the policies.py file.
     *
     * @param pvName name of the PV to create a PVTypeInfo object for
     * @return a default PVTypeInfo object for the input pv name based on the
     * policies.py file.
     * @throws IOException if the policies file cannot be opened.
     */
    private PVTypeInfo createDefaultPVTypeInfoFromPolicies(String pvName) throws IOException {
        // Create Python interpreter to access policies.py methods
        try (PySystemState systemState = new PySystemState()) {
            PythonInterpreter interp = new PythonInterpreter(null, systemState);

            // Load the policies.py into the interpreter.
            try (InputStream is = configService.getPolicyText()) {
                interp.execfile(is);
            }

            // Call policies.py determinePolicy() method
            PyDictionary pvInfoDict = new PyDictionary();
            pvInfoDict.put("pvName", pvName);
            pvInfoDict.put("policyName", "");
            pvInfoDict.put("eventRate", 1.0);
            interp.set("pvInfo", pvInfoDict);
            interp.exec("pvPolicy = determinePolicy(pvInfo)");
            PyDictionary policy = (PyDictionary) interp.get("pvPolicy");

            LinkedList<String> dataStores = new LinkedList<String>();
            for (Object dataStore : (PyList) policy.get("dataStores")) {
                dataStores.add((String) dataStore);
            }
            interp.cleanup();

            // Create new PVTypeInfo with pvName and the default dataStores
            // obtained from the policies.py definitions
            PVTypeInfo typeInfo = new PVTypeInfo();
            typeInfo.setPvName(pvName);
            typeInfo.setPaused(true);
            typeInfo.setApplianceIdentity(configService.getMyApplianceInfo().getIdentity());
            // Somehow tell the code downstream that this is a fake typeInfo.
            typeInfo.setSamplingMethod(SamplingMethod.DONT_ARCHIVE);
            typeInfo.setDataStores(dataStores.toArray(new String[0]));

            return typeInfo;
        }
    }

    /**
     * Check to see if the property prompting a further search of the datastore is
     * set. This property is used to search for potentially retired PVs that are no
     * longer being archived. Default to false if property does not exist.
     *
     * @return boolean indicating whether to search for retired PVs based on the
     * defined property.
     */
    private Boolean isSearchStoreForRetiredPvs() {
        String searchStoreForRetiredPvsStr = configService.getInstallationProperties()
                .getProperty(SEARCH_STORE_FOR_RETIRED_PV_STR, "false");
        return Boolean.parseBoolean(searchStoreForRetiredPvsStr);
    }

    private record RequestTimes(Timestamp end, Timestamp start,
                                LinkedList<TimeSpan> requestTimes) {
    }

    static class MimeMappingInfo {
        Class<? extends MimeResponse> mimeresponseClass;
        String contentType;

        public MimeMappingInfo(Class<? extends MimeResponse> mimeresponseClass, String contentType) {
            super();
            this.mimeresponseClass = mimeresponseClass;
            this.contentType = contentType;
        }
    }

    /**
     * Based on the post processor, we make a call on where we can process the request in parallel
     * Either way, we return the result of this decision as two components
     * One is an executor to use
     * The other is a list of timespans that we have broken  the request into - the timespans will most likely be the time spans of the individual bins in the request.
     *
     * @author mshankar
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
     */
    private static class PVInfoForClusterRetrieval {

        private final String pvName;
        private final PVTypeInfo typeInfo;
        private final PostProcessor postProcessor;
        private final ApplianceInfo applianceInfo;

        private PVInfoForClusterRetrieval(String pvName, PVTypeInfo typeInfo,
                                          PostProcessor postProcessor, ApplianceInfo applianceInfo) {
            this.pvName = pvName;
            this.typeInfo = typeInfo;
            this.postProcessor = postProcessor;
            this.applianceInfo = applianceInfo;

            assert (this.pvName != null);
            assert (this.typeInfo != null);
            assert (this.postProcessor != null);
            assert (this.applianceInfo != null);
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
