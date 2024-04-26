/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * BPL for archiving a PV.
 *
 * Here's an example of how to archive a couple of PV's using a JSON request.
 * <pre><code>
 * $ cat archivePVs.json
 * [
 *  {"samplingperiod": "1.0", "pv": "mshankar:arch:sine", "samplingmethod": "SCAN"},
 *  {"samplingperiod": "2.0", "pv": "mshankar:arch:cosine", "samplingmethod": "MONITOR"}
 * ]
 * $ curl -H "Content-Type: application/json" --data @archivePVs.json http://localhost:17665/mgmt/bpl/archivePV
 * [
 *  { "pvName": "mshankar:arch:sine", "status": "Archive request submitted" },
 *  { "pvName": "mshankar:arch:cosine", "status": "Archive request submitted" }
 * ]
 *
 * After some time...
 *
 * $ GET "http://localhost:17665/mgmt/bpl/getPVStatus?pv=mshankar:arch:*"
 * [
 *  {"lastRotateLogs":"Never","appliance":"appliance0","pvName":"mshankar:arch:cosine","pvNameOnly":"mshankar:arch:cosine","connectionState":"true","lastEvent":"Oct\/12\/2015 13:54:53 -07:00","samplingPeriod":"2.0","isMonitored":"true","connectionLastRestablished":"Never","connectionFirstEstablished":"Oct\/12\/2015 13:53:05 -07:00","connectionLossRegainCount":"0","status":"Being archived"},
 *  {"lastRotateLogs":"Never","appliance":"appliance0","pvName":"mshankar:arch:sine","pvNameOnly":"mshankar:arch:sine","connectionState":"true","lastEvent":"Oct\/12\/2015 13:54:53 -07:00","samplingPeriod":"1.0","isMonitored":"false","connectionLastRestablished":"Never","connectionFirstEstablished":"Oct\/12\/2015 13:53:05 -07:00","connectionLossRegainCount":"0","status":"Being archived"}
 * ]
 *
 *</code></pre>
 *
 * @epics.BPLAction - Archive one or more PV's.
 * @epics.BPLActionParam pv - The name of the pv to be archived. If archiving more than one PV, use a comma separated list. You can also send the PV list as part of the POST body using standard techniques. If you need to specify different archiving parameters for each PV, send the data as a JSON array (remember to send the content type correctly).
 * @epics.BPLActionParam samplingperiod - The sampling period to be used. Optional, default value is 1.0 seconds.
 * @epics.BPLActionParam samplingmethod - The sampling method to be used. For now, this is one of SCAN or MONITOR. Optional, default value is MONITOR.
 * @epics.BPLActionParam controllingPV - The controlling PV for coditional archiving. Optional; if unspecified, we do not use conditional archiving.
 * @epics.BPLActionParam policy - Override the policy execution process and use this policy instead. Optional; if unspecified, we go thru the normal policy execution process.
 * @epics.BPLActionParam appliance - Optional; you can specify an appliance in a cluster. If specified (value is the identity of the appliance), the sampling and archiving are done on the specified appliance.
 * @epics.BPLActionEnd
 *
 *
 * @author mshankar
 *
 */
public class ArchivePVAction implements BPLAction {
    public static final Logger logger = LogManager.getLogger(ArchivePVAction.class);

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        if (!configService.hasClusterFinishedInitialization()) {
            // If you have defined spare appliances in the appliances.xml that will never come up; you should remove
            // them
            // This seems to be one of the few ways we can prevent split brain clusters from messing up the pv <->
            // appliance mapping.
            throw new IOException(
                    "Waiting for all the appliances listed in appliances.xml to finish loading up their PVs into the cluster");
        }

        String contentType = req.getContentType();
        if (contentType != null && contentType.equals(MimeTypeConstants.APPLICATION_JSON)) {
            processJSONRequest(req, resp, configService);
            return;
        }

        logger.info("Archiving pv(s) " + req.getParameter("pv"));
        String[] pvs = req.getParameter("pv").split(",");
        String samplingPeriodStr = req.getParameter("samplingperiod");
        boolean samplingPeriodSpecified = samplingPeriodStr != null && !samplingPeriodStr.isEmpty();
        float samplingPeriod = PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
        if (samplingPeriodSpecified) {
            samplingPeriod = Float.parseFloat(samplingPeriodStr);
        }

        String samplingMethodStr = req.getParameter("samplingmethod");
        SamplingMethod samplingMethod = SamplingMethod.MONITOR;
        if (samplingMethodStr != null) {
            samplingMethod = SamplingMethod.valueOf(samplingMethodStr);
        }

        String controllingPV = req.getParameter("controllingPV");
        if (controllingPV != null && !controllingPV.isEmpty()) {
            logger.debug("We are conditionally archiving using controlling PV " + controllingPV);
        }

        String policyName = req.getParameter("policy");
        if (policyName != null && !policyName.isEmpty()) {
            logger.info("We have a user override for policy " + policyName);
        }
        List<String> fieldsAsPartOfStream = ArchivePVAction.getFieldsAsPartOfStream(configService);

        if (pvs.length < 1) {
            return;
        }

        boolean skipCapacityPlanning = false;
        String applianceId = req.getParameter("appliance");
        if (applianceId != null) {
            logger.debug("Appliance specified " + applianceId);
            skipCapacityPlanning = true;
            String myIdentity = configService.getMyApplianceInfo().getIdentity();
            if (!applianceId.equals(myIdentity)) {
                ApplianceInfo applInfo = configService.getAppliance(applianceId);
                StringWriter buf = new StringWriter();
                buf.append(applInfo.getMgmtURL());
                buf.append("/archivePV?");
                buf.append(req.getQueryString());
                String redirectURL = buf.toString();
                logger.info("Redirecting archivePV request for PV to " + applInfo.getIdentity() + " using URL "
                        + redirectURL);
                JSONArray status = GetUrlContent.getURLContentAsJSONArray(redirectURL);
                resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
                try (PrintWriter out = resp.getWriter()) {
                    out.println(JSONValue.toJSONString(status));
                }
                return;
            }
        }

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println("[");
            boolean isFirst = true;
            for (String pv : pvs) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    out.println(",");
                }
                logger.debug("Calling archivePV for pv " + pv);
                archivePV(
                        out,
                        pv,
                        samplingPeriodSpecified,
                        samplingMethod,
                        samplingPeriod,
                        controllingPV,
                        policyName,
                        null,
                        skipCapacityPlanning,
                        configService,
                        fieldsAsPartOfStream);
            }
            out.println("]");
        }
    }

    /**
     * Performance optimization - pass in fieldsArchivedAsPartOfStream as part of archivePV call.
     * @param configService ConfigService
     * @return All Fields as stream
     */
    public static List<String> getFieldsAsPartOfStream(ConfigService configService) {
        List<String> fieldsArchivedAsPartOfStream = new LinkedList<String>();
        try {
            fieldsArchivedAsPartOfStream = configService.getFieldsArchivedAsPartOfStream();
        } catch (IOException ex) {
            logger.error("Exception fetching standard fields", ex);
        }
        return fieldsArchivedAsPartOfStream;
    }

    /**
     * Does this pvName imply a connection using PVAccess?
     * @param pvName  The name of PV.
     * @param defaultProtocol  The defaultProtocol see org.epics.archiverappliance.mgmt.config.defaultAccessProtocol
     *                         default is CA
     * @return boolean True or False
     */
    public static boolean usePVAccess(String pvName, String defaultProtocol) {
        PVNames.EPICSVersion version = PVNames.pvNameVersion(pvName);
        switch (version) {
            case DEFAULT -> {
                return defaultProtocol.equals("PVA");
            }
            case V3 -> {
                return false;
            }
            case V4 -> {
                return true;
            }
        }
        return false;
    }

    /**
     * This is the main method for adding PVs into the archiver. All other entry points should eventually call this method.
     * @param out PrintWriter
     * @param pvName The name of PV.
     * @param overridePolicyParams True or False
     * @param overriddenSamplingMethod  SamplingMethod
     * @param overRiddenSamplingPeriod  &emsp;
     * @param controllingPV The PV used for controlling whether we archive this PV or not in conditional archiving.
     * @param policyName  If you want to override the policy on a per PV basis.
     * @param alias Optional, any alias that you'd like to register at the same time.
     * @param skipCapacityPlanning  By default false. However, if you want to skip capacity planning and assign to this appliance, set this to true.
     * @param configService ConfigService
     * @param fieldsArchivedAsPartOfStream  &emsp;
     * @throws IOException  &emsp;
     */
    public static void archivePV(
            PrintWriter out,
            String pvName,
            boolean overridePolicyParams,
            SamplingMethod overriddenSamplingMethod,
            float overRiddenSamplingPeriod,
            String controllingPV,
            String policyName,
            String alias,
            boolean skipCapacityPlanning,
            ConfigService configService,
            List<String> fieldsArchivedAsPartOfStream)
            throws IOException {
        String fieldName = PVNames.getFieldName(pvName);
        boolean isStandardFieldName = false;

        if (fieldName != null && !fieldName.isEmpty()) {
            if (fieldName.equals("VAL")) {
                logger.debug("Treating .VAL as pv Name alone for " + pvName);
                fieldName = null;
                pvName = PVNames.stripFieldNameFromPVName(pvName);
            } else if (fieldsArchivedAsPartOfStream.contains(fieldName)) {
                logger.debug("Field " + fieldName + " is one of the standard fields for pv " + pvName);
                pvName = PVNames.stripFieldNameFromPVName(pvName);
                isStandardFieldName = true;
            }
        }

        if (!PVNames.isValidPVName(pvName)) {
            String msg = "PV name fails syntax check " + pvName;
            logger.error(msg);
            throw new IOException(msg);
        }

        // Check for V4 syntax, V3 syntax or default protocol; here's where we lose the prefix
        String defaultProtocol = configService
                .getInstallationProperties()
                .getProperty("org.epics.archiverappliance.mgmt.bpl.ArchivePVAction.defaultAccessProtocol", "CA");

        boolean usePVAccess = usePVAccess(pvName, defaultProtocol);
        pvName = PVNames.stripPrefixFromName(pvName);

        PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
        if (typeInfo != null) {
            logger.debug("We are already archiving this pv " + pvName + " and have a typeInfo");
            if (fieldName != null && !fieldName.isEmpty()) {
                if (isStandardFieldName) {
                    logger.debug("Checking to see if field " + fieldName + " is being archived");
                    if (typeInfo.checkIfFieldAlreadySepcified(fieldName)) {
                        logger.debug("Field " + fieldName + " is already being archived");
                    } else {
                        logger.debug("Adding field " + fieldName + " to a pv that is already being archived");
                        typeInfo.addArchiveField(fieldName);
                        typeInfo.setModificationTime(TimeUtils.now());
                        configService.updateTypeInfoForPV(pvName, typeInfo);
                        // If we determine we need to kick off a pause/resume; here's where we need to do it.
                    }
                }
            }

            if (alias != null) {
                configService.addAlias(alias, pvName);
            }

            out.println("{ \"pvName\": \"" + pvName + "\", \"status\": \"Already submitted\" }");
            return;
        }

        boolean requestAlreadyMade = configService.doesPVHaveArchiveRequestInWorkflow(pvName);
        if (requestAlreadyMade) {
            UserSpecifiedSamplingParams userParams = configService.getUserSpecifiedSamplingParams(pvName);
            if (fieldName != null && !fieldName.isEmpty()) {
                if (isStandardFieldName) {
                    if (userParams != null && !userParams.checkIfFieldAlreadySepcified(fieldName)) {
                        logger.debug("Adding field " + fieldName
                                + " to an existing request. Note we are not updating persistence here.");
                        userParams.addArchiveField(fieldName);
                    }
                }
            }

            if (alias != null) {
                logger.debug("Adding alias " + alias + " to user params of " + pvName + "(1)");
                userParams.addAlias(alias);
            }

            logger.warn("We have a pending request for pv " + pvName);
            out.println("{ \"pvName\": \"" + pvName + "\", \"status\": \"Already submitted\" }");
            return;
        }

        try {
            String actualPVName = pvName;

            if (overridePolicyParams) {
                String minumumSamplingPeriodStr = configService
                        .getInstallationProperties()
                        .getProperty(
                                "org.epics.archiverappliance.mgmt.bpl.ArchivePVAction.minimumSamplingPeriod", "0.1");
                float minumumSamplingPeriod = Float.parseFloat(minumumSamplingPeriodStr);
                if (overRiddenSamplingPeriod < minumumSamplingPeriod) {
                    logger.warn(
                            "Enforcing the minumum sampling period of " + minumumSamplingPeriod + " for pv " + pvName);
                    overRiddenSamplingPeriod = minumumSamplingPeriod;
                }
                logger.debug("Overriding policy params with sampling method " + overriddenSamplingMethod
                        + " and sampling period " + overRiddenSamplingPeriod);
                UserSpecifiedSamplingParams userSpecifiedSamplingParams = new UserSpecifiedSamplingParams(
                        overridePolicyParams ? overriddenSamplingMethod : SamplingMethod.MONITOR,
                        overRiddenSamplingPeriod,
                        controllingPV,
                        policyName,
                        skipCapacityPlanning,
                        usePVAccess);
                if (fieldName != null && !fieldName.isEmpty() && isStandardFieldName) {
                    userSpecifiedSamplingParams.addArchiveField(fieldName);
                }
                if (usePVAccess) {
                    userSpecifiedSamplingParams.setUsePVAccess(usePVAccess);
                }

                if (alias != null) {
                    logger.debug("Adding alias " + alias + " to user params of " + actualPVName + "(2)");
                    userSpecifiedSamplingParams.addAlias(alias);
                }

                configService.addToArchiveRequests(actualPVName, userSpecifiedSamplingParams);
            } else {
                UserSpecifiedSamplingParams userSpecifiedSamplingParams = new UserSpecifiedSamplingParams();
                if (fieldName != null && !fieldName.isEmpty() && isStandardFieldName) {
                    userSpecifiedSamplingParams.addArchiveField(fieldName);
                }
                if (usePVAccess) {
                    userSpecifiedSamplingParams.setUsePVAccess(usePVAccess);
                }
                if (skipCapacityPlanning) {
                    userSpecifiedSamplingParams.setSkipCapacityPlanning(skipCapacityPlanning);
                }

                if (alias != null) {
                    logger.debug("Adding alias " + alias + " to user params of " + actualPVName + "(3)");
                    userSpecifiedSamplingParams.addAlias(alias);
                }

                if (policyName != null) {
                    logger.debug("Overriding the policy to " + policyName + " for pv " + pvName
                            + " for a default period/method");
                    userSpecifiedSamplingParams.setPolicyName(policyName);
                }

                configService.addToArchiveRequests(actualPVName, userSpecifiedSamplingParams);
            }
            // Submit the request to the archive engine.
            // We have to make this a call into the engine to get over that fact that only the engine can load JCA
            // libraries
            configService.getMgmtRuntimeState().startPVWorkflow(pvName);
            out.println("{ \"pvName\": \"" + pvName + "\", \"status\": \"Archive request submitted\" }");
        } catch (Exception ex) {
            logger.error("Exception archiving PV " + pvName, ex);
            out.println("{ \"pvName\": \"" + pvName + "\", \"status\": \"Exception occured\" }");
        }
    }

    private void processJSONRequest(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.debug("Archiving multiple PVs from a JSON POST request");
        List<String> fieldsAsPartOfStream = ArchivePVAction.getFieldsAsPartOfStream(configService);
        try (LineNumberReader lineReader =
                new LineNumberReader(new InputStreamReader(new BufferedInputStream(req.getInputStream())))) {
            JSONParser parser = new JSONParser();
            JSONArray pvArchiveParams = (JSONArray) parser.parse(lineReader);
            logger.debug("PV count " + pvArchiveParams.size());

            String myIdentity = configService.getMyApplianceInfo().getIdentity();
            if (!allRequestsCanBeSampledOnThisAppliance(pvArchiveParams, myIdentity)) {
                logger.debug("Breaking down the requests into appliance calls.");
                breakDownPVRequestsByAssignedAppliance(pvArchiveParams, myIdentity, configService, resp);
                return;
            }

            logger.debug("All calls can be sampled on this appliance.");

            resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
            try (PrintWriter out = resp.getWriter()) {
                out.println("[");

                boolean isFirst = true;
                for (Object pvArchiveParamObj : pvArchiveParams) {
                    logger.debug("Processing...");
                    JSONObject pvArchiveParam = (JSONObject) pvArchiveParamObj;
                    logger.debug("Processing...");
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        out.println(",");
                    }
                    logger.debug("Processing...");
                    String pvName = (String) pvArchiveParam.get("pv");
                    logger.debug("Calling archivePV for pv " + pvName);
                    // By the time we get here, if the appliance is set in the request, we skip capacityPlanning.
                    boolean skipCapacityPlanning = pvArchiveParam.containsKey("appliance");
                    if (skipCapacityPlanning) {
                        logger.debug("Skipping capacity planning for PV " + pvName);
                    }
                    SamplingMethod samplingMethod = SamplingMethod.MONITOR;
                    float samplingPeriod = PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
                    try {
                        samplingMethod = pvArchiveParam.containsKey("samplingmethod")
                                ? SamplingMethod.valueOf(((String) pvArchiveParam.get("samplingmethod")).toUpperCase())
                                : SamplingMethod.MONITOR;
                        samplingPeriod = pvArchiveParam.containsKey("samplingperiod")
                                ? Float.parseFloat((String) pvArchiveParam.get("samplingperiod"))
                                : PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
                    } catch (Exception ex) {
                        logger.error(
                                "Cannot parse sampling period and method " + JSONValue.toJSONString(pvArchiveParam));
                        continue;
                    }

                    ArchivePVAction.archivePV(
                            out,
                            pvName,
                            pvArchiveParam.containsKey("samplingperiod"),
                            samplingMethod,
                            samplingPeriod,
                            (String) pvArchiveParam.get("controllingPV"),
                            (String) pvArchiveParam.get("policy"),
                            (String) pvArchiveParam.get("alias"),
                            skipCapacityPlanning,
                            configService,
                            fieldsAsPartOfStream);
                }

                out.println("]");
                out.flush();
            }
        } catch (Exception ex) {
            logger.error("Exception processing archiveJSON", ex);
            throw new IOException(ex);
        }
        return;
    }

    /**
     * Given a JSON array of archive requests, can we process all these requests on this appliance.
     * We can do this if the applianceID is not specified or if the applianceID is this appliance for all requests.
     * @param pvArchiveParams JSONArray
     * @param myIdentity  &emsp;
     * @return boolean True or False
     */
    private boolean allRequestsCanBeSampledOnThisAppliance(JSONArray pvArchiveParams, String myIdentity) {
        for (Object pvArchiveParamObj : pvArchiveParams) {
            JSONObject pvArchiveParam = (JSONObject) pvArchiveParamObj;
            if (pvArchiveParam.containsKey("appliance")
                    && !pvArchiveParam.get("appliance").equals(myIdentity)) {
                logger.debug(
                        "Not all PVs can be sampled on this appliance. We need to break up the request based on appliance");
                return false;
            }
        }
        logger.debug("All PVs can be sampled on this appliance");
        return true;
    }

    /**
     * Break down a JSON request for archiving into parts based on appliance and then make the calls to the individual appliances.
     * All archive requests that do not have an appliance specified will be sampled on this appliance and go thru capacity planning.
     * @param pvArchiveParams JSONArray
     * @param myIdentity emsp
     * @param configService ConfigServic
     */
    @SuppressWarnings("unchecked")
    private void breakDownPVRequestsByAssignedAppliance(
            JSONArray pvArchiveParams, String myIdentity, ConfigService configService, HttpServletResponse resp)
            throws IOException {
        HashMap<String, LinkedList<JSONObject>> archiveRequestsByAppliance =
                new HashMap<String, LinkedList<JSONObject>>();
        archiveRequestsByAppliance.put(myIdentity, new LinkedList<JSONObject>());
        for (Object pvArchiveParamObj : pvArchiveParams) {
            JSONObject pvArchiveParam = (JSONObject) pvArchiveParamObj;
            if (pvArchiveParam.containsKey("appliance")) {
                String assignedAppliance = (String) pvArchiveParam.get("appliance");
                if (configService.getAppliance(assignedAppliance) == null) {
                    throw new IOException("Cannot find appliance info for " + assignedAppliance + " for pv "
                            + pvArchiveParam.get("pv"));
                }
                if (!archiveRequestsByAppliance.containsKey(assignedAppliance)) {
                    archiveRequestsByAppliance.put(assignedAppliance, new LinkedList<JSONObject>());
                }
                logger.debug("PV " + pvArchiveParam.get("pv") + " should be sampled on appliance " + assignedAppliance);
                archiveRequestsByAppliance.get(assignedAppliance).add(pvArchiveParam);
            } else {
                logger.debug("PV " + pvArchiveParam.get("pv") + " should be sampled here " + myIdentity);
                archiveRequestsByAppliance.get(myIdentity).add(pvArchiveParam);
            }
        }

        // Now we have the requests broken down by appliance.
        // Route them appropriately and combine the results...
        JSONArray finalResult = new JSONArray();
        for (String appliance : archiveRequestsByAppliance.keySet()) {
            LinkedList<JSONObject> requestsForAppliance = archiveRequestsByAppliance.get(appliance);
            if (!requestsForAppliance.isEmpty()) {
                String archiveURL = configService.getAppliance(appliance).getMgmtURL() + "/archivePV";
                JSONArray archiveResponse =
                        GetUrlContent.postDataAndGetContentAsJSONArray(archiveURL, requestsForAppliance);
                finalResult.addAll(archiveResponse);
            }
        }
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(JSONValue.toJSONString(finalResult));
        }
    }
}
