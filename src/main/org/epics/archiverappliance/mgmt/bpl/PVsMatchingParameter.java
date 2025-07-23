package org.epics.archiverappliance.mgmt.bpl;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

/**
 * Small utility class for listing PVs that match a parameter
 * @author mshankar
 *
 */
public class PVsMatchingParameter {
    private static Logger logger = LogManager.getLogger(PVsMatchingParameter.class.getName());

    public static LinkedList<String> getMatchingPVs(
            HttpServletRequest req, ConfigService configService, int defaultLimit) {
        return getMatchingPVs(req, configService, false, defaultLimit);
    }
    /**
     * Given a BPL request, get all the matching PVs
     * @param req HttpServletRequest
     * @param configService  ConfigService
     * @param includePVSThatDontExist Some BPL requires us to include PVs that don't exist so that they can give explicit status
     * @param defaultLimit The default value for the limit if the limit is not specified in the request.
     * @return LinkedList Matching PVs
     */
    public static LinkedList<String> getMatchingPVs(
            HttpServletRequest req, ConfigService configService, boolean includePVSThatDontExist, int defaultLimit) {
        // The assumption taken previously was that each query parameter will have a single value only.
        // If this assumption is to be changed then the below simplification would have to be removed.
        Map<String, String> requestParameters = getRequestParameters(req);
        int limit = getLimit(defaultLimit, requestParameters);

        List<String> pvs = new ArrayList<>();
        if (requestParameters.get("pv") != null) {
            pvs = Arrays.asList(requestParameters.get("pv").split(","));
        }
        String regex = null;
        if (requestParameters.get("regex") != null) {
            regex = requestParameters.get("regex");
        }
        return getMatchingPVs(pvs, regex, limit, configService, includePVSThatDontExist);
    }

    public static Map<String, String> getRequestParameters(HttpServletRequest req) {
        return req.getParameterMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, (entry) -> entry.getValue()[0]));
    }

    /**
     * Given a BPL request, get all the matching PVs
     * @param pvs List of pvs and regexes
     * @param regex Single regex search
     * @param limit Limit of query on pvs
     * @param configService ConfigService
     * @param includePVSThatDontExist Some BPL requires us to include PVs that don't exist so that they can give explicit status
     * @return LinkedList Matching PVs
     */
    public static LinkedList<String> getMatchingPVs(
            List<String> pvs, String regex, int limit, ConfigService configService, boolean includePVSThatDontExist) {
        LinkedList<String> pvNames = new LinkedList<String>();

        if (!pvs.isEmpty()) {
            LinkedList<String> pvNames1 =
                    getConfigServicePVs(pvs, limit, configService, includePVSThatDontExist, pvNames);
            if (pvNames1 != null) return pvNames1;

        } else {
            LinkedList<String> pvNames1;
            if (regex != null) {
                pvNames1 = getRegexMatches(regex, limit, configService, pvNames);
            } else {
                pvNames1 = getAllPVs(limit, configService, pvNames);
            }
            if (pvNames1 != null) return pvNames1;
        }
        return pvNames;
    }

    private static LinkedList<String> getAllPVs(int limit, ConfigService configService, LinkedList<String> pvNames) {
        for (String pvName : configService.getAllPVs()) {
            pvNames.add(pvName);
            if (limit != -1 && pvNames.size() >= limit) {
                return pvNames;
            }
        }
        for (String pvName : configService.getAllAliases()) {
            pvNames.add(pvName);
            if (limit != -1 && pvNames.size() >= limit) {
                return pvNames;
            }
        }
        return null;
    }

    private static LinkedList<String> getRegexMatches(
            String regex, int limit, ConfigService configService, LinkedList<String> pvNames) {
        Pattern pattern = Pattern.compile(regex);
        for (String pvName : configService.getAllPVs()) {
            if (pattern.matcher(pvName).matches()) {
                pvNames.add(pvName);
                if (limit != -1 && pvNames.size() >= limit) {
                    return pvNames;
                }
            }
        }
        for (String pvName : configService.getAllAliases()) {
            if (pattern.matcher(pvName).matches()) {
                pvNames.add(pvName);
                if (limit != -1 && pvNames.size() >= limit) {
                    return pvNames;
                }
            }
        }
        return null;
    }

    private static LinkedList<String> getConfigServicePVs(
            List<String> pvs,
            int limit,
            ConfigService configService,
            boolean includePVSThatDontExist,
            LinkedList<String> pvNames) {
        for (String pv : pvs) {
            if (StringUtils.containsAny(pv, "*?")) {
                WildcardFileFilter matcher = new WildcardFileFilter(pv);
                for (String pvName : configService.getAllPVs()) {
                    if (matcher.accept((new File(pvName)))) {
                        pvNames.add(pvName);
                        if (limit != -1 && pvNames.size() >= limit) {
                            return pvNames;
                        }
                    }
                }
                for (String pvName : configService.getAllAliases()) {
                    if (matcher.accept((new File(pvName)))) {
                        pvNames.add(pvName);
                        if (limit != -1 && pvNames.size() >= limit) {
                            return pvNames;
                        }
                    }
                }
            } else {
                ApplianceInfo info = configService.getApplianceForPV(pv);
                if (info != null) {
                    pvNames.add(pv);
                    if (limit != -1 && pvNames.size() >= limit) {
                        return pvNames;
                    }
                } else {
                    if (includePVSThatDontExist) {
                        pvNames.add(pv);
                        if (limit != -1 && pvNames.size() >= limit) {
                            return pvNames;
                        }
                    }
                }
            }
        }
        return null;
    }

    public static int getLimit(int defaultLimit, Map<String, String> requestParameters) {
        int limit = defaultLimit;

        String limitParam = requestParameters.get("limit");
        if (limitParam != null) {
            limit = Integer.parseInt(limitParam);
        }
        return limit;
    }

    public static LinkedList<String> getPVNamesFromPostBody(HttpServletRequest req) throws IOException {
        LinkedList<String> pvNames = new LinkedList<String>();
        String contentType = req.getContentType();
        if (contentType == null) {
            contentType = MimeTypeConstants.APPLICATION_FORM_URLENCODED;
        }
        switch (contentType) {
            case MimeTypeConstants.APPLICATION_JSON:
                try (LineNumberReader lineReader =
                        new LineNumberReader(new InputStreamReader(new BufferedInputStream(req.getInputStream())))) {
                    JSONParser parser = new JSONParser();
                    for (Object pvName : (JSONArray) parser.parse(lineReader)) {
                        if (pvName instanceof JSONObject) {
                            pvNames.add((String) ((JSONObject) pvName).get("pv"));
                        } else {
                            pvNames.add((String) pvName);
                        }
                    }
                } catch (ParseException ex) {
                    throw new IOException(ex);
                }
                return pvNames;
            case MimeTypeConstants.TEXT_PLAIN:
                // For the default we assume text/plain which is a list of PV's separated by unix newlines
                try (LineNumberReader lineReader =
                        new LineNumberReader(new InputStreamReader(new BufferedInputStream(req.getInputStream())))) {
                    String pv = lineReader.readLine();
                    logger.debug("Parsed pv " + pv);
                    while (pv != null) {
                        pvNames.add(pv);
                        pv = lineReader.readLine();
                    }
                }
                return pvNames;
            case MimeTypeConstants.APPLICATION_FORM_URLENCODED:
            default:
                String[] pvs = req.getParameter("pv").split(",");
                pvNames.addAll(Arrays.asList(pvs));
                return pvNames;
        }
    }
}
