package org.epics.archiverappliance.retrieval;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.FieldValues;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.utils.ui.GetUrlContent;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is responsible for fetching metadata for a given process variable (PV) at specified time points (start, end, latest).
 * It provides methods to create a MetaDataTime object from request strings, fetch metadata from the engine, and merge metadata into remote descriptions.
 */
public class MetaDataTime {
    private static final Logger logger = LogManager.getLogger(MetaDataTime.class.getName());

    private enum MetaDataTimePoint {
        LATEST,
        START,
        END;
    }

    Set<MetaDataTimePoint> timePoints;

    /**
     * Constructs a MetaDataTime object with the specified time points.
     *
     * @param timePoints A set of MetaDataTimePoint indicating which time points to fetch metadata for
     */
    MetaDataTime(Set<MetaDataTimePoint> timePoints) {
        this.timePoints = timePoints;
    }

    /**
     * Create a MetaDataTime object from request strings.
     *
     * @param fetchStartMetadataStr  String representation of whether to fetch start metadata
     * @param fetchEndMetadataStr    String representation of whether to fetch end metadata
     * @param fetchLatestMetadataStr String representation of whether to fetch latest metadata
     * @return A MetaDataTime object with the specified time points
     */
    public static MetaDataTime fromRequestStrings(
            String fetchStartMetadataStr, String fetchEndMetadataStr, String fetchLatestMetadataStr) {
        boolean fetchStart = Boolean.parseBoolean(fetchStartMetadataStr);
        boolean fetchEnd = Boolean.parseBoolean(fetchEndMetadataStr);
        boolean fetchLatest = Boolean.parseBoolean(fetchLatestMetadataStr);
        return new MetaDataTime(Stream.of(MetaDataTimePoint.values())
                .filter(point -> (point == MetaDataTimePoint.START && fetchStart)
                        || (point == MetaDataTimePoint.END && fetchEnd)
                        || (point == MetaDataTimePoint.LATEST && fetchLatest))
                .collect(Collectors.toSet()));
    }

    /**
     * Make a call to the engine to fetch the latest metadata and then add it to the mergeConsumer
     *
     * @param pvName the name of the process variable
     * @param applianceForPV the appliance information for the process variable
     * @return a map containing the latest metadata fetched from the engine, or an empty map if an exception occurs
     */
    @SuppressWarnings("unchecked")
    private Map<String, String> fetchLatestMetadataFromEngine(String pvName, ApplianceInfo applianceForPV) {
        try {
            String metadataURL = applianceForPV.getEngineURL() + "/getMetadata?pv="
                    + URLEncoder.encode(pvName, StandardCharsets.UTF_8);
            logger.debug("Getting metadata from the engine using " + metadataURL);
            return GetUrlContent.getURLContentAsJSONObject(metadataURL);
        } catch (Exception ex) {
            logger.warn("Exception fetching latest metadata for pv " + pvName, ex);
        }
        return Map.of();
    }

    private Map<String, String> fetchMetaDataAtTime(
            String pvName, Instant time, Period searchPeriod, ConfigService configService, String prefix) {
        try {
            PVWithData data = GetDataAtTime.getDataAtTimeForPVFromStores(pvName, time, searchPeriod, configService);
            if (data != null) {
                FieldValues event = (DBRTimeEvent) data.event();
                Map<String, String> fields = event.getFields();
                if (prefix != null && !prefix.isEmpty()) {
                    Map<String, String> prefixedFields = new HashMap<>();
                    for (Map.Entry<String, String> entry : fields.entrySet()) {
                        prefixedFields.put(prefix + entry.getKey(), entry.getValue());
                    }
                    return prefixedFields;
                }
                return fields;
            }
        } catch (Exception ex) {
            logger.warn("Exception fetching first metadata for pv " + pvName, ex);
        }
        return Map.of();
    }

    /**
     * Fetches metadata for the given PV at the specified time points (start, end, latest) and returns a map of metadata.
     *
     * @param typeInfo The PVTypeInfo from the config database - could be null
     * @param pvName The name of the process variable
     * @param applianceForPV The appliance information for the process variable
     * @param start The start time for fetching metadata
     * @param end The end time for fetching metadata
     * @param configService The configuration service to use for fetching data
     * @return A map containing the fetched metadata
     */
    public Map<String, String> getMetadata(
            PVTypeInfo typeInfo,
            String pvName,
            ApplianceInfo applianceForPV,
            Instant start,
            Instant end,
            ConfigService configService) {
        return getMetadata(
                typeInfo, pvName, applianceForPV, start, end, searchPeriodBetween(start, end), configService);
    }

    public Map<String, String> getMetadata(
            PVTypeInfo typeInfo,
            String pvName,
            ApplianceInfo applianceForPV,
            Instant start,
            Instant end,
            Period searchPeriod,
            ConfigService configService) {
        Map<String, String> metadata = new HashMap<>();
        boolean beingArchived =
                typeInfo != null && typeInfo.getSamplingMethod() != PolicyConfig.SamplingMethod.DONT_ARCHIVE;
        for (MetaDataTimePoint point : timePoints) {
            switch (point) {
                case START ->
                    metadata.putAll(fetchMetaDataAtTime(
                            pvName, start, searchPeriod, configService, timePoints.size() <= 1 ? "" : "start_"));
                case LATEST -> {
                    if (beingArchived) {
                        metadata.putAll(fetchLatestMetadataFromEngine(pvName, applianceForPV));
                    }
                }
                case END ->
                    metadata.putAll(fetchMetaDataAtTime(
                            pvName, end, searchPeriod, configService, timePoints.size() <= 1 ? "" : "end_"));
            }
        }
        return metadata;
    }

    public static Period searchPeriodBetween(Instant start, Instant end) {
        if (start == null || end == null || !end.isAfter(start)) return Period.ofDays(1);
        Duration duration = Duration.between(start, end);
        long days = duration.toDays();
        if (!duration.minusDays(days).isZero()) days++;
        return Period.ofDays((int) Math.min(Integer.MAX_VALUE, Math.max(1, days)));
    }

    /**
     * Merges info from pvTypeTnfo that comes from the config database into the remote description that gets sent over the wire.
     *
     * @param typeInfo The PVTypeInfo from the config database - could be null
     * @param eventDesc The remote description that gets sent over the wire - could be null
     * @param metadata Latest from the engine - could be null
     *
     */
    public static void mergeMetaData(PVTypeInfo typeInfo, EventStreamDesc eventDesc, Map<String, String> metadata)
            throws IOException {
        if (eventDesc instanceof RemotableEventStreamDesc remoteDesc) {
            logger.debug("Merging typeinfo into remote desc for pv " + eventDesc.getPvName() + " into source "
                    + eventDesc.getSource());
            remoteDesc.mergeFrom(typeInfo, metadata);
        }
    }

    /**
     * Tries to merge info from pvTypeTnfo that comes from the config database into the remote description that gets sent over the wire.
     * If there is a mismatch in the DBR types, it logs an error and returns true, indicating that there was a mismatch.
     *
     * @param typeInfo The PVTypeInfo from the config database - could be null
     * @param sourceDesc The remote description that gets sent over the wire - could be null
     * @param metadata Latest from the engine - could be null
     * @return true if there was a mismatch in DBR types, false otherwise
     */
    public static boolean tryMergeMetaData(
            PVTypeInfo typeInfo, EventStreamDesc sourceDesc, Map<String, String> metadata) throws IOException {
        try {
            mergeMetaData(typeInfo, sourceDesc, metadata);
        } catch (MismatchedDBRTypeException mex) {
            logger.error(mex.getMessage(), mex);
            return true;
        }
        return false;
    }
}
