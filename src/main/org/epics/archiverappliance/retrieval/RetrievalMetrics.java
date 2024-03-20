package org.epics.archiverappliance.retrieval;

import org.epics.archiverappliance.common.TimeUtils;

import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public class RetrievalMetrics {
    private long numberOfRequests = 0;
    private Instant lastRequest = null;
    private final Set<String> userIdentifiers = new HashSet<>();

    public RetrievalMetrics() {}

    public static final RetrievalMetrics EMPTY_METRICS = new RetrievalMetrics();

    public void updateMetrics(Instant lastRequest, String user) {
        this.numberOfRequests += 1;
        this.userIdentifiers.add(user);
        this.lastRequest = lastRequest;
    }

    public RetrievalMetrics sumMetrics(RetrievalMetrics newMetrics) {
        this.numberOfRequests += newMetrics.numberOfRequests;
        this.userIdentifiers.addAll(newMetrics.userIdentifiers);
        if (newMetrics.lastRequest != null) {
            if (this.lastRequest == null) {
                this.lastRequest = newMetrics.lastRequest;
            } else {
                long latestEpochMillis =
                        Long.max(this.lastRequest.toEpochMilli(), newMetrics.lastRequest.toEpochMilli());
                this.lastRequest = Instant.ofEpochMilli(latestEpochMillis);
            }
        }
        return this;
    }

    public LinkedList<Map<String, String>> getDetails() {
        LinkedList<Map<String, String>> result = new LinkedList<>();
        addDetailedStatus(result, "Number of Retrieval Requests", String.valueOf(this.numberOfRequests));
        addDetailedStatus(
                result, "Time of last Retrieval Request", TimeUtils.convertToHumanReadableString(lastRequest));
        addDetailedStatus(result, "Number of unique users", String.valueOf(userIdentifiers.size()));
        return result;
    }

    private static void addDetailedStatus(LinkedList<Map<String, String>> statuses, String name, String value) {
        Map<String, String> obj = new LinkedHashMap<String, String>();
        obj.put("name", name);
        obj.put("value", value);
        obj.put("source", "retrieval");
        statuses.add(obj);
    }
}
