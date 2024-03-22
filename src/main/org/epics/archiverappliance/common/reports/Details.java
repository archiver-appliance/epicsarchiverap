package org.epics.archiverappliance.common.reports;

import org.epics.archiverappliance.config.ConfigService;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public interface Details {

    static Map<String, String> metricDetail(String source, String name, String value) {
        Map<String, String> obj = new LinkedHashMap<String, String>();
        obj.put("name", name);
        obj.put("value", value);
        obj.put("source", source);
        return obj;
    }

    ConfigService.WAR_FILE source();

    default Map<String, String> metricDetail(String name, String value) {
        return metricDetail(source().toString(), name, value);
    }

    LinkedList<Map<String, String>> details(ConfigService configService);
}
