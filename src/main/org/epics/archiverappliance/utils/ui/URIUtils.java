/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.ui;

import edu.stanford.slac.archiverappliance.plain.URLKey;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Some utilities for parsing URI's
 * @author mshankar
 *
 */
public class URIUtils {
    /**
     * Parse the query string of a URI (typically used in archiver config strings) and return these as a name value pair hashmap.
     * We do not handle multiple values for the same param in this call; we simply replace previous names.
     * @param uri URI
     * @return HashMap Parse the query string
     * @throws IOException  &emsp;
     */
    public static HashMap<String, String> parseQueryString(URI uri) throws IOException {
        HashMap<String, String> ret = new HashMap<String, String>();
        if (uri == null) return ret;
        List<NameValuePair> nvs = URLEncodedUtils.parse(uri, "UTF-8");
        if (nvs == null) return ret;
        for (NameValuePair nv : nvs) {
            ret.put(nv.getName(), nv.getValue());
        }
        return ret;
    }

    /**
     * If you do expect a param to have multiple values, use this method to get all the possible values for a name.
     * @param uri URI
     * @param paramName  &emsp;
     * @return multiple values of a param
     * @throws IOException  &emsp;
     */
    public static List<String> getMultiValuedParamFromQueryString(URI uri, String paramName) throws IOException {
        LinkedList<String> ret = new LinkedList<String>();
        if (uri == null) return ret;
        List<NameValuePair> nvs = URLEncodedUtils.parse(uri, "UTF-8");
        if (nvs == null) return ret;
        for (NameValuePair nv : nvs) {
            if (nv.getName().equals(paramName)) {
                ret.add(nv.getValue());
            }
        }
        return ret;
    }

    public static String pluginString(String proto, String hostname, String params) {
        return proto + "://" + hostname + "?" + params;
    }

    public static String pluginString(String proto, String hostname, Map<URLKey, String> params) {

        return pluginString(
                proto,
                hostname,
                URLEncodedUtils.format(
                        params.entrySet().stream()
                                .map(e -> new BasicNameValuePair(e.getKey().key(), e.getValue()))
                                .toList(),
                        StandardCharsets.UTF_8));
    }
}
