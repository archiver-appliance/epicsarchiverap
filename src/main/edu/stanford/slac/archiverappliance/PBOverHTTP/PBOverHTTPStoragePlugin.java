/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PBOverHTTP;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.ConversionFunction;
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.utils.ui.URIUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * A read-only storage plugin that gets data using the PB/http protocol from a server.
 * @author mshankar
 *
 */
public class PBOverHTTPStoragePlugin implements StoragePlugin {
    private static Logger logger = LogManager.getLogger(PBOverHTTPStoragePlugin.class.getName());
    private String accessURL = null;
    private String desc = "A event stream backed by a .raw response from a remote server.";
    private String name;
    private boolean skipExternalServers = false;
    public static final String PBHTTP_PLUGIN_IDENTIFIER = "pbraw";

    @Override
    public String pluginIdentifier() {
        return PBHTTP_PLUGIN_IDENTIFIER;
    }

    @Override
    public List<Callable<EventStream>> getDataForPV(
            BasicContext context, String pvName, Instant startTime, Instant endTime, PostProcessor postProcessor)
            throws IOException {
        String getURL = accessURL + "?pv=" + pvName
                + "&from=" + TimeUtils.convertToISO8601String(startTime)
                + "&to=" + TimeUtils.convertToISO8601String(endTime)
                + (postProcessor != null ? "&pp=" + postProcessor.getExtension() : "")
                + (skipExternalServers ? "&skipExternalServers=true" : "");
        logger.info("URL to fetch data is " + getURL);
        return getDataBehindURL(getURL, startTime, postProcessor);
    }

    public List<Callable<EventStream>> getDataForMultiPVs(
            BasicContext context, List<String> pvNames, Instant startTime, Instant endTime, PostProcessor postProcessor)
            throws IOException {
        String getURL = accessURL;
        for (int i = 0; i < pvNames.size(); i++)
            if (i == 0) getURL += "?pv=" + pvNames.get(i);
            else getURL += "&pv=" + pvNames.get(i);
        getURL += "&from=" + TimeUtils.convertToISO8601String(startTime)
                + "&to=" + TimeUtils.convertToISO8601String(endTime)
                + (postProcessor != null ? "&pp=" + postProcessor.getExtension() : "")
                + (skipExternalServers ? "&skipExternalServers=true" : "");
        logger.info("URL to fetch data is " + getURL);
        return getDataBehindURL(getURL, startTime, postProcessor);
    }

    private List<Callable<EventStream>> getDataBehindURL(
            String getURL, Instant startTime, PostProcessor postProcessor) {
        try {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            HttpGet getMethod = new HttpGet(getURL);
            getMethod.addHeader(
                    "Connection",
                    "close"); // https://www.nuxeo.com/blog/using-httpclient-properly-avoid-closewait-tcp-connections/
            try (CloseableHttpResponse response = httpclient.execute(getMethod)) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        logger.debug("Obtained a HTTP entity of length " + entity.getContentLength());
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        entity.writeTo(bos);
                        bos.close();
                        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                        InputStreamBackedEventStream isStream = new InputStreamBackedEventStream(bis, startTime);
                        if (isStream.getDescription() != null) {
                            isStream.getDescription().setSource(this.getName());
                        } else {
                            logger.warn("No desc attached to input stream for url " + getURL);
                        }
                        return CallableEventStream.makeOneStreamCallableList(isStream, postProcessor, true);
                    } else {
                        logger.debug("Obtained empty HTTP entity from " + getURL);
                    }
                } else {
                    logger.warn("Invalid status code "
                            + response.getStatusLine().getStatusCode() + " when connecting to URL " + getURL);
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        ByteArrayOutputStream sbuf = new ByteArrayOutputStream();
                        entity.writeTo(sbuf);
                        logger.warn(sbuf.toString("UTF-8"));
                    }
                }
            }
        } catch (FileNotFoundException fex) {
            logger.debug("No data from remote site " + getURL);
            return null;
        } catch (Throwable t) {
            logger.warn("Exception fetching data from URL " + getURL, t);
        }
        return null;
    }

    @Override
    public int appendData(BasicContext context, String pvName, EventStream stream) {
        throw new RuntimeException("Append Data is not available for HTTP streams");
    }

    @Override
    public String getDescription() {
        return desc;
    }

    @Override
    public void initialize(String configURL, ConfigService configService) throws IOException {
        try {
            URI srcURI = new URI(configURL);
            HashMap<String, String> queryNVPairs = URIUtils.parseQueryString(srcURI);
            if (queryNVPairs.containsKey("rawURL")) {
                this.setAccessURL(queryNVPairs.get("rawURL"));
            } else {
                throw new IOException(
                        "Cannot initialize the pbraw plugin; this needs the URL to the engine/Raw over HTTP to be specified "
                                + configURL);
            }

            if (queryNVPairs.containsKey("name")) {
                name = queryNVPairs.get("name");
            } else {
                name = new URL(this.getAccessURL()).getHost();
                logger.debug("Using the default name of " + name + " for this plain pb engine");
            }

            if (queryNVPairs.containsKey("skipExternalServers")) {
                logger.debug(
                        "Telling the remote server to skip all data from external (potentially ChannelArchiver) servers");
                this.skipExternalServers = Boolean.parseBoolean(queryNVPairs.get("skipExternalServers"));
            }
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }

    public String getAccessURL() {
        return accessURL;
    }

    public void setAccessURL(String aURL) {
        this.accessURL = aURL;
        this.setDesc("PB over HTTP from URL " + aURL);
        loadPBclasses();
    }

    private static void loadPBclasses() {
        try {
            EPICSEvent.ScalarDouble.newBuilder()
                    .setSecondsintoyear(0)
                    .setNano(0)
                    .setVal(0)
                    .setSeverity(0)
                    .setStatus(0)
                    .build()
                    .toByteArray();
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

    public void setDesc(String newDesc) {
        this.desc = newDesc;
    }

    @Override
    public Event getLastKnownEvent(BasicContext context, String pvName) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Event getFirstKnownEvent(BasicContext context, String pvName) throws IOException {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void renamePV(BasicContext context, String oldName, String newName) throws IOException {
        // Nothing to do here.
    }

    @Override
    public void convert(BasicContext context, String pvName, ConversionFunction conversionFuntion) throws IOException {
        // Nothing to do here.
    }
}
