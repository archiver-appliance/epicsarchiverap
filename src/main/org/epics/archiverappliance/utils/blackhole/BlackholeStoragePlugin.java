/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.blackhole;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.ConversionFunction;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.utils.ui.URIUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * A storage plugin that deletes all data that goes into it.
 * Use this as an ETL dest for policies that eliminate data after a certain period.
 * @author mshankar
 *
 */
public class BlackholeStoragePlugin implements StoragePlugin, ETLDest {
    private static Logger logger = LogManager.getLogger(BlackholeStoragePlugin.class.getName());
    private String name = "blackhole";

    @Override
    public List<Callable<EventStream>> getDataForPV(
            BasicContext context, String pvName, Instant startTime, Instant endTime, PostProcessor postProcessor)
            throws IOException {
        // A blackhole plugin has no data
        return null;
    }

    @Override
    public int appendData(BasicContext context, String pvName, EventStream stream) {
        return 1;
    }

    @Override
    public Event getLastKnownEvent(BasicContext context, String pvName) throws IOException {
        // A blackhole plugin has no data
        return null;
    }

    @Override
    public Event getFirstKnownEvent(BasicContext context, String pvName) throws IOException {
        return null;
    }

    @Override
    public boolean appendToETLAppendData(String pvName, EventStream stream, ETLContext context) {
        return true;
    }

    @Override
    public boolean commitETLAppendData(String pvName, ETLContext context) throws IOException {
        return true;
    }

    @Override
    public boolean runPostProcessors(String pvName, ArchDBRTypes dbrtype, ETLContext context) throws IOException {
        return true;
    }

    @Override
    public PartitionGranularity getPartitionGranularity() {
        return PartitionGranularity.PARTITION_YEAR;
    }

    @Override
    public String getDescription() {
        return "A black hole plugin";
    }

    public String getURLRepresentation() {
        StringWriter ret = new StringWriter();
        ret.append("blackhole://localhost");
        try {
            ret.append("?name=");
            ret.append(URLEncoder.encode(name, "UTF-8"));
        } catch (UnsupportedEncodingException ex) {
        }

        return ret.toString();
    }

    @Override
    public void initialize(String configURL, ConfigService configService) throws IOException {
        try {
            URI srcURI = new URI(configURL);
            HashMap<String, String> queryNVPairs = URIUtils.parseQueryString(srcURI);

            if (queryNVPairs.containsKey("name")) {
                name = queryNVPairs.get("name");
            } else {
                logger.debug("Using the default name of " + name + " for this blackhole engine");
            }
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }

    public static final String BLACKHOLE_PLUGIN_IDENTIFIER = "blackhole";

    @Override
    public String pluginIdentifier() {
        return BLACKHOLE_PLUGIN_IDENTIFIER;
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
