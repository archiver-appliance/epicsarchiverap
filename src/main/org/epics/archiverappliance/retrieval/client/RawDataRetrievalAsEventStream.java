/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;


import edu.stanford.slac.archiverappliance.PBOverHTTP.InputStreamBackedEventStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.HashMap;

/**
 * Client side class for retrieving data from the appliance archiver using the PB over HTTP protocol.
 * This is mostly used by the unit tests where the ability to treat retrieval results as event streams is veru useful.
 * Java clients should use the pbrawclient which is a lightweight implementation of the same.
 * @author mshankar
 *
 */
public class RawDataRetrievalAsEventStream implements DataRetrievalForPVs {
	private static Logger logger = LogManager.getLogger(RawDataRetrievalAsEventStream.class.getName());
	private String accessURL = null;
	
	public RawDataRetrievalAsEventStream(String accessURL) {
		this.accessURL = accessURL;
	}

	@Override
    public EventStream getDataForPVS(String[] pvNames, Instant startTime, Instant endTime, RetrievalEventProcessor retrievalEventProcessor) {
		return getDataForPVS(pvNames, startTime, endTime, retrievalEventProcessor, false, null);
	}

	@Override
    public EventStream getDataForPVS(String[] pvNames, Instant startTime, Instant endTime, RetrievalEventProcessor retrievalEventProcessor, boolean useReducedDataSet) {
		return getDataForPVS(pvNames, startTime, endTime, retrievalEventProcessor, useReducedDataSet, null);
	}

	@Override
    public EventStream getDataForPVS(String[] pvNames, Instant startTime, Instant endTime, RetrievalEventProcessor retrievalEventProcessor, boolean useReducedDataSet, HashMap<String, String> otherParams) {
		StringWriter concatedPVs = new StringWriter();
		boolean isFirstEntry = true;
		for(String pvName : pvNames) {
			if(isFirstEntry) {
				isFirstEntry = false;
			} else {
				concatedPVs.append(",");
			}
			concatedPVs.append(pvName);
		}
		// We'll use java.net for now.
		StringWriter buf = new StringWriter();
		buf.append(accessURL)
		.append("?pv=").append(concatedPVs.toString())
		.append("&from=").append(TimeUtils.convertToISO8601String(startTime))
		.append("&to=").append(TimeUtils.convertToISO8601String(endTime));
		if(useReducedDataSet) {
			buf.append("&usereduced=true");
		}
		if(otherParams != null) {
			for(String key : otherParams.keySet()) {
				buf.append("&");
				buf.append(key);
				buf.append("=");
				buf.append(otherParams.get(key));
			}
		}
		String getURL = buf.toString();
		logger.info("URL to fetch data is " + getURL);
		try {
			URL url = new URL(getURL);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.connect();
			if(connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
				InputStream is = new BufferedInputStream(connection.getInputStream());
				if(is.available() <= 0) {
					logger.info("We got an empty stream as a response for PVs " + concatedPVs + " + using URL " + url);
					return null;
				}
				return new InputStreamBackedEventStream(is, startTime, retrievalEventProcessor);
			} else { 
				logger.info("No data found for PVs " + concatedPVs + " + using URL " + url);
				return null;
			}
		} catch(Exception ex) {
			logger.error("Exception fetching data from URL " + getURL, ex);
		}
		return null;
	}
	
	
}