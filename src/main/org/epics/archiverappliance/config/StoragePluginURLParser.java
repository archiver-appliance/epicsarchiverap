/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang3.text.StrLookup;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.mergededup.MergeDedupStoragePlugin;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.retrieval.channelarchiver.ChannelArchiverReadOnlyPlugin;
import org.epics.archiverappliance.utils.blackhole.BlackholeStoragePlugin;

import edu.stanford.slac.archiverappliance.PBOverHTTP.PBOverHTTPStoragePlugin;
import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

/**
 * Parses a URL representation of a storage plugin.
 * Storage plugins can optionally implement ETLSource, ETLDest and perhaps other interfaces.
 * This is one stop shopping for initializing all of these from a URL representation.
 * For example, <code>pb://localhost?name=LTS&amp;rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&amp;partitionGranularity=PARTITION_YEAR</code> will initialize a PlainPBStoragePlugin.
 * <ol>
 * <li>The <code>pb</code> prefix initializes {@link edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin PlainPBStoragePlugin}.</li>
 * <li>The <code>pbraw</code> prefix initializes {@link edu.stanford.slac.archiverappliance.PBOverHTTP.PBOverHTTPStoragePlugin PBOverHTTPStoragePlugin}.</li>
 * <li>The <code>blackhole</code> prefix initializes {@link org.epics.archiverappliance.utils.blackhole.BlackholeStoragePlugin BlackholeStoragePlugin}.</li>
 * <li>The <code>rtree</code> prefix initializes {@link org.epics.archiverappliance.retrieval.channelarchiver.ChannelArchiverReadOnlyPlugin ChannelArchiverReadOnlyPlugin}.</li>
 * </ol>
 * @author mshankar
 *
 */
public class StoragePluginURLParser {
	private static Logger logger = LogManager.getLogger(StoragePluginURLParser.class.getName());
	
	
	public static StoragePlugin parseStoragePlugin(String srcURIStr, ConfigService configService) throws IOException {
		try {
			srcURIStr = expandMacros(srcURIStr);
			URI srcURI = new URI(srcURIStr);
			String pluginIdentifier = srcURI.getScheme();
			switch(pluginIdentifier) {
			case "pb" : {
				return parsePlainPBStoragePlugin(srcURIStr, configService);
			}
			case "pbraw" : {
				return parseHTTPStoragePlugin(srcURIStr, configService);
			}
			case "blackhole" : {
				return parseBlackHolePlugin(srcURIStr, configService);
			}
			case "rtree" : {
				return parseChannelArchiverPlugin(srcURIStr, configService);
			}
			case "merge" : {
				return parseMergeDedupPlugin(srcURIStr, configService);
			}
			default : { 
				logger.error("Unsupported plugin " + pluginIdentifier + ". Did you forget to register this?");
			}
			}
		} catch(URISyntaxException ex) {
			throw new IOException(ex);
		}
			
		return null;
	}
	
	public static ETLSource parseETLSource(String srcURIStr, ConfigService configService) throws IOException {
		try {
			srcURIStr = expandMacros(srcURIStr);
			URI srcURI = new URI(srcURIStr);
			String pluginIdentifier = srcURI.getScheme();
			switch(pluginIdentifier) {
			case "pb" : {
				return parsePlainPBStoragePlugin(srcURIStr, configService);
			}
			case "merge" : {
				return parseMergeDedupPlugin(srcURIStr, configService);
			}
			case "blackhole" : {
				logger.warn("The blackhole plugin cannot serve as an ETL source; so it has to be the last plugin in the list of data stores.");
				return null;
			}
			default : { 
				logger.error("Unsupported plugin " + pluginIdentifier + ". Did you forget to register this?");
			}
			}
		} catch(URISyntaxException ex) {
			throw new IOException(ex);
		}
		return null;
	}
	
	public static ETLDest parseETLDest(String srcURIStr, ConfigService configService) throws IOException {
		try {
			srcURIStr = expandMacros(srcURIStr);
			URI srcURI = new URI(srcURIStr);
			String pluginIdentifier = srcURI.getScheme();
			switch(pluginIdentifier) {
			case "pb" : {
				return parsePlainPBStoragePlugin(srcURIStr, configService);
			}
			case "merge" : {
				return parseMergeDedupPlugin(srcURIStr, configService);
			}
			case "blackhole" : {
				return parseBlackHolePlugin(srcURIStr, configService);
			}
			default : { 
				logger.error("Unsupported plugin " + pluginIdentifier + ". Did you forget to register this?");
			}
			}
		} catch(URISyntaxException ex) {
			throw new IOException(ex);
		}

		return null;
	}
	


	private static PlainPBStoragePlugin parsePlainPBStoragePlugin(String srcURIStr, ConfigService configService) throws IOException {
		PlainPBStoragePlugin ret = new PlainPBStoragePlugin();
		ret.initialize(expandMacros(srcURIStr), configService);
		return ret;
	}

	private static PBOverHTTPStoragePlugin parseHTTPStoragePlugin(String srcURIStr, ConfigService configService) throws IOException {
		PBOverHTTPStoragePlugin  ret = new PBOverHTTPStoragePlugin();
		ret.initialize(srcURIStr, configService);
		return ret;
	}
	
	private static BlackholeStoragePlugin parseBlackHolePlugin(String srcURIStr, ConfigService configService) throws IOException {
		BlackholeStoragePlugin  ret = new BlackholeStoragePlugin();
		ret.initialize(srcURIStr, configService);
		return ret;
	}

	private static ChannelArchiverReadOnlyPlugin parseChannelArchiverPlugin(String srcURIStr, ConfigService configService) throws IOException {
		ChannelArchiverReadOnlyPlugin  ret = new ChannelArchiverReadOnlyPlugin();
		ret.initialize(srcURIStr, configService);
		return ret;
	}
	
	private static MergeDedupStoragePlugin parseMergeDedupPlugin(String srcURIStr, ConfigService configService) throws IOException {
		MergeDedupStoragePlugin  ret = new MergeDedupStoragePlugin();
		ret.initialize(srcURIStr, configService);
		return ret;
	}
	
	/**
	 * Expands macros in the plugin definition strings.
	 * Checks java.system.properties first (passed in with a -D to the JVM)
	 * Then checks the environment (for example, using export in Linux).
	 * If we are not able to match in either place, we return as is.
	 * 
	 * For example, if we did <code>export ARCHAPPL_SHORT_TERM_FOLDER=/dev/test</code>, and then used <code>pbraw://${ARCHAPPL_SHORT_TERM_FOLDER}<code> in the policy datastore definition, 
	 * these would be expanded into <code>pbraw:///dev/test<code></code>
	 * @param srcURIStr
	 * @return
	 */
	private static String expandMacros(String srcURIStr) {
		StrSubstitutor sub = new StrSubstitutor(new StrLookup<String>() {
			@Override
			public String lookup(String name) {
				String valueFromProps = System.getProperty(name);
				if(valueFromProps != null) {
					if(logger.isDebugEnabled()) logger.debug("Resolving " + name + " from system properties into " + valueFromProps);
					return valueFromProps;
				}
				String valueFromEnvironment = System.getenv(name);
				if(valueFromEnvironment != null) {
					if(logger.isDebugEnabled()) logger.debug("Resolving " + name + " from system environment into " + valueFromEnvironment);
					return valueFromEnvironment;
				}
				logger.error("Unable to find " + name + " in either the java system properties or the system environment. Returning as is without expanding");
				return name;
			}
		});
		return sub.replace(srcURIStr);
	}
}
