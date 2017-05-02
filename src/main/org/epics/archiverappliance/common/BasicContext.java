package org.epics.archiverappliance.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.utils.nio.ArchPaths;

/**
 * All the main units of work in the appliance that deal with plugins pass in a context that can be used to hold state that pertains to the unit of work.
 * For now, these are 
 * <ol>
 * <li>The main writer thread in the engine that writes to the short term store.</li>
 * <li>The ETL jobs that transfer data from one store to the other.</li>
 * <li>A client retrieval that gets data from the plugin.</li>
 * </ol>
 * This class contains functionality that plugins can rely on in all these contexts.
 * Callers are expected to call the close method on completion of the unit of work.
 * Contexts are not expected to be thread safe and are particular to the thread of execution.
 * @author mshankar
 *
 */
public class BasicContext implements Closeable {
	private ArchPaths paths = new ArchPaths();
	private LinkedList<Closeable> resources = new LinkedList<Closeable>();
	/**
	 * During retrieval, the controller can set this to the expected DBR types.
	 * Plugins can use this to convert or throw an exception if the expected DBR type does not match the DBR type in the plugin
	 */
	private ArchDBRTypes retrievalExpectedDBRType = null;
	
	/**
	 * Some plugins do not support EPICS aliases; for these, we have to pass in the name of the PV as is to get data back.
	 */
	private String pvNameFromRequest = null;
	
	
	public BasicContext() { 
		
	}
	
	public BasicContext(ArchDBRTypes retrievalExpectedDBRType, String pvNameFromRequest) {
		this.retrievalExpectedDBRType = retrievalExpectedDBRType;
		this.pvNameFromRequest = pvNameFromRequest;
	}

	/**
	 * The PlainPB plugin deals with paths that can be translated into NIO by our implementation of Paths.
	 * We generate one per context so that things can be closed correctly.
	 * @return paths ArchPaths
	 */
	public ArchPaths getPaths() {
		return paths;
	}
	
	/**
	 * Add a resource that needs to be closed once we finish the unit of work..
	 * @param resource Closeable resouce
	 */
	public void addResource(Closeable resource) {
		resources.add(resource);
	}
	

	@Override
	public void close() throws IOException {
		try { paths.close(); } catch (Throwable t) {}
		for(Closeable resource : resources) {
			try { resource.close(); } catch (Throwable t) {}
		}
	}

	/**
	 * @return retrievalExpectedDBRType ArchDBRTypes
	 */
	public ArchDBRTypes getRetrievalExpectedDBRType() {
		return retrievalExpectedDBRType;
	}

	/**
	 * @return pvNameFromRequest  &emsp;
	 */
	public String getPvNameFromRequest() {
		return pvNameFromRequest;
	}
}
