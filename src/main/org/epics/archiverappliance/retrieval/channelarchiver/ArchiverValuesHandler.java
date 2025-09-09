/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.channelarchiver;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.ChangeInYearsException;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;

/** Quick handler to parse archiver.values returns.
 * We use STAX to enable stream processing at least from this side of the socket...
 *
 * @author mshankar
 */
public class ArchiverValuesHandler implements XMLRPCStaxProcessor, EventStream, Iterator<Event>, RemotableOverRaw {
	private static final Logger logger = LogManager.getLogger(ArchiverValuesHandler.class.getName());

	public enum ArchiverValuesType {
		// See page 43 of the Channel Archiver Manual for this...
		CHANNEL_ARCHIVER_STRING(0),
		CHANNEL_ARCHIVER_ENUM(1),
		CHANNEL_ARCHIVER_INT(2),
		CHANNEL_ARCHIVER_DOUBLE(3);

		final int val;
		ArchiverValuesType(int val) {
			this.val = val;
		}

		static final HashMap<Integer, ArchiverValuesType> int2Enum = new HashMap<Integer, ArchiverValuesType>();
		static {
			for(ArchiverValuesType type : ArchiverValuesType.values()) {
				int2Enum.put(type.val, type);
			}
		}

		static ArchiverValuesType lookup(int val) {
			return int2Enum.get(val);
		}

		ArchDBRTypes getDBRType(int elementCount) {
			if(elementCount > 1) {
                return switch (this) {
                    case CHANNEL_ARCHIVER_STRING -> ArchDBRTypes.DBR_WAVEFORM_STRING;
                    case CHANNEL_ARCHIVER_DOUBLE -> ArchDBRTypes.DBR_WAVEFORM_DOUBLE;
                    case CHANNEL_ARCHIVER_ENUM -> ArchDBRTypes.DBR_WAVEFORM_ENUM;
                    case CHANNEL_ARCHIVER_INT -> ArchDBRTypes.DBR_WAVEFORM_INT;
                };
			} else {
                return switch (this) {
                    case CHANNEL_ARCHIVER_STRING -> ArchDBRTypes.DBR_SCALAR_STRING;
                    case CHANNEL_ARCHIVER_DOUBLE -> ArchDBRTypes.DBR_SCALAR_DOUBLE;
                    case CHANNEL_ARCHIVER_ENUM -> ArchDBRTypes.DBR_SCALAR_ENUM;
                    case CHANNEL_ARCHIVER_INT -> ArchDBRTypes.DBR_SCALAR_INT;
                };
			}
		}
	}

	/**
	 * The PV for which we are processing data
	 */
	private final String pvName;

	/**
	 * Maintains the current node tree. Start element adds an element to this list while stop element removes the last added element.
	 */
	LinkedList<String> currentNodes = new LinkedList<String>();

	/**
	 * The last member.name that we saw; used as the name part of the name value pair in the hashmap.
	 */
	private String lastName = null;

	/**
	 * Are we in the midst of processing the meta information from the Channel Archiver?
	 */
	private boolean inMeta = false;
	/**
	 * Are we in the midst of processing the values portion of the XML document from the Channel Archiver.
	 */
	private boolean inValues = false;

	/**
	 * What type of values do we have? The ChannelA Archiver maps all the EPICS types into 4 types.
	 */
	private ArchiverValuesType valueType = null;
	/**
	 * Scalars have an element count of 1; vectors more than 1
	 */
	private int elementCount = -1;

	/**
	 * The ArchDBR Type
	 */
	private ArchDBRTypes dbrType = null;
	/**
	 * What we use to construct this into an event
	 */
	private Constructor<? extends DBRTimeEvent> serializingConstructor = null;

	/**
	 * Name value pairs for the meta information like limits etc
	 */
	private final HashMap<String, String> metaInformation = new HashMap<String, String>();
	/**
	 * A potentially partial version of the current event that is still in the process of being built.
	 * Once built, this becomes the current event
	 */
	private HashMap<String, Object> workingCopyOfEvent = null;
	/**
	 * The current completely built event
	 */
	private DBRTimeEvent currentEvent = null;

	/**
	 * Used to detect ChangeInYearsException
	 */
	private short yearOfCurrentEvent = -1;

	/**
	 * Used to detect ChangeInYearsException
	 */
	private short yearOfPreviousEvent = -1;

	/**
	 * Used to enforce monotonically increasing timestamps
	 */
	private long previousEventEpochSeconds = -1;

	/**
	 *  Used to throw ChangeInYearsException
	 */
	private boolean throwYearTransitionException = false;

	@Override
	public boolean startElement(String localName) throws IOException {
		currentNodes.add(localName);

		if(inValues && localName.equals("struct")) {
				workingCopyOfEvent = new HashMap<String, Object>();
			}


		return true;
	}



	@Override
	public boolean endElement(String localName, String value) throws IOException {
		boolean continueProcessing = true;
		String lastTwoNodes = getLastTwoNodes();

		String poppedElement = currentNodes.pollLast();
		assert(localName.equals(poppedElement));

		if(lastTwoNodes.equals("member.name")) {
			lastName = value;

			if(lastName.equals("meta")) {
				// <member><name>meta starts the meta section
				inMeta = true;
				return continueProcessing;
			} else if(lastName.equals("values")) {
				// <member><name>values starts the values section
				inMeta = false;
				inValues = true;
				return continueProcessing;
			}
		}

		if(!inMeta && !inValues && lastName != null && lastName.equals("name") && lastTwoNodes.equals("value.string")) {
					// This is the PV's name
            metaInformation.put("pvName", value);
					lastName = null;
				}


		if(inMeta) {
			if(localName.equals("struct")) {
				// meta is a struct of name/value pairs. If we encounter a end-struct in meta, we can assume that we are done with meta processing.
				inMeta = false;
				return continueProcessing;
			}

			if(lastTwoNodes.equals("value.i4") || lastTwoNodes.equals("value.string") || lastTwoNodes.equals("value.double")) {
				// All meta information I've see so far fits into int/string/double.
                metaInformation.put(lastName, value);
				lastName = null;
			}
		}

		if(!inMeta && lastName != null) {
			// The type and element count that serve to determine the ArchDBRTYPE come between the meta and values
			if(lastName.equals("type") && lastTwoNodes.equals("value.i4")) {
                valueType = ArchiverValuesType.lookup(Integer.parseInt(value));
				lastName = null;
			} else if(lastName.equals("count") && lastTwoNodes.equals("value.i4")) {
                elementCount = Integer.parseInt(value);
				lastName = null;
			}

			// The Channel Archiver upconverts floats to doubles and so on.
			// The appliance cares a lot about types.
			// This piece of code attempts to bridge the gap.
			if(serializingConstructor == null) {
				// The caller has passed in an expected DBR Type perhaps because we are archiving in the appliance as this type.
				if(this.expectedDBRType != null) {
					logger.debug("Using expected DBR type of " + expectedDBRType);
					dbrType = expectedDBRType;
				} else {
					// We did not get an expected type so we make a best guess
					if(valueType != null && elementCount != -1) {
						logger.debug("Inferring dbrtype from the value type and element count" + getValueType() + " and " + getElementCount());
						dbrType = getValueType().getDBRType(getElementCount());
					}
				}
				if(dbrType != null) {
					serializingConstructor = DBR2PBTypeMapping.getPBClassFor(dbrType).getSerializingConstructor();
				}
			}
		}

		if(inValues) {
			if(localName.equals("struct") && (workingCopyOfEvent != null)) {
				// Encountering a end struct in the values section marks the end of the current event.
				try {
					currentEvent = (DBRTimeEvent) serializingConstructor.newInstance(new HashMapEvent(dbrType, workingCopyOfEvent));
					long currentEventEpochSeconds = currentEvent.getEpochSeconds();
					if(previousEventEpochSeconds > 0 && currentEventEpochSeconds < previousEventEpochSeconds) {
							logger.error("Skipping decreasing timestamp from CA " + TimeUtils.convertToHumanReadableString(currentEventEpochSeconds) + " and previous " + TimeUtils.convertToHumanReadableString(previousEventEpochSeconds));
							currentEvent = null;
							workingCopyOfEvent = null;
							return continueProcessing;
						}

					previousEventEpochSeconds = currentEventEpochSeconds;
					yearOfPreviousEvent = yearOfCurrentEvent;
					yearOfCurrentEvent = TimeUtils.computeYearForEpochSeconds(currentEventEpochSeconds);
					if(yearOfPreviousEvent != -1 && yearOfCurrentEvent != yearOfPreviousEvent) {
						// The ChannelArchiver does not have the same contract as us and the data can span years.
						// If the years change, we throw an specific exception that the retrievers can ignore/handle
						throwYearTransitionException = true;
					}
					workingCopyOfEvent = null;
					continueProcessing = false;
				} catch(IllegalAccessException | InstantiationException | InvocationTargetException ex) {
					logger.error("Exception serializing DBR Type " + dbrType + " for pv " + this.pvName, ex);
					currentEvent = null;
                }  catch(NumberFormatException ex) {
					// We ignore all samples that cannot be parsed
					logger.error("Ignoring sample that cannot be parsed " + dbrType + " for pv " + this.pvName, ex);
					currentEvent = null;
				}
			} else if(lastName != null && (lastTwoNodes.equals("value.i4") || lastTwoNodes.equals("value.string") || lastTwoNodes.equals("value.double"))) {
                if(lastName.equals("value") && dbrType.isWaveForm()) {
						// No choice but to add this SuppressWarnings here.
						@SuppressWarnings("unchecked")
						LinkedList<String> vals = (LinkedList<String>) workingCopyOfEvent.get(lastName);
						if(vals == null) {
							vals = new LinkedList<String>();
							workingCopyOfEvent.put(lastName, vals);
						}
						vals.add(value);
					} else {
						workingCopyOfEvent.put(lastName, value);
					}
					if(!dbrType.isWaveForm()) {
						lastName = null;
					}
				}

		}




		return continueProcessing;
	}

	/**
	 * Return the last two nodes in dotted notation.
	 * @return
	 */
	private String getLastTwoNodes() {
		if(currentNodes.size() <= 2) return "";
		StringWriter buf = new StringWriter();
		buf.append(currentNodes.get(currentNodes.size() - 2));
		buf.append(".");
		buf.append(currentNodes.getLast());
		return buf.toString();
	}


	private StringWriter buf = new StringWriter();
	private XMLStreamReader streamReader = null;
	private InputStream is = null;
	private String source = null;
	private ArchDBRTypes expectedDBRType = null;

	/**
	 * Create a archive.values handler given an event stream.
	 * If all goes well, processing should stop after each event (and hence should stop after the first event).
	 * @param pvName The name of PV
	 * @param is  InputStream
	 * @param source  &emsp;
	 * @param expectedDBRType This is the expected DBR type. This can be null in which case we do a best guess.
	 * @throws IOException  &emsp;
	 */
	public ArchiverValuesHandler(String pvName, InputStream is, String source, ArchDBRTypes expectedDBRType) throws IOException {
		this.pvName = pvName;
		this.source = source;
		try {
			XMLInputFactory f = XMLInputFactory.newInstance();
			this.is = is;
			this.expectedDBRType = expectedDBRType;
			streamReader = f.createXMLStreamReader(this.is);
			boolean continueProcessing = true;
			while(streamReader.hasNext() && continueProcessing) {
				switch(streamReader.getEventType()) {
				case XMLStreamConstants.START_ELEMENT:
					continueProcessing = this.startElement(streamReader.getLocalName());
					buf = new StringWriter();
					break;
				case XMLStreamConstants.CHARACTERS:
					String chars = streamReader.getText();
					buf.append(chars);
					break;
				case XMLStreamConstants.END_ELEMENT:
					String text = buf.toString();
					continueProcessing = this.endElement(streamReader.getLocalName(), text);
					buf = new StringWriter();
					break;
				default:
					// Should not really be here. Don't do anything..
				}
				streamReader.next();
			}
		} catch(XMLStreamException ex) {
			throw new IOException("Exception from " + source + " for pv " + this.pvName, ex);
		}
	}

	/**
	 * Do we have another event?
	 * @return boolean True or False
	 */
	@Override
	public boolean hasNext() {
		if(throwYearTransitionException) {
			throwYearTransitionException = false;
			throw new ChangeInYearsException(yearOfPreviousEvent, yearOfCurrentEvent);
		}
		return currentEvent != null;
	}

	/**
	 * Get the next event
	 * @return Event get the next event
	 */
	@Override
	public Event next() {
		DBRTimeEvent retVal = currentEvent;
		currentEvent = null;
		try {
			boolean continueProcessing = true;
			while(streamReader.hasNext() && continueProcessing) {
				switch(streamReader.getEventType()) {
				case XMLStreamConstants.START_ELEMENT:
					continueProcessing = this.startElement(streamReader.getLocalName());
					buf = new StringWriter();
					break;
				case XMLStreamConstants.CHARACTERS:
					String chars = streamReader.getText();
					buf.append(chars);
					break;
				case XMLStreamConstants.END_ELEMENT:
					String text = buf.toString();
					continueProcessing = this.endElement(streamReader.getLocalName(), text);
					buf = new StringWriter();
					break;
				default:
					// Should not really be here. Don't do anything..
				}
				streamReader.next();
			}
		} catch(ChangeInYearsException ex) {
			throw ex;
		} catch(Exception ex) {
			logger.error("Exception determining next event for pv " + this.pvName, ex);
			currentEvent = null;
		}
		return retVal;
	}



	public HashMap<String, String> getMetaInformation() {
		return metaInformation;
	}



	public ArchiverValuesType getValueType() {
		return valueType;
	}



	public int getElementCount() {
		return elementCount;
	}



	@Override
	public void close() throws IOException {
		try {
			if (streamReader != null) {
				streamReader.close();
			}
			streamReader = null;
		} catch(Throwable t) {
			logger.error("Exception closing XML STAX reader", t);
		}
		try {
			if(is != null) {
				is.close();
			}
			is = null;
		} catch(Throwable t) {
			logger.error("Exception closing input stream", t);
		}
	}



	@Override
	public Iterator<Event> iterator() {
		return this;
	}



	@Override
	public RemotableEventStreamDesc getDescription() {
		RemotableEventStreamDesc retVal = new RemotableEventStreamDesc(dbrType, this.pvName, yearOfCurrentEvent);
		retVal.setSource(source);
		retVal.setElementCount(elementCount);
		addMappedHeader("units", "EGU", retVal);
		addMappedHeader("prec", "PREC", retVal);
		return retVal;
	}

	/**
	 * Map a header from ChannelArchiver names to EPICS Archiver appliance names.
	 * @param caName the channel archiver name
	 * @param applName  the name used in the appliance
	 * @param desc RemotableEventStreamDesc
	 */
	private void addMappedHeader(String caName, String applName, RemotableEventStreamDesc desc) {
		if(this.metaInformation.containsKey(caName)) {
			desc.addHeader(applName, this.metaInformation.get(caName));
		}
	}


	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
