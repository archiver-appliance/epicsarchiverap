package edu.stanford.slac.archiverappliance.plain;

import org.epics.archiverappliance.Event;

import java.io.Closeable;
import java.util.Iterator;

public interface EventStreamIterator extends Iterator<Event>, Closeable {}
