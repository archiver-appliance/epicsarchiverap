package edu.stanford.slac.archiverappliance.PlainPB;

import org.epics.archiverappliance.Event;

import java.io.Closeable;
import java.util.Iterator;

public interface FileBackedPBEventStreamIterator extends Iterator<Event>, Closeable {}
