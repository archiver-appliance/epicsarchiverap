package edu.stanford.slac.archiverappliance.PlainPB;

import java.io.Closeable;
import java.util.Iterator;

import org.epics.archiverappliance.Event;

public interface FileBackedPBEventStreamIterator extends Iterator<Event>, Closeable{

}
