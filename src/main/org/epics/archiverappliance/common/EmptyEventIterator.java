package org.epics.archiverappliance.common;

import java.util.Iterator;

import org.epics.archiverappliance.Event;

/**
 * An empty event interator.
 * @author mshankar
 *
 */
public class EmptyEventIterator implements Iterator<Event> {
	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public Event next() {
		return null;
	}

	@Override
	public void remove() {
	}
}
