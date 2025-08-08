package org.epics.archiverappliance.retrieval;

import org.epics.archiverappliance.Event;

public record PVWithData(String pvName, Event event) {}
