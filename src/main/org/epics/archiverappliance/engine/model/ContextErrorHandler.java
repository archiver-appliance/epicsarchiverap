/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.aps.jca.event.ContextExceptionEvent;
import gov.aps.jca.event.ContextExceptionListener;
import gov.aps.jca.event.ContextMessageEvent;
import gov.aps.jca.event.ContextMessageListener;
import gov.aps.jca.event.ContextVirtualCircuitExceptionEvent;

/**
 * Handler for JCA Context errors and messages; places them in log.
 * 
 * @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class ContextErrorHandler implements ContextExceptionListener,
		ContextMessageListener {
	private static final Logger logger = LogManager.getLogger(ArchiveChannel.class);

	/** @see ContextExceptionListener */
	@Override
	public void contextException(final ContextExceptionEvent ev) {

		logger.warn("Channel Access Exception from " + ev.getSource() + ":"
				+ ev.getMessage());
	}

	/** @see ContextExceptionListener */
	@Override
	public void contextVirtualCircuitException(
			ContextVirtualCircuitExceptionEvent ev) {
		// nop
	}

	/** @see ContextMessageListener */
	@Override
	public void contextMessage(final ContextMessageEvent ev) {

		logger.info("Channel Access Message from " + ev.getSource() + ":"
				+ ev.getMessage());
	}
}
