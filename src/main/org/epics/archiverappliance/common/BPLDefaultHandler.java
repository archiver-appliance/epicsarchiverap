package org.epics.archiverappliance.common;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.config.ConfigService;

public interface BPLDefaultHandler {
	public boolean handleRequest(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException;
}
