package org.epics.archiverappliance.mgmt;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;

/**
 * Make sure that the webapp has completely started up before we allow any UI actions..
 * @author mshankar
 *
 */
public class MgmtUIFilter implements Filter {
	private static Logger logger = Logger.getLogger(MgmtUIFilter.class.getName());
	private FilterConfig filterConfig = null;
	
	@Override
	public void destroy() {
		this.filterConfig = null;
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		ConfigService configService = (ConfigService) filterConfig.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
		MgmtRuntimeState runtime = configService.getMgmtRuntimeState();
		if(runtime.haveChildComponentsStartedUp()) {
			chain.doFilter(request, response);
		} else {
			logger.warn("Trying to access the UI before child components have started up.");
			HttpServletResponse resp = ((HttpServletResponse)response);
			resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "This appliance is still starting up. Please wait a few minutes before trying again. Thank you for your patience.");
			return;
		}
	}

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		this.filterConfig = filterConfig;
	}
	
}
