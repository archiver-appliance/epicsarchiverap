package org.epics.archiverappliance.mgmt;

import java.io.IOException;
import java.io.StringWriter;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;

/**
 * Make sure that the webapp has completely started up before we allow any UI actions..
 * @author mshankar
 *
 */
public class MgmtUIFilter implements Filter {
	private static Logger logger = LogManager.getLogger(MgmtUIFilter.class.getName());
	private FilterConfig filterConfig = null;
	
	@Override
	public void destroy() {
		this.filterConfig = null;
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		ConfigService configService = (ConfigService) filterConfig.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
		if(configService == null) { 
			// Geyang ran into an issue where initializing of the config service failed because of DNS reverse lookup issues. 
			// Hopefully, this gives more info during the initial installation.
			HttpServletResponse resp = ((HttpServletResponse)response);
			StringWriter errorMessage = new StringWriter();
			errorMessage.write("The config service for this installation did not start up correctly. Please check the logs for any exceptions or FATAL errors.");
			if(filterConfig.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME + ".exception") != null) { 
				errorMessage.append("\n");
				errorMessage.append((String) filterConfig.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME + ".exception"));
			}
			if(filterConfig.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME + ".stacktrace") != null) { 
				errorMessage.append("\n<i>");
				errorMessage.append((String) filterConfig.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME + ".stacktrace"));
				errorMessage.append("</i>");
			}
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, errorMessage.toString());
			return;
		}
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
