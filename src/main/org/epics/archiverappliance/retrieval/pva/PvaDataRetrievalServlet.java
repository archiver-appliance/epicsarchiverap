package org.epics.archiverappliance.retrieval.pva;

import static org.epics.archiverappliance.retrieval.pva.PvaDataRetrievalService.PVA_DATA_SERVICE;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.GenericServlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.pvaccess.PVAException;
import org.epics.pvaccess.server.rpc.RPCServer;

public class PvaDataRetrievalServlet extends GenericServlet {

	private static Logger logger = LogManager.getLogger(PvaDataRetrievalServlet.class.getName());

	/**
	 * 
	 */
	private static final long serialVersionUID = 9178874095748814721L;

	ExecutorService executorService = Executors.newSingleThreadExecutor();
	private final RPCServer server = new RPCServer(20, 1);

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		ConfigService configService = (ConfigService) getServletConfig().getServletContext()
				.getAttribute(ConfigService.CONFIG_SERVICE_NAME);
		server.registerService(PVA_DATA_SERVICE, new PvaDataRetrievalService(configService));
		executorService.execute(() -> {
			try {
				logger.info(Thread.currentThread().getName());
				server.printInfo();
				server.run(0);
			} catch (PVAException e) {
				logger.log(Level.FATAL, "Failed to start service : " + PVA_DATA_SERVICE, e);
			}
		});
		logger.info(ZonedDateTime.now(ZoneId.systemDefault()) + PVA_DATA_SERVICE + " is operational.");
	}

	@Override
	public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {

	}

	@Override
	public void destroy() {
		// TODO the shutdown can be improved
		logger.info("Shutting down service " + PVA_DATA_SERVICE);
		try {
			server.destroy();
			executorService.shutdown();
			logger.info(PVA_DATA_SERVICE + " Shutdown complete.");
		} catch (PVAException e) {
			logger.log(Level.FATAL, "Failed to close service : " + PVA_DATA_SERVICE, e);
		}
		super.destroy();
	}
}
