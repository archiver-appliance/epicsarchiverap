package org.epics.archiverappliance.mgmt.pva;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.GenericServlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
//import javax.servlet.http.HttpServlet;

import org.epics.archiverappliance.config.ArchServletContextListener;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.pvaccess.PVAException;
import org.epics.pvaccess.server.rpc.RPCServer;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;
import static org.epics.archiverappliance.mgmt.pva.PvaPvMgmtService.PVA_PV_MGMT_SERVICE;

/**
 * A servlet registered with the mgmt web.xml to be initialized with the context
 * provided by {@link ArchServletContextListener}
 * 
 * @author Kunal Shroff
 *
 */
public class PvaServlet extends GenericServlet {

	private static Logger logger = Logger.getLogger(PvaServlet.class.getName());

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	ExecutorService executorService = Executors.newSingleThreadExecutor();
	private final RPCServer server = new RPCServer(20, 1);

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		ConfigService configService = (ConfigService) getServletConfig().getServletContext()
				.getAttribute(ConfigService.CONFIG_SERVICE_NAME);
		logger.info("FFFFFFFFFFFFF "+ZonedDateTime.now(ZoneId.systemDefault()) + PVA_MGMT_SERVICE + " initializing...");
		server.registerService(PVA_MGMT_SERVICE, new PvaMgmtService(configService));
		server.registerService(PVA_PV_MGMT_SERVICE, new PvaPvMgmtService(configService));
		logger.info("FFFFFFFFFFFFFFF");
		executorService.execute(() -> {
			try {
				logger.info(Thread.currentThread().getName());
				logger.info("FFFFFFFFFFFFFFF pva services registered and now the server is starting.");
				server.printInfo();
				server.run(0);
			} catch (PVAException e) {
				logger.log(Level.SEVERE, "Failed to start service : " + PVA_MGMT_SERVICE, e);
			}
		});
		logger.info(ZonedDateTime.now(ZoneId.systemDefault()) + PVA_MGMT_SERVICE + " is operational.");
	}

	@Override
	public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {

	}

	@Override
	public void destroy() {
		// TODO the shutdown can be improved
		logger.info("Shutting down service " + PVA_MGMT_SERVICE);
		try {
			server.destroy();
			executorService.shutdown();
			logger.info(PVA_MGMT_SERVICE + " Shutdown complete.");
		} catch (PVAException e) {
			logger.log(Level.SEVERE, "Failed to close service : " + PVA_MGMT_SERVICE, e);
		}
		super.destroy();
	}

}
