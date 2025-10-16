package org.epics.archiverappliance.mgmt.pva;

import static org.epics.archiverappliance.mgmt.pva.PvaMgmtService.PVA_MGMT_SERVICE;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ArchServletContextListener;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.pva.server.PVAServer;
import org.epics.pva.server.ServerPV;

import java.io.IOException;
import java.io.Serial;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import jakarta.servlet.GenericServlet;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;

/**
 * A servlet registered with the mgmt web.xml to be initialized with the context
 * provided by {@link ArchServletContextListener}
 *
 * @author Kunal Shroff
 *
 */
public class PvaServlet extends GenericServlet {

    private static final Logger logger = LogManager.getLogger(PvaServlet.class.getName());

    @Serial
    private static final long serialVersionUID = 1L;

    private final PVAServer server;
    private ServerPV serverPV;

    public PvaServlet() throws Exception {
        server = new PVAServer();
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        ConfigService configService =
                (ConfigService) getServletConfig().getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
        serverPV = server.createPV(PVA_MGMT_SERVICE, new PvaMgmtService(configService));
        logger.info(ZonedDateTime.now(ZoneId.systemDefault()) + PVA_MGMT_SERVICE + " is operational.");
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {}

    @Override
    public void destroy() {
        logger.info("Shutting down service " + PVA_MGMT_SERVICE);
        serverPV.close();
        server.close();
        logger.info(PVA_MGMT_SERVICE + " Shutdown complete.");
        super.destroy();
    }
}
