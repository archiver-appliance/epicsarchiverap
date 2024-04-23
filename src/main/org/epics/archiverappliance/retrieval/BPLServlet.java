/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.BasicDispatcher;
import org.epics.archiverappliance.common.GetVersion;
import org.epics.archiverappliance.common.ProcessMetricsReport;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.retrieval.bpl.AreWeArchivingPV;
import org.epics.archiverappliance.retrieval.bpl.FilterArchivedPVs;
import org.epics.archiverappliance.retrieval.bpl.GetClientConfiguration;
import org.epics.archiverappliance.retrieval.bpl.GetMatchingPVs;
import org.epics.archiverappliance.retrieval.bpl.GetPVMetaData;
import org.epics.archiverappliance.retrieval.bpl.ResetFailoverCachesForThisAppliance;
import org.epics.archiverappliance.retrieval.bpl.SearchForPVsRegex;
import org.epics.archiverappliance.retrieval.bpl.reports.ApplianceMetrics;
import org.epics.archiverappliance.retrieval.bpl.reports.ApplianceMetricsDetails;
import org.epics.archiverappliance.retrieval.bpl.reports.InstanceReportDetails;

import java.io.IOException;
import java.util.HashMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * The main business logic servlet for retrieval. All BPLActions are registered here.
 * @author mshankar
 *
 */
public class BPLServlet extends HttpServlet {
    private static final long serialVersionUID = 7987830282574602915L;
    private ConfigService configService = null;
    private static HashMap<String, Class<? extends BPLAction>> getActions =
            new HashMap<String, Class<? extends BPLAction>>();

    static {
        getActions.put("/getApplianceMetrics", ApplianceMetrics.class);
        getActions.put("/getApplianceMetricsForAppliance", ApplianceMetricsDetails.class);
        getActions.put("/getInstanceMetricsForAppliance", InstanceReportDetails.class);
        getActions.put("/searchForPVsRegex", SearchForPVsRegex.class);
        getActions.put("/getMatchingPVs", GetMatchingPVs.class);
        getActions.put("/getProcessMetrics", ProcessMetricsReport.class);
        getActions.put("/getVersion", GetVersion.class);
        getActions.put("/getClientConfig", GetClientConfiguration.class);
        getActions.put("/getMetadata", GetPVMetaData.class);
        getActions.put("/areWeArchivingPV", AreWeArchivingPV.class);
        getActions.put("/resetFailoverCachesForThisAppliance", ResetFailoverCachesForThisAppliance.class);
    }

    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        BasicDispatcher.dispatch(req, resp, configService, getActions);
    }

    private static HashMap<String, Class<? extends BPLAction>> postActions =
            new HashMap<String, Class<? extends BPLAction>>();

    static {
        postActions.put("/filterArchivedPVs", FilterArchivedPVs.class);
        // Uncomment after adding readonly support for the client config.
        // postActions.put("/putClientConfig", PutClientConfiguration.class);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        BasicDispatcher.dispatch(req, resp, configService, postActions);
    }

    @Override
    public void init() throws ServletException {
        this.configService = (ConfigService) this.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
    }
}
