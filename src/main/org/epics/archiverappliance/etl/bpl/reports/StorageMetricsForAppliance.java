package org.epics.archiverappliance.etl.bpl.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;

import java.io.IOException;
import java.io.PrintWriter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class StorageMetricsForAppliance implements BPLAction {

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        try (PrintWriter out = resp.getWriter()) {
            out.println(StorageWithLifetime.getStorageMetrics(configService));
        }
    }
}
