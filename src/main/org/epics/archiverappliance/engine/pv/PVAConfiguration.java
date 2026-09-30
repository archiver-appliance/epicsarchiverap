package org.epics.archiverappliance.engine.pv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;

public record PVAConfiguration(long pvaReadTimeoutSecs) {
    private static final Logger logger = LogManager.getLogger(PVAConfiguration.class);

    private static final String PVA_READ_TIMEOUT_PROPERTY =
            "org.epics.archiverappliance.engine.epics.pvaReadTimeoutSecs";
    private static final long DEFAULT_PVA_READ_TIMEOUT_SECS = 10;

    public PVAConfiguration(ConfigService configService) {
        this(resolvePvaReadTimeout(configService));
    }

    private static void invalidPvaReadTimeoutWarning(String configured, NumberFormatException ex) {
        logger.warn(
                "Invalid {} value {}; using {} seconds",
                PVA_READ_TIMEOUT_PROPERTY,
                configured,
                DEFAULT_PVA_READ_TIMEOUT_SECS,
                ex);
    }

    private static void invalidPvaReadTimeoutWarning(String configured) {
        invalidPvaReadTimeoutWarning(configured, null);
    }

    private static long resolvePvaReadTimeout(ConfigService configService) {
        if (configService == null || configService.getInstallationProperties() == null) {
            return DEFAULT_PVA_READ_TIMEOUT_SECS;
        }
        String configured = configService
                .getInstallationProperties()
                .getProperty(PVA_READ_TIMEOUT_PROPERTY, Long.toString(DEFAULT_PVA_READ_TIMEOUT_SECS));
        try {
            long timeout = Long.parseLong(configured);
            if (timeout > 0) return timeout;
        } catch (NumberFormatException ex) {
            invalidPvaReadTimeoutWarning(configured, ex);
            return DEFAULT_PVA_READ_TIMEOUT_SECS;
        }
        invalidPvaReadTimeoutWarning(configured);
        return DEFAULT_PVA_READ_TIMEOUT_SECS;
    }
}
