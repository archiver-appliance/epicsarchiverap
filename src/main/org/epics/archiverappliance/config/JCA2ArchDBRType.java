package org.epics.archiverappliance.config;

import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

/**
 * Mapping between {@link ArchDBRTypes} and {@link DBRType}
 */
public enum JCA2ArchDBRType {
    JCAMAPPING_SCALAR_STRING(DBRType.TIME_STRING, false, ArchDBRTypes.DBR_SCALAR_STRING),
    JCAMAPPING_SCALAR_SHORT(DBRType.TIME_SHORT, false, ArchDBRTypes.DBR_SCALAR_SHORT),
    JCAMAPPING_SCALAR_FLOAT(DBRType.TIME_FLOAT, false, ArchDBRTypes.DBR_SCALAR_FLOAT),
    JCAMAPPING_SCALAR_ENUM(DBRType.TIME_ENUM, false, ArchDBRTypes.DBR_SCALAR_ENUM),
    JCAMAPPING_SCALAR_BYTE(DBRType.TIME_BYTE, false, ArchDBRTypes.DBR_SCALAR_BYTE),
    JCAMAPPING_SCALAR_INT(DBRType.TIME_INT, false, ArchDBRTypes.DBR_SCALAR_INT),
    JCAMAPPING_SCALAR_DOUBLE(DBRType.TIME_DOUBLE, false, ArchDBRTypes.DBR_SCALAR_DOUBLE),
    JCAMAPPING_WAVEFORM_STRING(DBRType.TIME_STRING, true, ArchDBRTypes.DBR_WAVEFORM_STRING),
    JCAMAPPING_WAVEFORM_SHORT(DBRType.TIME_SHORT, true, ArchDBRTypes.DBR_WAVEFORM_SHORT),
    JCAMAPPING_WAVEFORM_FLOAT(DBRType.TIME_FLOAT, true, ArchDBRTypes.DBR_WAVEFORM_FLOAT),
    JCAMAPPING_WAVEFORM_ENUM(DBRType.TIME_ENUM, true, ArchDBRTypes.DBR_WAVEFORM_ENUM),
    JCAMAPPING_WAVEFORM_BYTE(DBRType.TIME_BYTE, true, ArchDBRTypes.DBR_WAVEFORM_BYTE),
    JCAMAPPING_WAVEFORM_INT(DBRType.TIME_INT, true, ArchDBRTypes.DBR_WAVEFORM_INT),
    JCAMAPPING_WAVEFORM_DOUBLE(DBRType.TIME_DOUBLE, true, ArchDBRTypes.DBR_WAVEFORM_DOUBLE);

    private static final Logger logger = LogManager.getLogger(JCA2ArchDBRType.class.getName());
    private final DBRType dbrtype;
    private final boolean waveform;
    private final ArchDBRTypes archDBRType;

    JCA2ArchDBRType(DBRType rawDBRType, boolean isWaveform, ArchDBRTypes archDBRType) {
        this.dbrtype = rawDBRType;
        this.waveform = isWaveform;
        this.archDBRType = archDBRType;
    }

    /**
     * Get the equivalent archiver data type given a JCA DBR
     * @param d JCA DBR
     * @return ArchDBRTypes  &emsp;
     */
    public static ArchDBRTypes valueOf(DBR d) {
        boolean isVector = (d.getCount() > 1);
        DBRType dt = d.getType();
        var res = Arrays.stream(JCA2ArchDBRType.values())
                .filter(t -> t.waveform == isVector && t.dbrtype.equals(dt))
                .findFirst();
        if (res.isPresent())
            return res.get().archDBRType;
        logger.error("Cannot determine ArchDBRType for DBRType " + (dt != null ? dt.getName() : "null") + " and count "
                + d.getCount());
        return null;
    }
}
