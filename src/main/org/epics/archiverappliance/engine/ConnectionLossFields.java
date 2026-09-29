package org.epics.archiverappliance.engine;

public enum ConnectionLossFields {
    CNX_LOST_EPSECS("cnxlostepsecs"),
    CNX_REGAINED_EPSECS("cnxregainedepsecs"),
    STARTUP("startup");

    private final String fieldName;

    ConnectionLossFields(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }
}
