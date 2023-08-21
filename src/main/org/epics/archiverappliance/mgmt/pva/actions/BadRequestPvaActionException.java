package org.epics.archiverappliance.mgmt.pva.actions;

/**
 * Exception to return a bad request when a problem happens
 * handling a pvAccess request through a {@link PvaAction} interface
 */
public class BadRequestPvaActionException extends PvaActionException {
    public BadRequestPvaActionException(Exception e) {
        super(e);
    }

    public BadRequestPvaActionException(String msg, IllegalArgumentException ex) {
        super(msg, ex);
    }

    public BadRequestPvaActionException(String msg) {
        super(msg);
    }
}
