package org.epics.archiverappliance.mgmt.pva.actions;

/**
 * Exception for a generic error during the handling of a pvAccess RPC request
 * using {@link PvaAction}
 */
public class PvaActionException extends Exception {
    public PvaActionException(Exception e) {
        super(e);
    }
    public PvaActionException(Throwable e) {
        super(e);
    }
    public PvaActionException(String msg) {
        super(msg);
    }

    public PvaActionException(String msg, Exception ex) {
        super(msg, ex);
    }
    public PvaActionException(String msg, Throwable ex) {
        super(msg, ex);
    }
}
