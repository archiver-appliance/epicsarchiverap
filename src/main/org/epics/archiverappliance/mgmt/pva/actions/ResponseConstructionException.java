package org.epics.archiverappliance.mgmt.pva.actions;

/**
 * Exception for handling request in {@link PvaAction} when the reponse
 * cannot be constructed for whatever reason is thrown.
 */
public class ResponseConstructionException extends PvaActionException {
    public ResponseConstructionException(Exception e) {
        super("Response could not be constructed", e);
    }
    public ResponseConstructionException(Throwable e) {
        super("Response could not be constructed", e);
    }
    public ResponseConstructionException(String response, Exception e) {
        super("Failure to construct response based on " + response, e);
    }
}
