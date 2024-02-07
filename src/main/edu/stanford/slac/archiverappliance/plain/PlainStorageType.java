package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.parquet.ParquetPlainFileHandler;
import edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler;

/**
 * Lists possible file extensions for this plugin.
 */
public enum PlainStorageType {
    PARQUET(new ParquetPlainFileHandler()),
    PB(new PBPlainFileHandler()),
    ;
    private final PlainFileHandler plainFileHandler;

    PlainStorageType(PlainFileHandler plainFileHandler) {
        this.plainFileHandler = plainFileHandler;
    }

    public PlainFileHandler plainFileHandler() {
        return this.plainFileHandler;
    }
}
