package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.parquet.ParquetPlainFileHandler;
import edu.stanford.slac.archiverappliance.plain.pb.PBPlainFileHandler;

/**
 * Lists possible file extensions for this plugin.
 */
public enum PlainStorageType {
    PARQUET,
    PB,
    ;

    public PlainFileHandler plainFileHandler() {
        return switch (this) {
            case PB -> new PBPlainFileHandler();
            case PARQUET -> new ParquetPlainFileHandler();
        };
    }
}
