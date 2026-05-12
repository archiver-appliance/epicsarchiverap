package org.epics.archiverappliance.etl;

import java.io.IOException;

/*
 * Typically implemented by the actual NIO2 FileSystem implementations used by ETL storage plugins that need post-ETL optimizations.
 */
public interface ETLOptimizable {
    public boolean optimize() throws IOException;
}
