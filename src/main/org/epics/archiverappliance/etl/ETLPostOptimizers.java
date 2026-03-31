package org.epics.archiverappliance.etl;

import java.io.IOException;
import java.util.Collection;

/*
 * An optional interface implemented by ETLDest's that indicate whether this storage plugin has need for post-ETL optimizations.
 * For example, a GZTar storage plugin may want to optimize the tar file after an ETL operation to remove any logically deleted entries.
 * The ETL framework will call the optimize method for all ETLOptimizable's after an ETL operation is complete for a PV.
 */
public interface ETLPostOptimizers {
    public Collection<ETLOptimizable> getPostETLOptimizables(String pvName, ETLContext context) throws IOException;
}
