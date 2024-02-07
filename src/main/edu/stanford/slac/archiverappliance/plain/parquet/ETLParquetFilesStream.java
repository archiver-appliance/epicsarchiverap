package edu.stanford.slac.archiverappliance.plain.parquet;

import org.epics.archiverappliance.etl.ETLBulkStream;

import java.nio.file.Path;
import java.util.List;

/**
 * ETL Parquet files stream, provides access to the list of parquet files for combination.
 */
public interface ETLParquetFilesStream extends ETLBulkStream {

    /**
     * Get parquet file paths
     *
     * @return List of paths to parquet files
     */
    List<Path> getPaths();

    /**
     * Get parquet first file info
     *
     * @return First parquet file info
     */
    ParquetInfo getFirstFileInfo();
}
