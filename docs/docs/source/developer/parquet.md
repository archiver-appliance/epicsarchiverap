# Apache Parquet Backend

The EPICS Archiver Appliance supports [Apache Parquet](https://parquet.apache.org/) as an alternative storage backend to the default Protocol Buffers (PB) format. Parquet is a columnar storage file format available to any project in the Hadoop ecosystem, regardless of the choice of data processing framework, data model, or programming language.

## Benefits of Parquet

Using Parquet as a backend offers several advantages:

1.  **Efficiency**: Parquet's columnar format allows for better compression and encoding, often resulting in smaller file sizes compared to the row-based PB format.
2.  **Predicate Pushdown**: Parquet supports predicate pushdown, allowing the archiver to skip reading irrelevant data based on time filters, which can significantly improve retrieval performance for large datasets.
3.  **Interoperability**: Parquet files can be easily read by many data analysis tools and frameworks (e.g., Apache Spark, Pandas, Dask, DuckDB, Apache Arrow) without needing specialized archiver libraries.
4.  **Schema Evolution**: While the archiver uses a fixed schema for EPICS events, Parquet's support for nested data types and schema evolution provides a robust foundation for future extensions.

## Implementation Details

The Parquet backend is implemented as a
[PlainFileHandler](../_static/javadoc/edu/stanford/slac/archiverappliance/plain/PlainFileHandler.html){.external}
within the
[PlainStoragePlugin](../_static/javadoc/edu/stanford/slac/archiverappliance/plain/PlainStoragePlugin.html){.external}. It uses the same partitioning logic as the PB backend (e.g., hourly, daily, or yearly partitions).

### Schema

The schema used in Parquet files is derived from the Protocol Buffers definitions in `EPICSEvent.proto`. This ensures consistency between the PB and Parquet backends.

### Compression

Parquet supports various compression codecs. The archiver appliance specifically supports:

- **UNCOMPRESSED**: No compression.
- **SNAPPY**: High speed, reasonable compression.
- **GZIP**: High compression, lower speed.
- **LZO**: High speed, reasonable compression.
- **BROTLI**: High compression, lower speed.
- **LZ4**: High speed, reasonable compression.
- **ZSTD**: Excellent balance between compression ratio and speed.

#### ZSTD Configuration

When using ZSTD compression, several advanced configuration options are available via the storage plugin URL:

- `zstdBufferPool` (boolean): Enables the use of a buffer pool for ZSTD compression/decompression. Defaults to `false`.
- `zstdLevel` (integer): Sets the ZSTD compression level (typically 1-22). Defaults to `3`.
- `zstdWorkers` (integer): Sets the number of worker threads for ZSTD. Defaults to `0` (single-threaded).

## Configuration

To use the Parquet backend, use the `parquet:` scheme in your `dataStores` definition within `policies.py`.

Example:

```python
"dataStores": [
    "pb://localhost?name=STS&rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}&partitionGranularity=PARTITION_HOUR",
    "parquet://localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&partitionGranularity=PARTITION_DAY&compress=ZSTD&zstdLevel=0",
    "parquet://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR&compress=ZSTD&zstdLevel=5"
]
```

## Conversion

The archiver appliance includes a `ConvertFile` utility that can be used to convert existing PB files to Parquet or vice-versa.

```bash
# Convert a PB file to Parquet with ZSTD compression
java -cp ... edu.stanford.slac.archiverappliance.plain.utils.ConvertFile /data/pv.pb PARQUET compress=ZSTD zstdLevel=3
```
