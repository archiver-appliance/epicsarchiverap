package edu.stanford.slac.archiverappliance.plain;

/**
 * Keys for use in the PlainStoragePlugin url.
 *
 *  @see #NAME
 */
public enum URLKeys {
    /**
     * name blah
     */
    NAME("name"),
    ROOT_FOLDER("rootFolder"),
    PARTITION_GRANULARITY("partitionGranularity"),
    HOLD("hold"),
    GATHER("gather"),
    COMPRESS("compress"),
    REDUCE_DATA("reducedata"),
    CONSOLIDATE_ON_SHUTDOWN("consolidateOnShutdown"),
    ETL_INTO_STORE_IF("etlIntoStoreIf"),
    ETL_OUT_STORE_IF("etlOutofStoreIf"),
    POST_PROCESSOR("pp"),
    /**
     * ZSTD buffer pool only used with @see #ZSTD
     * Values are true or false, default true.
     */
    ZSTD_BUFFER_POOL("zstdBufferPool"),
    /**
     * ZSTD Level only used with @see #ZSTD
     * Values are positive, range 1-22, default of 3.
     */
    ZSTD_LEVEL("zstdLevel"),
    /**
     * ZSTD workers only used with @see #ZSTD
     * Values are positive, default of 0.
     */
    ZSTD_WORKERS("zstdWorkers");

    @Override
    public String toString() {
        return key;
    }

    private final String key;

    URLKeys(String key) {
        this.key = key;
    }

    public String key() {
        return this.key;
    }
}
