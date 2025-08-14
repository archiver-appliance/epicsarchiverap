package edu.stanford.slac.archiverappliance.plain;

public enum URLKey {
    NAME("name"),
    ROOT_FOLDER("rootFolder"),
    HOLD("hold"),
    GATHER("gather"),
    PARTITION_GRANULARITY("partitionGranularity"),
    COMPRESS("compress"),
    ETL_INTO_STORE_IF("etlIntoStoreIf"),
    ETL_OUT_OF_STORE_IF("etlOutofStoreIf"),
    REDUCE("reducedata"),
    CONSOLIDATE_ON_SHUTDOWN("consolidateOnShutdown"),
    POST_PROCESSORS("pp"),
    TERMINATOR("terminator"),
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

    private final String key;

    URLKey(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}
