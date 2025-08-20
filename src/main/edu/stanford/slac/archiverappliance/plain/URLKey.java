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
    ;

    private final String key;

    URLKey(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}
