package com.streever.hadoop.hms.mirror;

public class MetadataConfig {
    public enum Strategy {
        /*
        Using the schema will pull from the lower cluster, build the upper cluster
        schema and create then attach to LOWER data.
         */
        DIRECT,
        /*
        This method has issue in EMR.  Can't run "EXPORT" commands in Hive EMR. Use DIRECT for
        EMR Mirrors.

        Use a transition db to get a schema with no data attach via EXPORT.  Then import
        the shell schema in the upper cluster and attach to the LOWER data.
         */
        TRANSITION;
    }
    private int concurrency = 4;
    private Strategy strategy = Strategy.DIRECT;

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }
}
