package com.streever.hadoop.hms.mirror;

public class StorageConfig {

    public enum Strategy {
        /*
        Use Hive SQL to move data between Hive Tables.
         */
        SQL,
        /*
        Move data with EXPORT/IMPORT hive features.
         */
        EXPORT_IMPORT,
        /*
        With thresholds, determine which case is better: SQL or EXPORT_IMPORT for
        a table.
         */
        HYBRID,
        /*
        Will require manual intervention. Concept for this effort is still a WIP.
         */
        DISTCP;
    }

    private int concurrency = 4;
    private StorageConfig.Strategy strategy = StorageConfig.Strategy.SQL;

    public class Hybrid {
        private int sqlPartitionLimit = 100;
        private long sqlSizeLimit = (1024*1024*1024); // 1Gb

        public int getSqlPartitionLimit() {
            return sqlPartitionLimit;
        }

        public void setSqlPartitionLimit(int sqlPartitionLimit) {
            this.sqlPartitionLimit = sqlPartitionLimit;
        }

        public long getSqlSizeLimit() {
            return sqlSizeLimit;
        }

        public void setSqlSizeLimit(long sqlSizeLimit) {
            this.sqlSizeLimit = sqlSizeLimit;
        }

        @Override
        public String toString() {
            return "Hybrid{" +
                    "sqlPartitionLimit=" + sqlPartitionLimit +
                    ", sqlSizeLimit=" + sqlSizeLimit +
                    '}';
        }
    }

    private Hybrid hybrid = new Hybrid();

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

    public Hybrid getHybrid() {
        return hybrid;
    }

    public void setHybrid(Hybrid hybrid) {
        this.hybrid = hybrid;
    }

    @Override
    public String toString() {
        return "StorageConfig{" +
                "concurrency=" + concurrency +
                ", strategy=" + strategy +
                ", hybrid=" + hybrid +
                '}';
    }
}
