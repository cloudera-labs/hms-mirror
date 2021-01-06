package com.streever.hadoop.hms.mirror;

public class StorageConfig {

    private int concurrency = 4;
    private Strategy strategy = Strategy.HYBRID;
    private String transferPrefix;
    private String exportBaseDirPrefix;
    private Boolean migrateACID = Boolean.FALSE;

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

    public class Distcp {

        private PathStrategy pathStrategy = PathStrategy.DB;

        public PathStrategy getPathStrategy() {
            return pathStrategy;
        }

        public void setPathStrategy(PathStrategy pathStrategy) {
            this.pathStrategy = pathStrategy;
        }
    }

    private Hybrid hybrid = new Hybrid();
    private Distcp distcp;

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
        if (this.strategy == Strategy.DISTCP && this.distcp == null) {
            this.distcp = new Distcp();
        }
    }

    public void setTransferPrefix(String transferPrefix) {
        this.transferPrefix = transferPrefix;
    }

    public String getTransferPrefix() {
        if (strategy == Strategy.SQL || strategy == Strategy.HYBRID) {
            if (transferPrefix == null) {
                return "transfer_";
            } else {
                return transferPrefix;
            }
        }
        return transferPrefix;
    }

    public String getExportBaseDirPrefix() {
        if (strategy == Strategy.EXPORT_IMPORT || strategy == Strategy.HYBRID) {
            if (exportBaseDirPrefix == null) {
                return "/apps/hive/warehouse/export_";
            } else {
                return exportBaseDirPrefix;
            }
        }
        return exportBaseDirPrefix;
    }

    public void setExportBaseDirPrefix(String exportBaseDirPrefix) {
        this.exportBaseDirPrefix = exportBaseDirPrefix;
    }

    public Hybrid getHybrid() {
        return hybrid;
    }

    public void setHybrid(Hybrid hybrid) {
        this.hybrid = hybrid;
    }

    public Distcp getDistcp() {
        return distcp;
    }

    public void setDistcp(Distcp distcp) {
        this.distcp = distcp;
    }

    public Boolean getMigrateACID() {
        return migrateACID;
    }

    public void setMigrateACID(Boolean migrateACID) {
        this.migrateACID = migrateACID;
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
