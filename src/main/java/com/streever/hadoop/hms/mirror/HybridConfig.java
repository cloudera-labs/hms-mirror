package com.streever.hadoop.hms.mirror;

public class HybridConfig {
    private int sqlPartitionLimit = 100;
    private long sqlSizeLimit = (1024 * 1024 * 1024); // 1Gb

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

}
