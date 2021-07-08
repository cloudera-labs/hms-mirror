package com.streever.hadoop.hms.mirror;

public class HybridConfig {
    private int exportImportPartitionLimit = 100;
    private int sqlPartitionLimit = 500;
    private long sqlSizeLimit = (1024 * 1024 * 1024); // 1Gb

    public int getExportImportPartitionLimit() {
        return exportImportPartitionLimit;
    }

    public void setExportImportPartitionLimit(int exportImportPartitionLimit) {
        this.exportImportPartitionLimit = exportImportPartitionLimit;
    }

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
