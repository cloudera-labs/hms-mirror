package com.streever.hadoop.hms.mirror;

public class MetadataConfig {
    private int concurrency = 4;
    private String transferPrefix;
    private String exportBaseDirPrefix;

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

    public String getTransferPrefix() {
        if (strategy == Strategy.EXPORT_IMPORT) {
            if (transferPrefix == null) {
                return "transfer_";
            } else {
                return transferPrefix;
            }
        }
        return transferPrefix;
    }

    public void setTransferPrefix(String transferPrefix) {
        this.transferPrefix = transferPrefix;
    }

    public String getExportBaseDirPrefix() {
        if (strategy == Strategy.EXPORT_IMPORT) {
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

    @Override
    public String toString() {
        return "MetadataConfig{" +
                "concurrency=" + concurrency +
                ", strategy=" + strategy +
                '}';
    }
}
