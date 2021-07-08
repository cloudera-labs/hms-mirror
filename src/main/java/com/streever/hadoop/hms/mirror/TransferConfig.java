package com.streever.hadoop.hms.mirror;

public class TransferConfig {
    private int concurrency = 4;
    private String transferPrefix = "hms_mirror_transfer_";
    private String exportBaseDirPrefix = "/apps/hive/warehouse/export_";
    private String intermediateStorage = null;

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public String getTransferPrefix() {
        return transferPrefix;
    }

    public void setTransferPrefix(String transferPrefix) {
        this.transferPrefix = transferPrefix;
    }

    public String getExportBaseDirPrefix() {
        return exportBaseDirPrefix;
    }

    public void setExportBaseDirPrefix(String exportBaseDirPrefix) {
        this.exportBaseDirPrefix = exportBaseDirPrefix;
    }

    public String getIntermediateStorage() {
        return intermediateStorage;
    }

    public void setIntermediateStorage(String intermediateStorage) {
        this.intermediateStorage = intermediateStorage;
    }
}
