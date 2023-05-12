package com.cloudera.utils.hadoop.hms.mirror;

import java.util.Map;
import java.util.TreeMap;

public class StorageMigration {

    private DataStrategy strategy = DataStrategy.SQL;
    private Boolean distcp = Boolean.FALSE;
    private DistcpFlow dataFlow = DistcpFlow.PULL;

    public DataStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(DataStrategy strategy) {
        switch (strategy) {
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
                this.strategy = strategy;
                break;
            default:
                throw new RuntimeException("Invalid strategy for STORAGE_MIGRATION");
        }
    }

    public DistcpFlow getDataFlow() {
        return dataFlow;
    }

    public void setDataFlow(DistcpFlow dataFlow) {
        this.dataFlow = dataFlow;
    }

    public Boolean isDistcp() {
        return distcp;
    }

    public void setDistcp(Boolean distcp) {
        this.distcp = distcp;
    }
}
