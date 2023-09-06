package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum;

public class StorageMigration {

    private DataStrategyEnum strategy = DataStrategyEnum.SQL;
    private Boolean distcp = Boolean.FALSE;
    private DistcpFlow dataFlow = DistcpFlow.PULL;

    public DataStrategyEnum getStrategy() {
        return strategy;
    }

    public void setStrategy(DataStrategyEnum strategy) {
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
