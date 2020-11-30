package com.streever.hadoop.hms.mirror;

public class PartitionDiscovery {
    // "discover.partitions"="true" won't be available till Hive 4.0?
    private Boolean auto = Boolean.TRUE;
    private Boolean initMSCK = Boolean.TRUE;
//    private PartitionDiscoveryStrategy defaultStrategy = PartitionDiscoveryStrategy.TRANSLATE;
//    private Integer limit = null;
//    private PartitionDiscoveryStrategy limitStrategy = null;

    public Boolean getAuto() {
        return auto;
    }

    public void setAuto(Boolean auto) {
        this.auto = auto;
    }

    public Boolean getInitMSCK() {
        return initMSCK;
    }

    public void setInitMSCK(Boolean initMSCK) {
        this.initMSCK = initMSCK;
    }

    //    public PartitionDiscoveryStrategy getDefaultStrategy() {
//        return defaultStrategy;
//    }
//
//    public void setDefaultStrategy(PartitionDiscoveryStrategy defaultStrategy) {
//        this.defaultStrategy = defaultStrategy;
//    }
//
//    public Integer getLimit() {
//        return limit;
//    }
//
//    public void setLimit(Integer limit) {
//        this.limit = limit;
//    }
//
//    public PartitionDiscoveryStrategy getLimitStrategy() {
//        return limitStrategy;
//    }
//
//    public void setLimitStrategy(PartitionDiscoveryStrategy limitStrategy) {
//        this.limitStrategy = limitStrategy;
//    }
}
