package com.cloudera.utils.hadoop.hms.mirror;

public class Optimization {

    /*
    Control whether we'll set the 'hive.optimize.sort.dynamic.partition` conf to 'true' or not.  If this is not set,
    we'll use a PRESCRIPTIVE approach with the transfer SQL on partitioned tables by adding a DISTRIBUTE BY clause.
     */
    private Boolean sortDynamicPartitionInserts = Boolean.FALSE;
    /*
    Skip all optimizations by setting:
    - hive.optimize.sort.dynamic.partition=false
    - Not using DISTRIBUTE BY.
    - But do include additional settings specified by user in 'overrides'.
     */
    private Boolean skip = Boolean.FALSE;
    private Overrides overrides = new Overrides();
    private Boolean buildShadowStatistics = Boolean.FALSE;
//    private Boolean smallFiles = Boolean.FALSE;
//    private Integer tezGroupingMaxSizeMb = 128;

    public Boolean getSortDynamicPartitionInserts() {
        return sortDynamicPartitionInserts;
    }

    public void setSortDynamicPartitionInserts(Boolean sortDynamicPartitionInserts) {
        this.sortDynamicPartitionInserts = sortDynamicPartitionInserts;
    }

    public Boolean getSkip() {
        return skip;
    }

    public void setSkip(Boolean skip) {
        this.skip = skip;
    }


    public Boolean getBuildShadowStatistics() {
        return buildShadowStatistics;
    }

    public void setBuildShadowStatistics(Boolean buildShadowStatistics) {
        this.buildShadowStatistics = buildShadowStatistics;
    }

    public Overrides getOverrides() {
        return overrides;
    }

    public void setOverrides(Overrides overrides) {
        this.overrides = overrides;
    }

//    public Boolean getSmallFiles() {
//        return smallFiles;
//    }
//
//    public void setSmallFiles(Boolean smallFiles) {
//        this.smallFiles = smallFiles;
//    }
//
//    public Integer getTezGroupingMaxSizeMb() {
//        return tezGroupingMaxSizeMb;
//    }
//
//    public void setTezGroupingMaxSizeMb(Integer tezGroupingMaxSizeMb) {
//        this.tezGroupingMaxSizeMb = tezGroupingMaxSizeMb;
//    }
}
