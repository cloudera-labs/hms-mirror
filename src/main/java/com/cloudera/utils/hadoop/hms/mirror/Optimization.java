package com.cloudera.utils.hadoop.hms.mirror;

public class Optimization {

    /*
    Control whether we'll set the 'hive.optimize.sort.dynamic.partition` conf to 'true' or not.  If this is not set,
    we'll use a PRESCRIPTIVE approach with the transfer SQL on partitioned tables by adding a DISTRIBUTE BY clause.
     */
    private Boolean sortDynamicPartitionInserts = Boolean.FALSE;
    private Boolean buildShadowStatistics = Boolean.FALSE;
    private Boolean smallFiles = Boolean.FALSE;
    private Integer tezGroupingMaxSizeMb = 128;

    public Boolean getSortDynamicPartitionInserts() {
        return sortDynamicPartitionInserts;
    }

    public void setSortDynamicPartitionInserts(Boolean sortDynamicPartitionInserts) {
        this.sortDynamicPartitionInserts = sortDynamicPartitionInserts;
    }

    public Boolean getBuildShadowStatistics() {
        return buildShadowStatistics;
    }

    public void setBuildShadowStatistics(Boolean buildShadowStatistics) {
        this.buildShadowStatistics = buildShadowStatistics;
    }

    public Boolean getSmallFiles() {
        return smallFiles;
    }

    public void setSmallFiles(Boolean smallFiles) {
        this.smallFiles = smallFiles;
    }

    public Integer getTezGroupingMaxSizeMb() {
        return tezGroupingMaxSizeMb;
    }

    public void setTezGroupingMaxSizeMb(Integer tezGroupingMaxSizeMb) {
        this.tezGroupingMaxSizeMb = tezGroupingMaxSizeMb;
    }
}
