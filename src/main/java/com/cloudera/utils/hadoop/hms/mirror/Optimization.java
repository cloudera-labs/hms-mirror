package com.cloudera.utils.hadoop.hms.mirror;

public class Optimization {

    private Boolean sortDynamicPartitionInserts = Boolean.TRUE;
    private Boolean buildShadowStatistics = Boolean.TRUE;
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
