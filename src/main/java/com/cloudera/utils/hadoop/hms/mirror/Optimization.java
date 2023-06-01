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
    private Boolean autoTune = Boolean.FALSE;
    private Boolean compressTextOutput = Boolean.FALSE;

    private Overrides overrides = new Overrides();
    private Boolean buildShadowStatistics = Boolean.FALSE;

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

    public Boolean getAutoTune() {
        return autoTune;
    }

    public void setAutoTune(Boolean autoTune) {
        this.autoTune = autoTune;
    }

    public Boolean getCompressTextOutput() {
        return compressTextOutput;
    }

    public void setCompressTextOutput(Boolean compressTextOutput) {
        this.compressTextOutput = compressTextOutput;
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

}
