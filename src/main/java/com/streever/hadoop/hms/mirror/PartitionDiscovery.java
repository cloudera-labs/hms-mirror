package com.streever.hadoop.hms.mirror;

public class PartitionDiscovery {

    /*
    Partition Discovery is NOT enable by default in most cluster.  On the Metastore Leader, the `PartitionManagementTask`
    will run when
     */
    private Boolean auto = Boolean.TRUE;
    /*
    Setting this will trigger an immediate msck on the table, which will affect performance of this job.  Consider
    using `auto`, to set the 'discovery'.  Make sure you activate and size the PartitionManagementTask process.
     */
    private Boolean initMSCK = Boolean.TRUE;

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

}
