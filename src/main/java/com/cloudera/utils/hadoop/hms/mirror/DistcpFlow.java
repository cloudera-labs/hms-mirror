package com.cloudera.utils.hadoop.hms.mirror;

public enum DistcpFlow {

    /*
    Data is being 'pulled' from the source cluster by the target cluster (RIGHT 'pulls' from LEFT).

    This is typical in sidecar on-prem migrations, where the newer cluster is usually where the 'distcp' commands
    are run.
     */
    PULL,
    /*
    Data is 'pushed' to the target from the 'source' cluster.  LEFT pushes to target.  This is the typical pattern
    for on-prem to cloud migrations since the cloud environments can not (usually) access the on-prem environments.
     */
    PUSH,
    /*
    Data is 'pushed' from LEFT to transition area.  Then the 'data' is PULL from the 'transition' area by the RIGHT.
     */
    PUSH_PULL;
}
