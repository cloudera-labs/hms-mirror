package com.streever.hadoop.hms.mirror;

public enum Stage {
    /*
    Move the Metadata from Cluster A to Cluster B, while maintaining the same location reference.  For HDFS storage locations this means the 'target' cluster will need access to the 'source' clusters HDFS location.

    For Cloud storage scenarios, the storage is shared anyhow.  So the 'target' cluster only needs to need credentials to access the common/share storage location.
     */
    METADATA,
    /*
    Move the data with the metadata from Cluster A to Cluster B.
     */
    STORAGE,
    /*
    Use this to convert a table from transactional to non-transactional or non-transactional to transactional.
     */
    CONVERT;
}
