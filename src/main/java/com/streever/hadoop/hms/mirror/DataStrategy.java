package com.streever.hadoop.hms.mirror;

public enum DataStrategy {
    /*
    The DUMP strategy will run against the LEFT cluster (attaching to it) and build scripts
    that can be applied to the RIGHT cluster, without having to connect to the RIGHT cluster.

    All other attributes of the RIGHT cluster will be considered in the conversion, we just won't
    connect.  Check the output sql for the scripts to run.
     */
    DUMP,
    /*
    This will transfer the schema only, replace the location with the RIGHT
    clusters location namespace and maintain the relative path.
    The data is transferred by an external process, like 'distcp'.
     */
    SCHEMA_ONLY,
    /*
    Assumes the clusters are LINKED.  We'll transfer the schema and leave
    the location as is on the new cluster.  This provides a means to test
    Hive on the RIGHT cluster using the LEFT clusters storage.
     */
    LINKED,
    /*
    Assumes the clusters are LINKED.  We'll use SQL to migrate the data
    from one cluster to another.
     */
    SQL,
    /*
    Assumes the clusters are LINKED.  We'll use EXPORT_IMPORT to get the
    data to the new cluster.  EXPORT to a location on the LEFT cluster
    where the RIGHT cluster can pick it up with IMPORT.
     */
    EXPORT_IMPORT,
    /*
    Hybrid is a strategy that select either SQL or EXPORT_IMPORT for the
    tables data strategy depending on the criteria of the table.
     */
    HYBRID,
    /*
    The data migration with the schema will go through an intermediate
    storage location.  The process will EXPORT data on the LEFT cluster
    to the intermediate location with IMPORT from the RIGHT cluster.
     */
    INTERMEDIATE,
    /*
    The data storage is shared between the two clusters and no data
    migration is required.  Schema's are transferred using the same
    location.  Commit/ownership?
     */
    COMMON
}
