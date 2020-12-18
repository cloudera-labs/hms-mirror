package com.streever.hadoop.hms.mirror;

public enum Strategy {


    // METADATA

    /*
    Using the schema will pull from the lower cluster, build the upper cluster
    schema and create then attach to LOWER data.
    */
    DIRECT,
    /*
    This method has issue in EMR.  Can't run "EXPORT" commands in Hive EMR. Use DIRECT for
    EMR Mirrors.

    Use a transition db to get a schema with no data attach via EXPORT.  Then import
    the shell schema in the upper cluster and attach to the LOWER data.
     */
    TRANSITION,


    // STORAGE
    /*
    Use Hive SQL to move data between Hive Tables.
    */
    SQL,
    /*
    Move data with EXPORT/IMPORT hive features.
     */
    EXPORT_IMPORT,
    /*
    With thresholds, determine which case is better: SQL or EXPORT_IMPORT for
    a table.
     */
    HYBRID,
    /*
    Will require manual intervention. Concept for this effort is still a WIP.
     */
    DISTCP;

}
