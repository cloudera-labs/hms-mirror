package com.streever.hadoop.hms.mirror;

public enum Strategy {


    // METADATA

    /*
    Using the schema will pull from the lower cluster, build the upper cluster
    schema and create then attach to LEFT data.
    */
    DIRECT,

    // TODO: Provide an SCHEMA_EXTRACT process.
    /*
    Schema extract will attach to the LEFT cluster and extract out the current schema and build a separate
    SQL script for each database listed.
     */
    SCHEMA_EXTRACT,

    // STORAGE
    /*
    Use Hive SQL to move data between Hive Tables.
    */
    SQL,
    /*
    With thresholds, determine which case is better: SQL or EXPORT_IMPORT for
    a table.
     */
    HYBRID,
    /*
    Will require manual intervention. Concept for this effort is still a WIP.
     */


    // METADATA and STORAGE
    /*
    Move data with EXPORT/IMPORT hive features.

    FOR METADATA:
    This method has issue in EMR.  Can't run "EXPORT" commands in Hive EMR. Use DIRECT for
    EMR Mirrors.

    Use a transition db to get a schema with no data attach via EXPORT.  Then import
    the shell schema in the upper cluster and attach to the LEFT data.

     */
    EXPORT_IMPORT,
    /*
    For METADATA will move the METADATA only, expecting the data to be there already.
    For STORAGE this will generate a script of 'distcp' commands to run to move the data
         from the LEFT cluster to the RIGHT cluster.
    */
    DISTCP;

}
