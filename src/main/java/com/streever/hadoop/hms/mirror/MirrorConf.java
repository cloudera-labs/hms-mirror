package com.streever.hadoop.hms.mirror;

public class MirrorConf {
    public static final String DESCRIBE_DB = "DESCRIBE DATABASE EXTENDED {0}";

    public static final String SHOW_TABLES = "SHOW TABLES";
    public static final String SHOW_VIEWS = "SHOW VIEWS";
    public static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE {0}";
    public static final String SHOW_PARTITIONS = "SHOW PARTITIONS {0}.{1}";
    public static final String MSCK_REPAIR_TABLE = "MSCK REPAIR TABLE {0}";
    public static final String CREATE_DB =
            "CREATE DATABASE IF NOT EXISTS {0}";
    public static final String CREATE_LIKE =
            "CREATE TABLE IF NOT EXISTS {0} LIKE {1}";
    public static final String CREATE_EXTERNAL_LIKE =
            "CREATE EXTERNAL TABLE IF NOT EXISTS {0} LIKE {1}";
    public static final String USE = "USE {0}";
    public static final String DROP_TABLE = "DROP TABLE IF EXISTS {0}";
    public static final String DROP_VIEW = "DROP VIEW IF EXISTS {0}";
    public static final String RENAME_TABLE = " ALTER TABLE {0} RENAME TO {1}";
    public static final String EXPORT_TABLE =
            "EXPORT TABLE {0} TO \"{1}\"";
    public static final String IMPORT_EXTERNAL_TABLE =
            "IMPORT EXTERNAL TABLE {0} FROM \"{1}\"";
    public static final String IMPORT_TABLE =
            "IMPORT TABLE {0} FROM \"{1}\"";
    public static final String IMPORT_EXTERNAL_TABLE_LOCATION =
            "IMPORT EXTERNAL TABLE {0} FROM \"{1}\" LOCATION \"{2}\"";
    public static final String ADD_TBL_PROP =
            "ALTER TABLE {0}.{1} SET TBLPROPERTIES (\"{2}\"=\"{3}\")";
    public static final String REMOVE_TBL_PROP =
            "ALTER TABLE {0}.{1} UNSET TBLPROPERTIES (\"{2}\")";
    public static final String ALTER_TABLE_LOCATION =
            "ALTER TABLE {0}.{1} SET LOCATION \"{2}\"";
    public static final String HMS_MIRROR_LEGACY_MANAGED_FLAG = "hmsMirror_LegacyManaged";
    public static final String DISCOVER_PARTITIONS = "discover.partitions";
    public static final String EXTERNAL_TABLE_PURGE = "external.table.purge";
    public static final String TRANSACTIONAL = "transactional";
    public static final String AVRO_SCHEMA_URL_KEY = "avro.schema.url";
    public static final String TEZ_EXECUTION_DESC = "Set 'tez' as the execution engine";
    public static final String SET_TEZ_AS_EXECUTION_ENGINE = "set hive.execution.engine=tez";
    public static final String SQL_DATA_TRANSFER = "FROM {0} INSERT OVERWRITE TABLE {1} SELECT *";
    public static final String SQL_DATA_TRANSFER_WITH_PARTITIONS = "FROM {0} INSERT OVERWRITE TABLE {1} PARTITION ({2}) SELECT *";
    /*
    METADATA Transfer Flag
     */
    public static final String HMS_MIRROR_METADATA_FLAG = "hmsMirror_Metadata_Stage1";
    public static final String HMS_MIRROR_CONVERTED_FLAG = "hmsMirror_Converted";

    // Data Migration Flags
    /*
    Didn't move data (cloud storage scenario), but RIGHT cluster managed data flags
    converted to upper cluster AND reset/unset in lower cluster.
    */
    public static final String HMS_MIRROR_STORAGE_OWNER_FLAG = "hmsMirror_Storage_OWNER_Stage2";
    /*
    Migrate Metadata only and use a temp table in the RIGHT cluster with a reference to the data
    in the LEFT cluster and USE SQL to migrated the data from the temp table to a target table
    in the RIGHT cluster that matches the LEFT cluster relative location.
     */
    public static final String HMS_MIRROR_STORAGE_SQL_FLAG = "hmsMirror_Storage_SQL_Stage2";
    /*
    Using Hive EXPORT to build a transferrable package of the schema and data in the lower cluster.
    In the RIGHT cluster, with access to the LEFT cluster EXPORT location, IMPORT the table and data
    into the RIGHT cluster.
    Purge/Managed Adjustments: TBD
     */
    public static final String HMS_MIRROR_STORAGE_IMPORT_FLAG = "hmsMirror_Storage_IMPORT_Stage2";
    /*
    A mixed of SQL and IMPORT.  Using table characteristics like partition count and data sizes to
    determine whether to use SQL or EXPORT/IMPORT to move data.
    Purge/Managed Adjustments: TBD
     */
    public static final String HMS_MIRROR_STORAGE_HYBRID_FLAG = "hmsMirror_Storage_HYBRID_Stage2";
    /*
    Build the schema in the upper cluster via the Metadata Mirror process.  Then an 'external' process
    uses 'distcp' to migrate the data in the background.
    Requires EXTERNAL intervention.
     */
    public static final String HMS_MIRROR_STORAGE_DISTCP_FLAG = "hmsMirror_Storage_DISTCP_Stage2";

    public static final String LEGACY_DB_LOCATION_PROP = "hive.metastore.warehouse.dir";
    public static final String EXT_DB_LOCATION_PROP = "hive.metastore.warehouse.external.dir";
    public static final String MNGD_DB_LOCATION_PROP = "hive.metastore.warehouse.dir";

    public static final String DB_LOCATION = "LOCATION";
    public static final String DB_MANAGED_LOCATION = "MANAGEDLOCATION";
    public static final String COMMENT = "COMMENT";

}
