package com.streever.hadoop.hms.mirror;

public class MirrorConf {
    public static final String USE_DB = "USE {0}";
    public static final String SHOW_TABLES = "SHOW TABLES";
    public static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE {0}.{1}";
    public static final String SHOW_PARTITIONS = "SHOW PARTITIONS {0}.{1}";
    public static final String MSCK_REPAIR_TABLE = "MSCK REPAIR TABLE {0}.{1}";
    public static final String CREATE_DB =
            "CREATE DATABASE IF NOT EXISTS {0}";
    public static final String CREATE_LIKE =
            "CREATE TABLE IF NOT EXISTS {0}.{1} LIKE {2}.{3}";
    public static final String CREATE_EXTERNAL_LIKE =
            "CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1} LIKE {2}.{3}";
    public static final String USE = "USE {0}";
    public static final String DROP_TABLE = "DROP TABLE IF EXISTS {0}.{1}";
    public static final String EXPORT_TABLE =
            "EXPORT TABLE {0}.{1} TO \"{2}\"";
    public static final String IMPORT_EXTERNAL_TABLE =
            "IMPORT EXTERNAL TABLE {0}.{1} FROM \"{2}\"";
    public static final String IMPORT_TABLE =
            "IMPORT TABLE {0}.{1} FROM \"{2}\"";
    public static final String ADD_TBL_PROP =
            "ALTER TABLE {0}.{1} SET TBLPROPERTIES (\"{2}\"=\"{3}\")";
    public static final String REMOVE_TBL_PROP =
            "ALTER TABLE {0}.{1} UNSET TBLPROPERTIES (\"{2}\")";
    public static final String ALTER_TABLE_LOCATION =
            "ALTER TABLE {0}.{1} SET LOCATION \"{2}\"";
    public static final String LEGACY_MANAGED_FLAG = "hiveMirror_LegacyManaged";
    public static final String HMS_MIRROR_STAGE_ONE_FLAG = "hiveMirror_ConversionStage1";

}
