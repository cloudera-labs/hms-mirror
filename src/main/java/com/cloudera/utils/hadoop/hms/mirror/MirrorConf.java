/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.mirror;

public class MirrorConf {
    public static final String DESCRIBE_DB = "DESCRIBE DATABASE EXTENDED {0}";

    public static final String SHOW_TABLES = "SHOW TABLES";
    public static final String SHOW_VIEWS = "SHOW VIEWS";
    public static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE {0}";
    public static final String SHOW_PARTITIONS = "SHOW PARTITIONS {0}.{1}";
    public static final String MSCK_REPAIR_TABLE = "MSCK REPAIR TABLE {0}";
    public static final String MSCK_REPAIR_TABLE_DESC = "MSCK Repair Table";
    public static final String CREATE_DB =
            "CREATE DATABASE IF NOT EXISTS {0}";
    public static final String CREATE_DB_DESC = "Create Database";
    public static final String CREATE_TRANSFER_DB_DESC = "Create Transfer Database";
    public static final String DROP_DB =
            "DROP DATABASE IF EXISTS {0} CASCADE";
    public static final String DROP_DB_DESC = "Drop Database";
    public static final String ALTER_DB_LOCATION =
            "ALTER DATABASE {0} SET LOCATION \"{1}\"";
    public static final String ALTER_DB_LOCATION_DESC =
            "Alter Database Location";
    public static final String ALTER_DB_MNGD_LOCATION =
            "ALTER DATABASE {0} SET MANAGEDLOCATION \"{1}\"";
    public static final String ALTER_DB_MNGD_LOCATION_DESC =
            "Alter Database Managed Location";

    public static final String ANALYZE_TABLE_STATS = "";
    public static final String ANALYZE_COLUMN_STATS = "";

    public static final String CREATE_LIKE =
            "CREATE TABLE IF NOT EXISTS {0} LIKE {1}";
    public static final String CREATE_EXTERNAL_LIKE =
            "CREATE EXTERNAL TABLE IF NOT EXISTS {0} LIKE {1}";
    public static final String USE = "USE {0}";
    public static final String USE_DESC = "Set Database";
    public static final String DROP_TABLE = "DROP TABLE IF EXISTS {0}";
    public static final String DROP_TABLE_DESC = "Drop table";
    public static final String DROP_VIEW = "DROP VIEW IF EXISTS {0}";
    public static final String RENAME_TABLE_DESC = "Rename table";
    public static final String RENAME_TABLE = " ALTER TABLE {0} RENAME TO {1}";
    public static final String EXPORT_TABLE =
            "EXPORT TABLE {0} TO \"{1}\"";
    public static final String IMPORT_EXTERNAL_TABLE =
            "IMPORT EXTERNAL TABLE {0} FROM \"{1}\"";
    public static final String IMPORT_TABLE =
            "IMPORT TABLE {0} FROM \"{1}\"";
    public static final String IMPORT_EXTERNAL_TABLE_LOCATION =
            "IMPORT EXTERNAL TABLE {0} FROM \"{1}\" LOCATION \"{2}\"";
    public static final String ADD_TABLE_PROP_DESC =
            "Add/Update Table Property";
    public static final String ADD_TABLE_PROP =
            "ALTER TABLE {0} SET TBLPROPERTIES (\"{1}\"=\"{2}\")";
    public static final String REMOVE_TABLE_PROP =
            "ALTER TABLE {0} UNSET TBLPROPERTIES (\"{1}\")";
    public static final String REMOVE_TABLE_PROP_DESC =
            "Remove table property";
    public static final String ALTER_TABLE_LOCATION =
            "ALTER TABLE {0} SET LOCATION \"{1}\"";
    public static final String ALTER_TABLE_LOCATION_DESC =
            "Alter Table Location";
    public static final String ARCHIVE = "archive";
    public static final String HMS_MIRROR_LEGACY_MANAGED_FLAG = "hmsMirror_LegacyManaged";
    public static final String DOWNGRADED_FROM_ACID = "downgraded_from_acid";
    public static final String DISCOVER_PARTITIONS = "discover.partitions";
    public static final String EXTERNAL_TABLE_PURGE = "external.table.purge";
    public static final String TRANSACTIONAL = "transactional";
    public static final String TRANSACTIONAL_PROPERTIES = "transactional_properties";
    public static final String HMS_MIRROR_TRANSFER_TABLE = "hms-mirror_transfer_table";
    public static final String HMS_MIRROR_SHADOW_TABLE = "hms-mirror_shadow_table";
    public static final String BUCKETING_VERSION = "bucketing_version";
    public static final String AVRO_SCHEMA_URL_KEY = "avro.schema.url";
    public static final String TEZ_EXECUTION_DESC = "Set 'tez' as the execution engine";
    public static final String SET_TEZ_AS_EXECUTION_ENGINE = "set hive.execution.engine=tez";
    public static final String SQL_DATA_TRANSFER = "FROM {0} INSERT INTO TABLE {1} SELECT *";
    public static final String SQL_DATA_TRANSFER_OVERWRITE = "FROM {0} INSERT OVERWRITE TABLE {1} SELECT *";
    public static final String SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE = "FROM {0} INSERT OVERWRITE TABLE {1} PARTITION ({2}) SELECT * DISTRIBUTE BY {2}";
    public static final String SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE = "FROM {0} INSERT OVERWRITE TABLE {1} PARTITION ({2}) SELECT * ";
    public static final String SORT_DYNAMIC_PARTITION = "hive.optimize.sort.dynamic.partition";
    public static final String SORT_DYNAMIC_PARTITION_THRESHOLD = "hive.optimize.sort.dynamic.partition.threshold";

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
