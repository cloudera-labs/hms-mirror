/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hadoop.hms.mirror;

public interface MirrorConf {
    String DESCRIBE_DB = "DESCRIBE DATABASE EXTENDED {0}";
    String SHOW_DATABASES = "SHOW DATABASES";
    String SHOW_TABLES = "SHOW TABLES";
    String SHOW_VIEWS = "SHOW VIEWS";
    String SHOW_TABLE_EXTENDED_WITH_PARTITION = "SHOW TABLE EXTENDED LIKE {0} PARTITION ({1})";
    String SHOW_TABLE_EXTENDED = "SHOW TABLE EXTENDED LIKE {0}";
    String SHOW_CREATE_TABLE = "SHOW CREATE TABLE {0}";
    String DESCRIBE_FORMATTED_TABLE = "DESCRIBE FORMATTED {0}";
    String SHOW_PARTITIONS = "SHOW PARTITIONS {0}.{1}";
    String MSCK_REPAIR_TABLE = "MSCK REPAIR TABLE {0}";
    String MSCK_REPAIR_TABLE_DESC = "MSCK Repair Table";
    String CREATE_DB =
            "CREATE DATABASE IF NOT EXISTS {0}";
    String CREATE_DB_DESC = "Create Database";
    String CREATE_TRANSFER_DB_DESC = "Create Transfer Database";
    String DROP_DB =
            "DROP DATABASE IF EXISTS {0} CASCADE";
    String DROP_DB_DESC = "Drop Database";
    String ALTER_DB_LOCATION =
            "ALTER DATABASE {0} SET LOCATION \"{1}\"";
    String ALTER_DB_LOCATION_DESC =
            "Alter Database Location";
    String DEFAULT_MANAGED_BASE_DIR = "/warehouse/tablespace/managed/hive";
    String ALTER_DB_MNGD_LOCATION =
            "ALTER DATABASE {0} SET MANAGEDLOCATION \"{1}\"";
    String ALTER_DB_MNGD_LOCATION_DESC =
            "Alter Database Managed Location";
    String ALTER_TABLE_PARTITION_ADD_LOCATION_DESC = "Alter Table Partition Add Location";
    String ALTER_TABLE_PARTITION_ADD_LOCATION =
            "ALTER TABLE {0} ADD IF NOT EXISTS\n{1}";
    String CREATE_LIKE =
            "CREATE TABLE IF NOT EXISTS {0} LIKE {1}";
    String CREATE_EXTERNAL_LIKE =
            "CREATE EXTERNAL TABLE IF NOT EXISTS {0} LIKE {1}";
    String USE = "USE {0}";
    String USE_DESC = "Set Database";
    String DROP_TABLE = "DROP TABLE IF EXISTS {0}";
    String DROP_TABLE_DESC = "Drop table";
    String DROP_VIEW = "DROP VIEW IF EXISTS {0}";
    String RENAME_TABLE_DESC = "Rename table";
    String RENAME_TABLE = "ALTER TABLE {0} RENAME TO {1}";
    String SET_OWNER_DESC = "Set table owner";
    String SET_OWNER = "ALTER TABLE {0} SET OWNER USER {1}";
    String EXPORT_TABLE =
            "EXPORT TABLE {0} TO \"{1}\"";
    String IMPORT_EXTERNAL_TABLE =
            "IMPORT EXTERNAL TABLE {0} FROM \"{1}\"";
    String IMPORT_TABLE =
            "IMPORT TABLE {0} FROM \"{1}\"";
    String IMPORT_EXTERNAL_TABLE_LOCATION =
            "IMPORT EXTERNAL TABLE {0} FROM \"{1}\" LOCATION \"{2}\"";
    String ADD_TABLE_PROP_DESC =
            "Add/Update Table Property";
    String ADD_TABLE_PROP =
            "ALTER TABLE {0} SET TBLPROPERTIES (\"{1}\"=\"{2}\")";
    String REMOVE_TABLE_PROP =
            "ALTER TABLE {0} UNSET TBLPROPERTIES (\"{1}\")";
    String REMOVE_TABLE_PROP_DESC =
            "Remove table property";
    String ALTER_TABLE_LOCATION =
            "ALTER TABLE {0} SET LOCATION \"{1}\"";
    String ALTER_TABLE_LOCATION_DESC =
            "Alter Table Location";

    String ALTER_TABLE_PARTITION_LOCATION_DESC = "Alter Table Partition Spec {0} Location ";
    String ALTER_TABLE_PARTITION_LOCATION =
            "ALTER TABLE {0} PARTITION ({1}) SET LOCATION \"{2}\"";

    String ARCHIVE = "archive";
    String SQL_DATA_TRANSFER = "FROM {0} INSERT INTO TABLE {1} SELECT *";
    String SQL_DATA_TRANSFER_OVERWRITE = "FROM {0} INSERT OVERWRITE TABLE {1} SELECT *";
    String SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE = "FROM {0} INSERT OVERWRITE TABLE {1} PARTITION ({2}) SELECT * DISTRIBUTE BY {3}";
    String SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE = "FROM {0} INSERT OVERWRITE TABLE {1} PARTITION ({2}) SELECT * ";

    String DB_LOCATION = "LOCATION";
    String DB_MANAGED_LOCATION = "MANAGEDLOCATION";
    String COMMENT = "COMMENT";
    String FILE_FORMAT = "file.format";
    String FILE_COUNT = "file.count";
    String DIR_COUNT = "dir.count";
    String DATA_SIZE = "data.size";
    String AVG_FILE_SIZE = "avg.file.size";
    String TABLE_EMPTY = "table.empty";
    String PARTITION_COUNT = "partition.count";
    String NOT_SET = "NOT_SET";

//    String CONVERT_TO_ICEBERG_V1 = "ALTER TABLE {0} SET TBLPROPERTIES ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler')";
    String CONVERT_TO_ICEBERG = "ALTER TABLE {0} SET TBLPROPERTIES ({1})";
    String CONVERT_TO_ICEBERG_DESC = "Convert to Iceberg table format v{0}.";
}
