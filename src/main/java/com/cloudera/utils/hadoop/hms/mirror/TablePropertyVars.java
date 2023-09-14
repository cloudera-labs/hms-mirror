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

public interface TablePropertyVars {

    /*
METADATA Transfer Flag
 */
    String HMS_MIRROR_METADATA_FLAG = "hms-mirror_Metadata_Stage1";
    String HMS_MIRROR_CONVERTED_FLAG = "hms-mirror_Converted";
    // Data Migration Flags
    /*
    Didn't move data (cloud storage scenario), but RIGHT cluster managed data flags
    converted to upper cluster AND reset/unset in lower cluster.
    */
    String HMS_MIRROR_STORAGE_OWNER_FLAG = "hms-mirror_Storage_OWNER_Stage2";
    /*
    Migrate Metadata only and use a temp table in the RIGHT cluster with a reference to the data
    in the LEFT cluster and USE SQL to migrated the data from the temp table to a target table
    in the RIGHT cluster that matches the LEFT cluster relative location.
     */
    String HMS_MIRROR_STORAGE_SQL_FLAG = "hms-mirror_Storage_SQL_Stage2";
    /*
    Using Hive EXPORT to build a transferrable package of the schema and data in the lower cluster.
    In the RIGHT cluster, with access to the LEFT cluster EXPORT location, IMPORT the table and data
    into the RIGHT cluster.
    Purge/Managed Adjustments: TBD
     */
    String HMS_MIRROR_STORAGE_IMPORT_FLAG = "hms-mirror_Storage_IMPORT_Stage2";
    /*
    A mixed of SQL and IMPORT.  Using table characteristics like partition count and data sizes to
    determine whether to use SQL or EXPORT/IMPORT to move data.
    Purge/Managed Adjustments: TBD
     */
    String HMS_MIRROR_STORAGE_HYBRID_FLAG = "hms-mirror_Storage_HYBRID_Stage2";
    /*
    Build the schema in the upper cluster via the Metadata Mirror process.  Then an 'external' process
    uses 'distcp' to migrate the data in the background.
    Requires EXTERNAL intervention.
     */
    String HMS_MIRROR_STORAGE_DISTCP_FLAG = "hms-mirror_Storage_DISTCP_Stage2";
    String HMS_MIRROR_LEGACY_MANAGED_FLAG = "hms-mirror_LegacyManaged";
    String DOWNGRADED_FROM_ACID = "downgraded_from_acid";
    String DISCOVER_PARTITIONS = "discover.partitions";
    String TRANSLATED_TO_EXTERNAL = "TRANSLATED_TO_EXTERNAL";
    String EXTERNAL_TABLE_PURGE = "external.table.purge";
    String TRANSACTIONAL = "transactional";
    String TRANSACTIONAL_PROPERTIES = "transactional_properties";
    String HMS_MIRROR_TRANSFER_TABLE = "hms-mirror_transfer_table";
    String HMS_MIRROR_SHADOW_TABLE = "hms-mirror_shadow_table";
    String HMS_STORAGE_MIGRATION_FLAG = "hms-mirror-STORAGE_MIGRATED";
    String BUCKETING_VERSION = "bucketing_version";
    String AVRO_SCHEMA_URL_KEY = "avro.schema.url";

}
