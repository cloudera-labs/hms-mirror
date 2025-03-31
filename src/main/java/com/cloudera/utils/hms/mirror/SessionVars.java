/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror;

public interface SessionVars {
    String TEZ_EXECUTION_DESC = "SET 'tez' as the execution engine";
    String SET_TEZ_AS_EXECUTION_ENGINE = "SET hive.execution.engine=tez";

    String SET_SESSION_VALUE_INT = "SET {0}={1,number,0}";
    String SET_SESSION_VALUE_STRING = "SET {0}={1}";

    String SORT_DYNAMIC_PARTITION = "hive.optimize.sort.dynamic.partition";
    String SORT_DYNAMIC_PARTITION_THRESHOLD = "hive.optimize.sort.dynamic.partition.threshold";
    String HIVE_AUTO_TABLE_STATS = "hive.stats.autogather";
    String HIVE_AUTO_COLUMN_STATS = "hive.stats.column.autogather";
    String LEGACY_DB_LOCATION_PROP = "hive.metastore.warehouse.dir";
    String EXT_DB_LOCATION_PROP = "hive.metastore.warehouse.external.dir";
    String MNGD_DB_LOCATION_PROP = "hive.metastore.warehouse.dir";
    String HIVE_COMPRESS_OUTPUT = "hive.exec.compress.output";
    String TEZ_GROUP_MAX_SIZE = "tez.grouping.max-size";
    String HIVE_MAX_DYNAMIC_PARTITIONS = "hive.exec.max.dynamic.partitions";
    String HIVE_MAX_REDUCERS = "hive.exec.reducers.max";
}
