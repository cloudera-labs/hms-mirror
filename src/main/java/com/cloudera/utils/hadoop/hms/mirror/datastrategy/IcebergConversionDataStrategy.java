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

package com.cloudera.utils.hadoop.hms.mirror.datastrategy;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * This class is used to convert a Hive table to an Iceberg table.
 * Docs - https://docs.cloudera.com/cdp-public-cloud/cloud/iceberg-migrate-from-hive/topics/migrating-hive-to-iceberg.html
 *
 * Hive Tables that are compatible with converting to Iceberg are:
 *  - EXTERNAL
 *  - File Types:
 *      - ORC
 *      - Parquet
 *      - Avro
 *
 */
public class IcebergConversionDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LogManager.getLogger(IcebergConversionDataStrategy.class);
    @Override
    public Boolean execute() {
        return null;
    }

    @Override
    public Boolean buildOutDefinition() {
        return null;
    }

    @Override
    public Boolean buildOutSql() {
        return null;
    }
}
