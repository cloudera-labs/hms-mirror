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

import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hadoop.hms.mirror.MirrorConf;
import com.cloudera.utils.hadoop.hms.mirror.feature.IcebergState;
import com.cloudera.utils.hadoop.hms.util.FileFormatType;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to convert a Hive table to an Iceberg table.
 * Docs - https://docs.cloudera.com/cdp-public-cloud/cloud/iceberg-migrate-from-hive/topics/migrating-hive-to-iceberg.html
 * Iceberg Table Properties - https://iceberg.apache.org/docs/latest/configuration/#configuration
 * <p>
 * Hive Tables that are compatible with converting to Iceberg are:
 * - EXTERNAL
 * - File Types:
 * - ORC
 * - Parquet
 * - Avro
 */

public class IcebergConversionDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LogManager.getLogger(IcebergConversionDataStrategy.class);

    @Override
    public Boolean buildOutDefinition() {
        // Check definition to ensure it is compatible with Iceberg.
        // Alarm if not.
        // Alarm if already converted.
        LOG.debug("Table: " + dbMirror.getName() + " build Iceberg Conversions");

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        if (let == null) {
            LOG.error("Table is null for LEFT");
            return Boolean.FALSE;
        }

        IcebergState state = TableUtils.getIcebergConversionState(let);
        switch (state) {
            case NOT_CONVERTABLE:
                let.addIssue("Table is not compatible with Iceberg conversion.");
                return Boolean.FALSE;
            case CONVERTABLE:
                // Convert
                return Boolean.TRUE;
            case V1_FORMAT:
            case V2_FORMAT:
                let.addIssue("Table has already been converted.  Is currently " + state.toString());
                return Boolean.FALSE;
            default:
                return Boolean.FALSE;
        }
    }

    @Override
    public Boolean buildOutSql() {
        LOG.debug("Table: " + tableMirror.getName() + " buildout Iceberg Conversion SQL");
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        try {
            String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
            let.addSql(TableUtils.USE_DESC, useLeftDb);
            FileFormatType fileFormat = TableUtils.getFileFormatType(let.getDefinition());
            Map<String, String> tableProperties = new HashMap<String, String>();
            tableProperties.putAll(config.getIcebergConfig().getTableProperties());

            StringBuilder icebergProperties = new StringBuilder();
            if (fileFormat != FileFormatType.PARQUET) {
                tableProperties.put("write.format.default", fileFormat.toString().toLowerCase());
            }
            tableProperties.put("storage_handler", "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");

            if (config.getIcebergConfig().getVersion() == 2) {
                tableProperties.put("format-version", "2");
            }
            // Concatenate the table properties into a comma separated list.  Don't add the last comma
            // as it is not needed. Use stream to avoid the last comma.
            icebergProperties.append(tableProperties.entrySet().stream()
                    .map(entry -> "'" + entry.getKey() + "'='" + entry.getValue() + "'")
                    .reduce((a, b) -> a + "," + b).orElse(""));

            String convertToIceberg = MessageFormat.format(MirrorConf.CONVERT_TO_ICEBERG, let.getName(), icebergProperties.toString());
            String convertToIcebergDesc = MessageFormat.format(MirrorConf.CONVERT_TO_ICEBERG_DESC, config.getIcebergConfig().getVersion());
            let.addSql(convertToIcebergDesc, convertToIceberg);
            return Boolean.TRUE;
        } catch (Exception e) {
            LOG.error("Error converting table to Iceberg: " + e.getMessage());
            return Boolean.FALSE;
        }
    }

    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.FALSE;

        rtn = buildOutDefinition();//tblMirror.buildoutDUMPDefinition(config, dbMirror);
        if (rtn) {
            rtn = buildOutSql();//tblMirror.buildoutDUMPSql(config, dbMirror);
        }
        if (rtn) {
            config.getCluster(Environment.LEFT).runTableSql(tableMirror);
        }

        return rtn;
    }
}
