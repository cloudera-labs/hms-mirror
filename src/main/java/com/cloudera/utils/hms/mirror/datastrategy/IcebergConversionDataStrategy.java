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

package com.cloudera.utils.hms.mirror.datastrategy;

import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.TableMirror;
import com.cloudera.utils.hms.mirror.feature.IcebergState;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.util.FileFormatType;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

@Component
@Slf4j
public class IcebergConversionDataStrategy extends DataStrategyBase implements DataStrategy {

    @Getter
    private TableService tableService;

    public IcebergConversionDataStrategy(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        // Check definition to ensure it is compatible with Iceberg.
        // Alarm if not.
        // Alarm if already converted.
        log.debug("Table: " + tableMirror.getName() + " build Iceberg Conversions");

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        if (let == null) {
            log.error("Table is null for LEFT");
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
                let.addIssue("Table has already been converted.  Is currently " + state);
                return Boolean.FALSE;
            default:
                return Boolean.FALSE;
        }
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) {
        log.debug("Table: " + tableMirror.getName() + " buildout Iceberg Conversion SQL");
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        try {
            String useLeftDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getName());
            let.addSql(TableUtils.USE_DESC, useLeftDb);
            FileFormatType fileFormat = TableUtils.getFileFormatType(let.getDefinition());
            Map<String, String> tableProperties = new HashMap<String, String>();
            tableProperties.putAll(getConfigService().getConfig().getIcebergConfig().getTableProperties());

            StringBuilder icebergProperties = new StringBuilder();
            if (fileFormat != FileFormatType.PARQUET) {
                tableProperties.put("write.format.default", fileFormat.toString().toLowerCase());
            }
            tableProperties.put("storage_handler", "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");

            if (getConfigService().getConfig().getIcebergConfig().getVersion() == 2) {
                tableProperties.put("format-version", "2");
            }
            // Concatenate the table properties into a comma separated list.  Don't add the last comma
            // as it is not needed. Use stream to avoid the last comma.
            icebergProperties.append(tableProperties.entrySet().stream()
                    .map(entry -> "'" + entry.getKey() + "'='" + entry.getValue() + "'")
                    .reduce((a, b) -> a + "," + b).orElse(""));

            String convertToIceberg =
                    MessageFormat.format(MirrorConf.CONVERT_TO_ICEBERG, let.getName(), icebergProperties.toString());
            String convertToIcebergDesc =
                    MessageFormat.format(MirrorConf.CONVERT_TO_ICEBERG_DESC, getConfigService().getConfig().getIcebergConfig().getVersion());
            let.addSql(convertToIcebergDesc, convertToIceberg);
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error("Error converting table to Iceberg: " + e.getMessage());
            return Boolean.FALSE;
        }
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;

        rtn = buildOutDefinition(tableMirror);
        if (rtn) {
            rtn = buildOutSql(tableMirror);
        }
        if (rtn) {
            rtn = getTableService().runTableSql(tableMirror, Environment.LEFT);
        }

        return rtn;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }
}
