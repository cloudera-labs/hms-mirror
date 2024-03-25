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
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ExportCircularResolveService;
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

@Component
@Slf4j
public class ExportImportAcidDowngradeInPlaceDataStrategy extends DataStrategyBase implements DataStrategy {
    @Getter
    private ExportCircularResolveService exportCircularResolveService;
    @Getter
    private TableService tableService;


    public ExportImportAcidDowngradeInPlaceDataStrategy(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        /*
        rename original to archive
        export original table
        import as external to original tablename
        write cleanup sql to drop original_archive.
         */
        // Check Partition Limits before proceeding.
        rtn = getExportCircularResolveService().buildOutExportImportSql(tableMirror);
        if (rtn) {
            // Build cleanup Queries (drop archive table)
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            String cleanUpArchive = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
            let.addCleanUpSql(TableUtils.DROP_DESC, cleanUpArchive);

            // Check Partition Counts.
            if (let.getPartitioned() && let.getPartitions().size() > getConfigService().getConfig().getHybrid().getExportImportPartitionLimit()) {
                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the EXPORT_IMPORT " +
                        "partition limit (hybrid->exportImportPartitionLimit) of " +
                        getConfigService().getConfig().getHybrid().getExportImportPartitionLimit() +
                        ".  The queries will NOT be automatically run.");
                rtn = Boolean.FALSE;
            }
        }

        // run queries.
        if (rtn) {
            getTableService().runTableSql(tableMirror, Environment.LEFT);
        }

        return rtn;
    }

    @Autowired
    public void setExportCircularResolveService(ExportCircularResolveService exportCircularResolveService) {
        this.exportCircularResolveService = exportCircularResolveService;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }
}
