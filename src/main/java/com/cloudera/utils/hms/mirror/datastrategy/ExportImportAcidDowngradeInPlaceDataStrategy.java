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

import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

@Component
@Slf4j
@Getter
public class ExportImportAcidDowngradeInPlaceDataStrategy extends DataStrategyBase {

    private final ExportCircularResolveService exportCircularResolveService;
    private final TableService tableService;


    public ExportImportAcidDowngradeInPlaceDataStrategy(StatsCalculatorService statsCalculatorService,
                                                        ExecuteSessionService executeSessionService,
                                                        TranslatorService translatorService,
                                                        ExportCircularResolveService exportCircularResolveService,
                                                        TableService tableService) {
        super(statsCalculatorService, executeSessionService, translatorService);
        this.exportCircularResolveService = exportCircularResolveService;
        this.tableService = tableService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        return null;
    }

    @Override
    public Boolean build(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        /*
        rename original to archive
        export original table
        import as external to original tablename
        write cleanup sql to drop original_archive.
         */
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        // Check Partition Limits before proceeding.
        try {
            rtn = getExportCircularResolveService().buildOutExportImportSql(tableMirror);
        } catch (MissingDataPointException e) {
            let.addError("Failed to build out SQL: " + e.getMessage());
            rtn = Boolean.FALSE;
        }
        if (rtn) {
            // Build cleanup Queries (drop archive table)
            String cleanUpArchive = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
            let.addCleanUpSql(TableUtils.DROP_DESC, cleanUpArchive);

            // Check Partition Counts.
            if (let.getPartitioned() && let.getPartitions().size() > hmsMirrorConfig.getHybrid().getExportImportPartitionLimit()) {
                let.addError("The number of partitions: " + let.getPartitions().size() + " exceeds the EXPORT_IMPORT " +
                        "partition limit (hybrid->exportImportPartitionLimit) of " +
                        hmsMirrorConfig.getHybrid().getExportImportPartitionLimit() +
                        ".  The queries will NOT be automatically run.");
                rtn = Boolean.FALSE;
            }
        }

        // run queries.
//        if (rtn) {
//            getTableService().runTableSql(tableMirror, Environment.LEFT);
//        }

        return rtn;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        return getTableService().runTableSql(tableMirror, Environment.LEFT);
    }

}
