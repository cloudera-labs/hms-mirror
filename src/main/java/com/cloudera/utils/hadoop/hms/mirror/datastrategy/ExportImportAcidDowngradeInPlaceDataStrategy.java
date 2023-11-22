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
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

public class ExportImportAcidDowngradeInPlaceDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ExportImportAcidDowngradeInPlaceDataStrategy.class);

    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.TRUE;
        /*
        rename original to archive
        export original table
        import as external to original tablename
        write cleanup sql to drop original_archive.
         */
        // Check Partition Limits before proceeding.
        DataStrategy dsEI = DataStrategyEnum.EXPORT_IMPORT.getDataStrategy();
        dsEI.setTableMirror(tableMirror);
        dsEI.setDBMirror(dbMirror);
        dsEI.setConfig(config);
        rtn = dsEI.buildOutSql();// tableMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
        if (rtn) {
            // Build cleanup Queries (drop archive table)
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            String cleanUpArchive = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
            let.addCleanUpSql(TableUtils.DROP_DESC, cleanUpArchive);

            // Check Partition Counts.
            if (let.getPartitioned() && let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit()) {
                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the EXPORT_IMPORT " +
                        "partition limit (hybrid->exportImportPartitionLimit) of " + config.getHybrid().getExportImportPartitionLimit() +
                        ".  The queries will NOT be automatically run.");
                rtn = Boolean.FALSE;
            }
        }

        // run queries.
        if (rtn) {
            config.getCluster(Environment.LEFT).runTableSql(tableMirror);
        }

        return rtn;
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
