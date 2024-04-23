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

import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.service.HmsMirrorCfgService;
import com.cloudera.utils.hms.mirror.service.StatsCalculatorService;
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION;
import static com.cloudera.utils.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION_THRESHOLD;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;

@Component
@Slf4j
@Getter
public class SQLAcidDowngradeInPlaceDataStrategy extends DataStrategyBase implements DataStrategy {

    private TableService tableService;
    private StatsCalculatorService statsCalculatorService;

    public SQLAcidDowngradeInPlaceDataStrategy(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        // Use db
        String useDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getName());
        let.addSql(TableUtils.USE_DESC, useDb);

        // Build Right (to be used as new table on left).
        CopySpec leftNewTableSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);
        leftNewTableSpec.setTakeOwnership(Boolean.TRUE);
        leftNewTableSpec.setMakeExternal(Boolean.TRUE);
        // Location of converted data will got to default location.
        leftNewTableSpec.setStripLocation(Boolean.TRUE);

        rtn = getTableService().buildTableSchema(leftNewTableSpec);

        String origTableName = let.getName();

        // Rename Original Table
        // Remove property (if exists) to prevent rename from happening.
        if (TableUtils.hasTblProperty(TRANSLATED_TO_EXTERNAL, let)) {
            String unSetSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, origTableName, TRANSLATED_TO_EXTERNAL);
            let.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, unSetSql);
        }

        String newTblName = let.getName() + "_archive";
        String renameSql = MessageFormat.format(MirrorConf.RENAME_TABLE, origTableName, newTblName);
        TableUtils.changeTableName(let, newTblName);
        let.addSql(TableUtils.RENAME_TABLE, renameSql);

        // Check Buckets and Strip.
        int buckets = TableUtils.numOfBuckets(ret);
        if (buckets > 0 && buckets <= hmsMirrorConfig.getMigrateACID().getArtificialBucketThreshold()) {
            // Strip bucket definition.
            if (TableUtils.removeBuckets(ret, hmsMirrorConfig.getMigrateACID().getArtificialBucketThreshold())) {
                let.addIssue("Bucket Definition removed (was " + TableUtils.numOfBuckets(ret) + ") because it was EQUAL TO or BELOW " +
                        "the configured 'artificialBucketThreshold' of " +
                        hmsMirrorConfig.getMigrateACID().getArtificialBucketThreshold());
            }

        }
        // Create New Table.
        String newCreateTable = getTableService().getCreateStatement(tableMirror, Environment.RIGHT);
        let.addSql(TableUtils.CREATE_DESC, newCreateTable);

        rtn = Boolean.TRUE;

        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        // Check to see if there are partitions.
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        // Ensure we're in the right database.
        String database = tableMirror.getParent().getName();
        String useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);
        // Set Override Properties.
        if (!hmsMirrorConfig.getOptimization().getOverrides().getLeft().isEmpty()) {
            for (String key : hmsMirrorConfig.getOptimization().getOverrides().getLeft().keySet()) {
                let.addSql("Setting " + key, "set " + key + "=" + hmsMirrorConfig.getOptimization().getOverrides().getLeft().get(key));
            }
        }

        if (let.getPartitioned()) {
            if (hmsMirrorConfig.getOptimization().isSkip()) {
                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                }
                String partElement = TableUtils.getPartitionElements(let);
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                        let.getName(), ret.getName(), partElement);
                String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, let.getPartitions().size());
                let.addSql(new Pair(transferDesc, transferSql));
            } else if (hmsMirrorConfig.getOptimization().isSortDynamicPartitionInserts()) {
                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                        let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                    }
                }
                String partElement = TableUtils.getPartitionElements(let);
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                        let.getName(), ret.getName(), partElement);
                String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                let.addSql(new Pair(transferDesc, transferSql));
            } else {
                // Prescriptive Optimization.
                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                        let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                    }
                }

                String partElement = TableUtils.getPartitionElements(let);
                String distPartElement = statsCalculatorService.getDistributedPartitionElements(let);
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                        let.getName(), ret.getName(), partElement, distPartElement);
                String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                let.addSql(new Pair(transferDesc, transferSql));
            }
        } else {
            // Simple FROM .. INSERT OVERWRITE ... SELECT *;
            String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE, let.getName(), ret.getName());
            let.addSql(new Pair(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, transferSql));
        }

        rtn = Boolean.TRUE;

        return rtn;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;

        /*
        In this case, the LEFT is the source and we'll us the RIGHT cluster definition to hold the work. We need to ensure
        the RIGHT cluster is configured the same as the LEFT.
         */
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();
        /*
        rename original table
        remove artificial bucket in new table def
        create new external table with original name
        from original_archive insert overwrite table new external (deal with partitions).
        write cleanup sql to drop original_archive.
         */
        rtn = buildOutDefinition(tableMirror);//tableMirror.buildoutSQLACIDDowngradeInplaceDefinition(config, dbMirror);

        if (rtn) {
            // Build cleanup Queries (drop archive table)
            EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
            String cleanUpArchive = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
            let.addCleanUpSql(TableUtils.DROP_DESC, cleanUpArchive);

            // Check Partition Counts.
            if (let.getPartitioned() && let.getPartitions().size() > hmsMirrorConfig.getMigrateACID().getPartitionLimit()) {
                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the ACID SQL " +
                        "partition limit (migrateACID->partitionLimit) of " + hmsMirrorConfig.getMigrateACID().getPartitionLimit() +
                        ".  The queries will NOT be automatically run.");
                rtn = Boolean.FALSE;
            }
        }

        if (rtn) {
            // Build Transfer SQL
            rtn = buildOutSql(tableMirror);
        }

        // run queries.
        if (rtn) {
            getTableService().runTableSql(tableMirror, Environment.LEFT);
        }

        return rtn;
    }

    @Autowired
    public void setStatsCalculatorService(StatsCalculatorService statsCalculatorService) {
        this.statsCalculatorService = statsCalculatorService;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }
}
