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

import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.MessageFormat;

import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION_THRESHOLD;
import static com.cloudera.utils.hadoop.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;

public class SQLAcidDowngradeInPlaceDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LogManager.getLogger(SQLAcidDowngradeInPlaceDataStrategy.class);

    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.TRUE;

        /*
        In this case, the LEFT is the source and we'll us the RIGHT cluster definition to hold the work. We need to ensure
        the RIGHT cluster is configured the same as the LEFT.
         */
        Config config = Context.getInstance().getConfig();
        Cluster leftCluster = config.getCluster(Environment.LEFT);
        Cluster rightCluster = config.getCluster(Environment.RIGHT);
        rightCluster.setLegacyHive(leftCluster.getLegacyHive());
        rightCluster.setHdpHive3(leftCluster.isHdpHive3());
        /*
        rename original table
        remove artificial bucket in new table def
        create new external table with original name
        from original_archive insert overwrite table new external (deal with partitions).
        write cleanup sql to drop original_archive.
         */
        rtn = buildOutDefinition();//tableMirror.buildoutSQLACIDDowngradeInplaceDefinition(config, dbMirror);

        if (rtn) {
            // Build cleanup Queries (drop archive table)
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            String cleanUpArchive = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
            let.addCleanUpSql(TableUtils.DROP_DESC, cleanUpArchive);

            // Check Partition Counts.
            if (let.getPartitioned() && let.getPartitions().size() > config.getMigrateACID().getPartitionLimit()) {
                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the ACID SQL " +
                        "partition limit (migrateACID->partitionLimit) of " + config.getMigrateACID().getPartitionLimit() +
                        ".  The queries will NOT be automatically run.");
                rtn = Boolean.FALSE;
            }
        }

        if (rtn) {
            // Build Transfer SQL
            rtn = buildOutSql(); //tableMirror.buildoutSQLACIDDowngradeInplaceSQL(config, dbMirror);
        }

        // run queries.
        if (rtn) {
            config.getCluster(Environment.LEFT).runTableSql(tableMirror);
        }

        return rtn;
    }

    @Override
    public Boolean buildOutDefinition() {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        // Use db
        String useDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
        let.addSql(TableUtils.USE_DESC, useDb);

        // Build Right (to be used as new table on left).
        CopySpec leftNewTableSpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        leftNewTableSpec.setTakeOwnership(Boolean.TRUE);
        leftNewTableSpec.setMakeExternal(Boolean.TRUE);
        // Location of converted data will got to default location.
        leftNewTableSpec.setStripLocation(Boolean.TRUE);
//        leftNewTableSpec.set

        rtn = tableMirror.buildTableSchema(leftNewTableSpec);

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
        if (buckets > 0 && buckets <= config.getMigrateACID().getArtificialBucketThreshold()) {
            // Strip bucket definition.
            if (TableUtils.removeBuckets(ret, config.getMigrateACID().getArtificialBucketThreshold())) {
                let.addIssue("Bucket Definition removed (was " + TableUtils.numOfBuckets(ret) + ") because it was EQUAL TO or BELOW " +
                        "the configured 'artificialBucketThreshold' of " +
                        config.getMigrateACID().getArtificialBucketThreshold());
            }

        }
        // Create New Table.
        String newCreateTable = tableMirror.getCreateStatement(Environment.RIGHT);
        let.addSql(TableUtils.CREATE_DESC, newCreateTable);

        rtn = Boolean.TRUE;

        return rtn;
    }

    @Override
    public Boolean buildOutSql() {
        Boolean rtn = Boolean.FALSE;
        // Check to see if there are partitions.
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        // Ensure we're in the right database.
        String database = dbMirror.getName();
        String useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);
        // Set Override Properties.
        if (config.getOptimization().getOverrides().getLeft().size() > 0) {
            for (String key : config.getOptimization().getOverrides().getLeft().keySet()) {
                let.addSql("Setting " + key, "set " + key + "=" + config.getOptimization().getOverrides().getLeft().get(key));
            }
        }

        if (let.getPartitioned()) {
            if (config.getOptimization().getSkip()) {
                if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                }
                String partElement = TableUtils.getPartitionElements(let);
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                        let.getName(), ret.getName(), partElement);
                String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, let.getPartitions().size());
                let.addSql(new Pair(transferDesc, transferSql));
            } else if (config.getOptimization().getSortDynamicPartitionInserts()) {
                if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
                    if (!config.getCluster(Environment.LEFT).getHdpHive3()) {
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
                if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                    if (!config.getCluster(Environment.LEFT).getHdpHive3()) {
                        let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                    }
                }

                String partElement = TableUtils.getPartitionElements(let);
                String distPartElement = StatsCalculator.getDistributedPartitionElements(let);
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
}
