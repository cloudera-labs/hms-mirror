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

import com.cloudera.utils.hms.mirror.CopySpec;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.StatsCalculatorService;
import com.cloudera.utils.hms.mirror.service.TableService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import com.cloudera.utils.hms.util.ConfigUtils;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.List;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.SessionVars.*;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.ACID_INPLACE;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;

@Component
@Slf4j
@Getter
public class SQLAcidInPlaceDataStrategy extends DataStrategyBase implements DataStrategy {

    private TableService tableService;
    private StatsCalculatorService statsCalculatorService;

    public SQLAcidInPlaceDataStrategy(ExecuteSessionService executeSessionService, TranslatorService translatorService) {
        this.executeSessionService = executeSessionService;
        this.translatorService = translatorService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        // Check the Left to ensure it hasn't already been processed by 'inplace'
        if (TableUtils.hasTblProperty(ACID_INPLACE, let)) {
            let.addIssue(INPLACE_MIGRATED.getDesc());
            String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), tableMirror.getParent().getName(), tableMirror.getName(),
                    INPLACE_MIGRATED.getDesc());
            log.error(msg);
            return Boolean.FALSE;
        }

        // If 'inplace' and not 'downgrade', this process is about 'removing' the bucket definitions.
        // So if there are buckets and the threshold is met, skip the conversion.
        if (config.getMigrateACID().isInplace() && !config.getMigrateACID().isDowngrade()) {
            int bucketCount = TableUtils.numOfBuckets(let);
            if (bucketCount > config.getMigrateACID().getArtificialBucketThreshold()) {
                // Skip.
                String msg = MessageFormat.format(BUCKET_CONVERSION_THRESHOLD.getDesc(),
                        tableMirror.getParent().getName(), tableMirror.getName(),
                        config.getMigrateACID().getArtificialBucketThreshold(), bucketCount);
                log.error(msg);
                let.addIssue(msg);
                return Boolean.FALSE;
            }
        }

        // Use db
        String useDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getName());
        let.addSql(TableUtils.USE_DESC, useDb);

        // Build Right (to be used as new table on left).
        CopySpec leftNewTableSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);
        leftNewTableSpec.setTakeOwnership(Boolean.TRUE);
        if (config.getMigrateACID().isDowngrade()) {
            leftNewTableSpec.setMakeExternal(Boolean.TRUE);
        }
        // Location of converted data will got to default location.
        leftNewTableSpec.setStripLocation(Boolean.TRUE);

        rtn = buildTableSchema(leftNewTableSpec);

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
        String newCreateTable = getTableService().getCreateStatement(tableMirror, Environment.RIGHT);
        let.addSql(TableUtils.CREATE_DESC, newCreateTable);

        rtn = Boolean.TRUE;

        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        // Check to see if there are partitions.
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);

        // Ensure we're in the right database.
        String database = tableMirror.getParent().getName();
        String useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);
        // Set Override Properties.
        // Get the LEFT overrides for the DOWNGRADE.
        List<String> overrides = ConfigUtils.getPropertyOverridesFor(Environment.LEFT, hmsMirrorConfig);
//        Map<String, String> overrides = hmsMirrorConfig.getOptimization().getOverrides().getFor(Environment.LEFT);
//        if (!overrides.isEmpty()) {
            for (String setCmd : overrides) {
                let.addSql(setCmd, setCmd);
            }
//        }

        if (let.getPartitioned()) {
            if (hmsMirrorConfig.getOptimization().isSkip()) {
                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, MessageFormat.format(SET_SESSION_VALUE,SORT_DYNAMIC_PARTITION,"false"));
                }
                String partElement = TableUtils.getPartitionElements(let);
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                        let.getName(), ret.getName(), partElement);
                String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, let.getPartitions().size());
                let.addSql(new Pair(transferDesc, transferSql));
            } else if (hmsMirrorConfig.getOptimization().isSortDynamicPartitionInserts()) {
                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, MessageFormat.format(SET_SESSION_VALUE,SORT_DYNAMIC_PARTITION ,"true"));
                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                        let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, MessageFormat.format(SET_SESSION_VALUE,SORT_DYNAMIC_PARTITION_THRESHOLD ,"0"));
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
                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION, MessageFormat.format(SET_SESSION_VALUE,SORT_DYNAMIC_PARTITION ,"false"));
                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                        let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, MessageFormat.format(SET_SESSION_VALUE,SORT_DYNAMIC_PARTITION_THRESHOLD ,"-1"));
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
    public Boolean build(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        // Build cleanup Queries (drop archive table)
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);

        /*
        In this case, the LEFT is the source and we'll us the RIGHT cluster definition to hold the work. We need to ensure
        the RIGHT cluster is configured the same as the LEFT.
         */

        /*
        rename original table
        remove artificial bucket in new table def
        create new external table with original name
        from original_archive insert overwrite table new external (deal with partitions).
        write cleanup sql to drop original_archive.
         */
        try {
            rtn = buildOutDefinition(tableMirror);//tableMirror.buildoutSQLACIDDowngradeInplaceDefinition(config, dbMirror);
        } catch (RequiredConfigurationException e) {
            let.addError("Failed to build out definition: " + e.getMessage());
            rtn = Boolean.FALSE;
        }

        if (rtn) {
            String cleanUpArchive = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
            let.addCleanUpSql(TableUtils.DROP_DESC, cleanUpArchive);

            // Check Partition Counts.
            if (let.getPartitioned() && let.getPartitions().size() > hmsMirrorConfig.getMigrateACID().getPartitionLimit()) {
                let.addError("The number of partitions: " + let.getPartitions().size() + " exceeds the ACID SQL " +
                        "partition limit (migrateACID->partitionLimit) of " + hmsMirrorConfig.getMigrateACID().getPartitionLimit() +
                        ".  The queries will NOT be automatically run.");
                rtn = Boolean.FALSE;
            }
        }

        if (rtn) {
            // Build Transfer SQL
            try {
                rtn = buildOutSql(tableMirror);
            } catch (MissingDataPointException e) {
                let.addError("Failed to build out SQL: " + e.getMessage());
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

    @Autowired
    public void setStatsCalculatorService(StatsCalculatorService statsCalculatorService) {
        this.statsCalculatorService = statsCalculatorService;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }
}
