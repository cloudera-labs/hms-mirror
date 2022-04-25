/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.stage;

import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.HadoopSession;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Transfer implements Callable<ReturnStatus> {
    private static Logger LOG = LogManager.getLogger(Transfer.class);
    public static Pattern protocolNSPattern = Pattern.compile("(^.*://)([a-zA-Z0-9](?:(?:[a-zA-Z0-9-]*|(?<!-)\\.(?![-.]))*[a-zA-Z0-9]+)?)(:\\d{4})?");
    // Pattern to find the value of the last directory in a url.
    public static Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public Transfer(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
        this.tblMirror = tblMirror;
    }


    @Override
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();

        try {
            Date start = new Date();
            LOG.info("Migrating " + dbMirror.getName() + "." + tblMirror.getName());

            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);

            // Set Database to Transfer DB.
            tblMirror.setPhaseState(PhaseState.STARTED);

            tblMirror.setStrategy(config.getDataStrategy());

            tblMirror.incPhase();
            tblMirror.addStep("TRANSFER", config.getDataStrategy().toString());
            try {
                switch (config.getDataStrategy()) {
                    case DUMP:
                    case SCHEMA_ONLY:
                    case LINKED:
                    case COMMON:
                    case EXPORT_IMPORT:
                        successful = doBasic();
                        break;
                    case SQL:
                        if (config.getTransfer().getIntermediateStorage() != null || config.getTransfer().getCommonStorage() != null)
                            successful = doIntermediateTransfer();
                        else
                            successful = doSQL();
                        break;
                    case HYBRID:
                        successful = doHybrid();
                        break;
                    case CONVERT_LINKED:
                        successful = doConvertLinked();
                        break;
                    case STORAGE_MIGRATION:
                        successful = doStorageMigrationTransfer();
                        break;
                }
                if (successful)
                    tblMirror.setPhaseState(PhaseState.SUCCESS);
                else
                    tblMirror.setPhaseState(PhaseState.ERROR);
            } catch (RuntimeException rte) {
                tblMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + rte.getMessage());
                LOG.error("Transfer Error", rte);
                rte.printStackTrace();
            }

            Date end = new Date();
            Long diff = end.getTime() - start.getTime();
            tblMirror.setStageDuration(diff);
            LOG.info("Migration complete for " + dbMirror.getName() + "." + tblMirror.getName() + " in " +
                    Long.toString(diff) + "ms");
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } catch (Throwable t) {
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(t);
        }
        return rtn;
    }

    protected Boolean doSQL() {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);

        if (TableUtils.isACID(let.getName(), let.getDefinition())) {
            tblMirror.setStrategy(DataStrategy.ACID);
            if (config.getMigrateACID().isOn()) {
                rtn = doIntermediateTransfer();
            } else {
                let.addIssue(TableUtils.ACID_NOT_ON);
                rtn = Boolean.FALSE;
            }
        } else {
            rtn = tblMirror.buildoutDefinitions(config, dbMirror);

            if (rtn)
                rtn = AVROCheck();

            if (rtn)
                rtn = tblMirror.buildoutSql(config, dbMirror);

            // Construct Transfer SQL
            if (rtn) {
                if (let.getPartitioned()) {
                    // Check that the partition count doesn't exceed the configuration limit.
                    // Build Partition Elements.
                    if (config.getOptimization().getSortDynamicPartitionInserts()) {
                        ret.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION, "set " + MirrorConf.SORT_DYNAMIC_PARTITION + "=true");
                        ret.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                        String partElement = TableUtils.getPartitionElements(let);
                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                                set.getName(), ret.getName(), partElement);
                        String transferDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, let.getPartitions().size());
                        ret.addSql(new Pair(transferDesc, transferSql));
                    } else {
                        ret.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION, "set " + MirrorConf.SORT_DYNAMIC_PARTITION + "=false");
                        ret.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                        String partElement = TableUtils.getPartitionElements(let);
                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                                set.getName(), ret.getName(), partElement);
                        String transferDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, let.getPartitions().size());
                        ret.addSql(new Pair(transferDesc, transferSql));
                    }

                    if (config.getOptimization().getBuildShadowStatistics()) {
                        // TODO: The 'hive' service user needs WRITE permissions to the tables location.
                        // In my first test against hdp2.6, the ownership (without Ranger Plugins) was the user that build the original transfer table. doas=true.
                        // So this might fail in an environment that isn't too robust of access is different.
                    }
                    if (let.getPartitions().size() > config.getHybrid().getSqlPartitionLimit() && config.getHybrid().getSqlPartitionLimit() > 0) {
                        // The partition limit has been exceeded.  The process will need to be done manually.
                        let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                                "limit (hybrid->sqlPartitionLimit) of " + config.getHybrid().getSqlPartitionLimit() +
                                ".  This value is used to abort migrations that have a high potential for failure.  " +
                                "The migration will need to be done manually OR try increasing the limit. Review commandline option '-sp'.");
                        rtn = Boolean.FALSE;
                    }
                } else {
                    // No Partitions
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE, set.getName(), ret.getName());
                    ret.addSql(new Pair(TableUtils.LOAD_FROM_SHADOW_DESC, transferSql));
                }


                // Clean up shadow table.
                String dropShadowSql = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
                ret.getSql().add(new Pair(TableUtils.DROP_SHADOW_TABLE, dropShadowSql));

                // Execute the RIGHT sql if config.execute.
                if (rtn) {
                    config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
                }
            }
        }

        return rtn;
    }

    protected Boolean doIntermediateTransfer() {
        Boolean rtn = Boolean.FALSE;

        rtn = tblMirror.buildoutDefinitions(config, dbMirror);
        if (rtn)
            rtn = tblMirror.buildoutSql(config, dbMirror);

        if (rtn) {
            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
            EnvironmentTable tet = tblMirror.getEnvironmentTable(Environment.TRANSFER);
            EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);
            EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);

            // Construct Transfer SQL
            if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                // We need to ensure that 'tez' is the execution engine.
                let.addSql(new Pair(MirrorConf.TEZ_EXECUTION_DESC, MirrorConf.SET_TEZ_AS_EXECUTION_ENGINE));
            }
            // Need to see if the table has partitions.
            if (let.getPartitioned()) {
                // Check that the partition count doesn't exceed the configuration limit.
                // Build Partition Elements.
                if (config.getOptimization().getSortDynamicPartitionInserts() && !config.getCluster(Environment.LEFT).getLegacyHive()) {
                    let.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION, "set " + MirrorConf.SORT_DYNAMIC_PARTITION + "=true");
                    let.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                    String partElement = TableUtils.getPartitionElements(let);
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                            set.getName(), ret.getName(), partElement);
                    String transferDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, let.getPartitions().size());
                    let.addSql(new Pair(transferDesc, transferSql));
                } else {
                    let.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION, "set " + MirrorConf.SORT_DYNAMIC_PARTITION + "=false");
                    let.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                    String partElement = TableUtils.getPartitionElements(let);
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                            let.getName(), tet.getName(), partElement);
                    String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, let.getPartitions().size());
                    let.addSql(new Pair(transferDesc, transferSql));
                }

                if (TableUtils.isACID(let)) {
                    if (let.getPartitions().size() > config.getMigrateACID().getPartitionLimit() && config.getMigrateACID().getPartitionLimit() > 0) {
                        // The partition limit has been exceeded.  The process will need to be done manually.
                        let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                                "limit (migrateACID->partitionLimit) of " + config.getMigrateACID().getPartitionLimit() +
                                ".  This value is used to abort migrations that have a high potential for failure.  " +
                                "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ap'.");
                        rtn = Boolean.FALSE;
                    }
                } else {
                    if (let.getPartitions().size() > config.getHybrid().getSqlPartitionLimit() && config.getHybrid().getSqlPartitionLimit() > 0) {
                        // The partition limit has been exceeded.  The process will need to be done manually.
                        let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                                "limit (hybrid->sqlPartitionLimit) of " + config.getHybrid().getSqlPartitionLimit() +
                                ".  This value is used to abort migrations that have a high potential for failure.  " +
                                "The migration will need to be done manually OR try increasing the limit. Review commandline option '-sp'.");
                        rtn = Boolean.FALSE;
                    }
                }
            } else {
                // No Partitions
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE, let.getName(), tet.getName());
                let.addSql(new Pair(TableUtils.STAGE_TRANSFER_DESC, transferSql));
            }

            // Construct the Shadow Transfer SQL.  No transfer needed with commonStorage
            if (config.getTransfer().getCommonStorage() == null || (TableUtils.isACID(let) && !config.getMigrateACID().isDowngrade())) {
                if (let.getPartitioned()) {
                    // Build Partition Elements.

                    if (config.getOptimization().getSortDynamicPartitionInserts() && !config.getCluster(Environment.RIGHT).getLegacyHive()) {
                        ret.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION, "set " + MirrorConf.SORT_DYNAMIC_PARTITION + "=true");
                        ret.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                        String partElement = TableUtils.getPartitionElements(let);

                        String shadowTransferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                                set.getName(), ret.getName(), partElement);
                        String transferDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, let.getPartitions().size());
                        ret.addSql(new Pair(transferDesc, shadowTransferSql));
                    } else {
                        ret.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION, "set " + MirrorConf.SORT_DYNAMIC_PARTITION + "=false");
                        ret.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                        String partElement = TableUtils.getPartitionElements(let);
                        String shadowTransferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                                set.getName(), ret.getName(), partElement);
                        String transferDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, let.getPartitions().size());
                        ret.addSql(new Pair(transferDesc, shadowTransferSql));
                    }

                } else {
                    // No Partitions
                    String shadowTransferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE, set.getName(), ret.getName());
                    ret.addSql(new Pair(TableUtils.LOAD_FROM_SHADOW_DESC, shadowTransferSql));
                }
                tblMirror.addStep("Transfer/Shadow SQL", "Built");
            }

            // Execute the LEFT sql if config.execute.
            if (rtn) {
                rtn = config.getCluster(Environment.LEFT).runTableSql(tblMirror);
            }

            // Execute the RIGHT sql if config.execute.
            if (rtn) {
                rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
            }


            // Cleanup, POST creation and TRANSFERS.
            // LEFT TRANSFER table
            List<Pair> leftCleanup = new ArrayList<Pair>();

            Pair cleanUp = new Pair("Post Migration Cleanup", "-- To be run AFTER final RIGHT SQL statements.");
            let.addSql(cleanUp);

            String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
            Pair leftUsePair = new Pair(TableUtils.USE_DESC, useLeftDb);
            leftCleanup.add(leftUsePair);
            let.addSql(leftUsePair);

            if (config.isReplace() && TableUtils.isACID(let)) {
                // If 'replace' option is specified (along with -da and -cs), we change out the ACID table.
                // alter 'let' and rename to an archive
                String archiveOrigStmt = MessageFormat.format(MirrorConf.RENAME_TABLE, let.getName(),
                        MirrorConf.ARCHIVE + "_" + let.getName() + "_" + tblMirror.getUnique());
                Pair renamePairDesc = new Pair(MirrorConf.RENAME_TABLE_DESC, archiveOrigStmt);
                leftCleanup.add(renamePairDesc);
                let.addSql(renamePairDesc);
                // alter 'tet' and rename to original table name.
                String renameStmt = MessageFormat.format(MirrorConf.RENAME_TABLE, tet.getName(), let.getName());
                Pair renamePair = new Pair(MirrorConf.RENAME_TABLE_DESC, renameStmt);
                leftCleanup.add(renamePair);
                let.addSql(renamePair);

                String dropOriginalTable = MessageFormat.format(MirrorConf.DROP_TABLE,
                        MirrorConf.ARCHIVE + "_" + let.getName() + "_" + tblMirror.getUnique());
                let.addCleanUpSql(MirrorConf.DROP_TABLE_DESC, dropOriginalTable);
                tblMirror.addIssue(Environment.LEFT,"Run the LEFT Cleanup Script to remove the archive table after validation.");
                dbMirror.addIssue("The original table: " + let.getName() + " was renamed to: " + MirrorConf.ARCHIVE + "_" + let.getName() + "_" + tblMirror.getUnique() +
                        ", it can be removed by running the LEFT Cleanup Script.");
            } else {
                //
                String dropTransfer = MessageFormat.format(MirrorConf.DROP_TABLE, tet.getName());
                Pair leftDropPair = new Pair(TableUtils.DROP_TRANSFER_TABLE, dropTransfer);
                leftCleanup.add(leftDropPair);
                let.addSql(leftDropPair);
                tblMirror.addStep("LEFT ACID Transfer/Shadow SQL Cleanup", "Built");
            }

            if (rtn) {
                // Run the Cleanup Scripts
                config.getCluster(Environment.LEFT).runTableSql(leftCleanup, tblMirror, Environment.LEFT);
            }

            // RIGHT Shadow table
            if (config.getTransfer().getCommonStorage() == null) {
                List<Pair> rightCleanup = new ArrayList<Pair>();

                String useRightDb = MessageFormat.format(MirrorConf.USE, config.getResolvedDB(dbMirror.getName()));
                Pair rightUsePair = new Pair(TableUtils.USE_DESC, useRightDb);
                rightCleanup.add(rightUsePair);
                ret.addSql(rightUsePair);

                String rightDropShadow = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());

                Pair rightDropPair = new Pair(TableUtils.DROP_SHADOW_TABLE, rightDropShadow);

                rightCleanup.add(rightDropPair);
                ret.addSql(rightDropPair);
                tblMirror.addStep("RIGHT ACID Shadow SQL Cleanup", "Built");

                if (rtn) {
                    // Run the Cleanup Scripts
                    config.getCluster(Environment.RIGHT).runTableSql(rightCleanup, tblMirror, Environment.RIGHT);
                }
            }
        }
        return rtn;
    }

    protected Boolean doStorageMigrationTransfer() {
        Boolean rtn = Boolean.FALSE;

        rtn = tblMirror.buildoutDefinitions(config, dbMirror);
        if (rtn)
            rtn = tblMirror.buildoutSql(config, dbMirror);

        if (rtn) {
            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//            EnvironmentTable tet = tblMirror.getEnvironmentTable(Environment.TRANSFER);
//            EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);
            EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);

            // Construct Transfer SQL
            if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                // We need to ensure that 'tez' is the execution engine.
                let.addSql(new Pair(MirrorConf.TEZ_EXECUTION_DESC, MirrorConf.SET_TEZ_AS_EXECUTION_ENGINE));
            }
            // Need to see if the table has partitions.
            if (let.getPartitioned()) {
                // Check that the partition count doesn't exceed the configuration limit.
                // Build Partition Elements.
                if (config.getOptimization().getSortDynamicPartitionInserts() && !config.getCluster(Environment.RIGHT).getLegacyHive()) {
                    let.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION, "set " + MirrorConf.SORT_DYNAMIC_PARTITION + "=true");
                    let.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                    String partElement = TableUtils.getPartitionElements(let);

                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                            let.getName(), ret.getName(), partElement);
                    String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                    ret.addSql(new Pair(transferDesc, transferSql));
                } else {
                    let.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION, "set " + MirrorConf.SORT_DYNAMIC_PARTITION + "=false");
                    let.addSql("Setting " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + MirrorConf.SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                    String partElement = TableUtils.getPartitionElements(let);
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                            let.getName(), ret.getName(), partElement);
                    String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                    let.addSql(new Pair(transferDesc, transferSql));
                }

                if (TableUtils.isACID(let)) {
                    if (let.getPartitions().size() > config.getMigrateACID().getPartitionLimit() && config.getMigrateACID().getPartitionLimit() > 0) {
                        // The partition limit has been exceeded.  The process will need to be done manually.
                        let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                                "limit (migrateACID->partitionLimit) of " + config.getMigrateACID().getPartitionLimit() +
                                ".  This value is used to abort migrations that have a high potential for failure.  " +
                                "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ap'.");
                        rtn = Boolean.FALSE;
                    }
                } else {
                    if (let.getPartitions().size() > config.getHybrid().getSqlPartitionLimit() && config.getHybrid().getSqlPartitionLimit() > 0) {
                        // The partition limit has been exceeded.  The process will need to be done manually.
                        let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                                "limit (hybrid->sqlPartitionLimit) of " + config.getHybrid().getSqlPartitionLimit() +
                                ".  This value is used to abort migrations that have a high potential for failure.  " +
                                "The migration will need to be done manually OR try increasing the limit. Review commandline option '-sp'.");
                        rtn = Boolean.FALSE;
                    }
                }
            } else {
                // No Partitions
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE, let.getName(), ret.getName());
                let.addSql(new Pair(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, transferSql));
            }
            if (rtn) {
                // Run the Transfer Scripts
                config.getCluster(Environment.LEFT).runTableSql(let.getSql(), tblMirror, Environment.LEFT);
            }
        }
        return rtn;
    }

    protected Boolean doHybrid() {
        Boolean rtn = Boolean.FALSE;

        // Need to look at table.  ACID tables go to doACID()
        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let.getName(), let.getDefinition())) {
            tblMirror.setStrategy(DataStrategy.ACID);
            if (config.getMigrateACID().isOn()) {
                rtn = doIntermediateTransfer();
            } else {
                let.addIssue(TableUtils.ACID_NOT_ON);
                rtn = Boolean.FALSE;
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit()) {
                    // SQL
                    let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the EXPORT_IMPORT " +
                            "partition limit (hybrid->exportImportPartitionLimit) of " + config.getHybrid().getExportImportPartitionLimit() +
                            ".  Hence, the SQL method has been selected for the migration.");

                    tblMirror.setStrategy(DataStrategy.SQL);
                    if (config.getTransfer().getIntermediateStorage() != null ||
                            config.getTransfer().getCommonStorage() != null) {
                        rtn = doIntermediateTransfer();
                    } else {
                        rtn = doSQL();
                    }
                } else {
                    // EXPORT
                    tblMirror.setStrategy(DataStrategy.EXPORT_IMPORT);
                    rtn = doBasic();
                }
            } else {
                // EXPORT
                tblMirror.setStrategy(DataStrategy.EXPORT_IMPORT);
                rtn = doBasic();
            }
        }
        return rtn;
    }

    protected Boolean doBasic() {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let.getName(), let.getDefinition()) &&
                !(config.getDataStrategy() == DataStrategy.DUMP
                        || config.getDataStrategy() == DataStrategy.SCHEMA_ONLY)) {
            if (config.getDataStrategy() != DataStrategy.COMMON) {
                if (config.getDataStrategy() == DataStrategy.EXPORT_IMPORT) {
//                    rtn = tblMirror.buildoutDefinitions(config, dbMirror);
                    if (config.getCluster(Environment.LEFT).getLegacyHive() != config.getCluster(Environment.RIGHT).getLegacyHive()) {
                        rtn = Boolean.FALSE;
                        tblMirror.addIssue(Environment.LEFT, "ACID table EXPORTs are NOT compatible for IMPORT to clusters on a different major version of Hive.");
                    } else {
                        rtn = tblMirror.buildoutSql(config, dbMirror);
                    }

                    // If EXPORT_IMPORT, need to run LEFT queries.
//                    if (rtn && tblMirror.getStrategy() == DataStrategy.EXPORT_IMPORT) {
                    if (rtn)
                        rtn = config.getCluster(Environment.LEFT).runTableSql(tblMirror);
//                    }

                    // Execute the RIGHT sql if config.execute.
                    if (rtn) {
                        rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
                    }

                } else {
                    rtn = doHybrid();
                }
            } else {
                rtn = Boolean.FALSE;
                tblMirror.addIssue(Environment.RIGHT,
                        "Can't transfer SCHEMA reference on COMMON storage for ACID tables.");
            }
        } else {
            if (config.getDataStrategy() != DataStrategy.EXPORT_IMPORT) {
                rtn = tblMirror.buildoutDefinitions(config, dbMirror);
            } else {
                rtn = Boolean.TRUE;
            }

            if (rtn)
                rtn = AVROCheck();

            if (rtn)
                rtn = tblMirror.buildoutSql(config, dbMirror);

            // If EXPORT_IMPORT, need to run LEFT queries.
            if (rtn && tblMirror.getStrategy() == DataStrategy.EXPORT_IMPORT) {
                rtn = config.getCluster(Environment.LEFT).runTableSql(tblMirror);
            }

            // Execute the RIGHT sql if config.execute.
            if (rtn) {
                rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
            }
        }
        return rtn;
    }

    protected Boolean doConvertLinked() {
        Boolean rtn = Boolean.FALSE;
        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);

        String targetDBName = config.getResolvedDB(tblMirror.getDbName());

        // If RIGHT doesn't exist, run SCHEMA_ONLY.
        if (ret == null) {
            tblMirror.addIssue(Environment.RIGHT, "Table doesn't exist.  To transfer, run 'SCHEMA_ONLY'");
        } else {
            // Make sure table isn't an ACID table.
            if (TableUtils.isACID(let)) {
                tblMirror.addIssue(Environment.LEFT, "ACID tables not eligible for this operation");
            } else if (tblMirror.isPartitioned(Environment.LEFT)) {
                // We need to drop the RIGHT and RECREATE.
                ret.addIssue("Table is partitioned.  Need to change data strategy to drop and recreate.");
                String useDb = MessageFormat.format(MirrorConf.USE, targetDBName);
                ret.addSql(MirrorConf.USE_DESC, useDb);

                // Make sure the table is NOT set to purge.
                if (TableUtils.isExternalPurge(ret)) {
                    String purgeSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, ret.getName(), MirrorConf.EXTERNAL_TABLE_PURGE);
                    ret.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, purgeSql);
                }

                String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, tblMirror.getName());
                ret.addSql(MirrorConf.DROP_TABLE_DESC, dropTable);
                tblMirror.setStrategy(DataStrategy.SCHEMA_ONLY);
                // Set False that it doesn't exists, which it won't, since we're dropping it.
                ret.setExists(Boolean.FALSE);
                rtn = doBasic();
            } else {
                // - AVRO LOCATION
                if (AVROCheck()) {
                    String useDb = MessageFormat.format(MirrorConf.USE, targetDBName);
                    ret.addSql(MirrorConf.USE_DESC, useDb);
                    // Look at the table definition and get.
                    // - LOCATION
                    String sourceLocation = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                    String targetLocation = config.getTranslator().
                            translateTableLocation(targetDBName, tblMirror.getName(), sourceLocation, config);
                    String alterLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, ret.getName(), targetLocation);
                    ret.addSql(MirrorConf.ALTER_TABLE_LOCATION_DESC, alterLocSql);
                    // TableUtils.updateTableLocation(ret, targetLocation)
                    // - Check Comments for "legacy.managed" setting.
                    //    - MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG (if so, set purge flag MirrorConf.EXTERNAL_TABLE_PURGE)
                    if (TableUtils.isHMSLegacyManaged(tblMirror.getName(), ret.getDefinition())) {
                        // ALTER TABLE x SET TBLPROPERTIES ('purge flag').
                        String purgeSql = MessageFormat.format(MirrorConf.ADD_TABLE_PROP, ret.getName(), MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                        ret.addSql(MirrorConf.ADD_TABLE_PROP_DESC, purgeSql);
                    }
                    rtn = Boolean.TRUE;

                    // Execute the RIGHT sql if config.execute.
                    if (rtn) {
                        rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
                    }

                }
            }
        }

        return rtn;
    }


    protected Boolean AVROCheck() {
        Boolean rtn = Boolean.TRUE;
        Boolean relative = Boolean.FALSE;
        // Check for AVRO
        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
        if (TableUtils.isAVROSchemaBased(let)) {
            String leftPath = TableUtils.getAVROSchemaPath(let);
            String rightPath = null;
            LOG.debug("Original AVRO Schema path: " + leftPath);
                /* Checks:
                - Is Path prefixed with a protocol?
                    - (Y) Does it match the LEFT's hcfsNamespace.
                        - (Y) Replace prefix with RIGHT 'hcfsNamespace' prefix.
                        - (N) Throw WARNING and set return to FALSE.  We don't recognize the prefix and
                                 can't guarantee that we can retrieve the file.
                    - (N) Leave it and copy the file to the same relative path on the RIGHT
                 */
            Matcher matcher = protocolNSPattern.matcher(leftPath);
            // ProtocolNS Found.
            String cpCmd = null;
            if (matcher.find()) {
                // Return the whole set of groups.
                String lns = matcher.group(0);

                // Does it match the "LEFT" hcfsNamespace.
                String leftNS = config.getCluster(Environment.LEFT).getHcfsNamespace();
                if (leftNS.endsWith("/")) {
                    leftNS = leftNS.substring(0, leftNS.length() - 1);
                }
                if (lns.startsWith(leftNS)) {
                    // They match, so replace with RIGHT hcfs namespace.
                    String newNS = config.getCluster(Environment.RIGHT).getHcfsNamespace();
                    if (newNS.endsWith("/")) {
                        newNS = newNS.substring(0, newNS.length() - 1);
                    }
                    rightPath = leftPath.replace(leftNS, newNS);
                    TableUtils.updateAVROSchemaLocation(ret, rightPath);
                } else {
                    // Protocol found doesn't match configured hcfs namespace for LEFT.
                    String warning = "AVRO Schema URL was NOT adjusted. Current (LEFT) path did NOT match the " +
                            "LEFT hcfsnamespace. " + leftPath + " is NOT in the " + config.getCluster(Environment.LEFT).getHcfsNamespace() +
                            ". Can't determine change, so we'll not do anything.";
                    ret.addIssue(warning);
                    ret.addIssue("Schema creation may fail if location isn't available to RIGHT cluster.");
                    LOG.warn(warning);
                }
            } else {
                // No Protocol defined.  So we're assuming that its a relative path to the
                // defaultFS
                String rpath = "AVRO Schema URL appears to be relative: " + leftPath + ". No table definition adjustments.";
                ret.addIssue(rpath);
                LOG.debug(rpath);
                rightPath = leftPath;
                relative = Boolean.TRUE;
            }

            if (leftPath != null && rightPath != null && config.isCopyAvroSchemaUrls() && config.isExecute()) {
                // Copy over.
                HadoopSession session = null;
                try {
                    session = config.getCliPool().borrow();
                    CommandReturn cr = null;
                    if (relative) {
                        leftPath = config.getCluster(Environment.LEFT).getHcfsNamespace() + leftPath;
                        rightPath = config.getCluster(Environment.RIGHT).getHcfsNamespace() + rightPath;
                    }
                    LOG.info("AVRO Schema COPY from: " + leftPath + " to " + rightPath);
                    // Ensure the path for the right exists.
                    matcher = lastDirPattern.matcher(rightPath);
                    if (matcher.find()) {
                        String pathEnd = matcher.group(1);
                        String mkdir = rightPath.substring(0, rightPath.length() - pathEnd.length());
                        cr = session.processInput("mkdir -p " + mkdir);
                        if (cr.isError()) {
                            ret.addIssue("Problem creating directory " + mkdir + ". " + cr.getError());
                            rtn = Boolean.FALSE;
                        } else {
                            cr = session.processInput("cp -f " + leftPath + " " + rightPath);
                            if (cr.isError()) {
                                ret.addIssue("Problem copying AVRO schema file from " + leftPath + " to " +
                                        mkdir + ".\n```" + cr.getError() + "```");
                                rtn = Boolean.FALSE;
                            }
                        }
                    }
                } catch (Throwable t) {
                    LOG.error(t);
                    ret.addIssue(t.getMessage());
                    rtn = Boolean.FALSE;
                } finally {
                    if (session != null)
                        config.getCliPool().returnSession(session);
                }
            }
            tblMirror.addStep("AVRO", "Checked");
        } else {
            // Not AVRO, so no action (passthrough)
            rtn = Boolean.TRUE;
        }

        return rtn;
    }

}
