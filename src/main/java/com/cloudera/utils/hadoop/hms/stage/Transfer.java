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

import java.net.ConnectException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Transfer implements Callable<ReturnStatus> {
    private static final Logger LOG = LogManager.getLogger(Transfer.class);
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
            EnvironmentTable tet = tblMirror.getEnvironmentTable(Environment.TRANSFER);
            EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);
            EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);

            // Set Database to Transfer DB.
            tblMirror.setPhaseState(PhaseState.STARTED);

            tblMirror.setStrategy(config.getDataStrategy());

            tblMirror.incPhase();
            tblMirror.addStep("TRANSFER", config.getDataStrategy().toString());
            try {
                switch (config.getDataStrategy()) {
                    case DUMP:
                        successful = doDump();
                        break;
                    case SCHEMA_ONLY:
                        successful = doSchemaOnly();
                        break;
                    case LINKED:
                        successful = doLinked();
                        break;
                    case COMMON:
                        successful = doCommon();
                        break;
                    case EXPORT_IMPORT:
                        successful = doExportImport();
                        break;
                    case SQL:
                        successful = doSQL();
                        break;
                    case HYBRID:
                        if (TableUtils.isACID(let) && config.getMigrateACID().isDowngradeInPlace()) {
                            successful = doHYBRIDACIDInplaceDowngrade();
                        } else {
                            successful = doHybrid();
                        }
                        break;
                    case CONVERT_LINKED:
                        successful = doConvertLinked();
                        break;
                    case STORAGE_MIGRATION:
                        successful = doStorageMigrationTransfer();
                        break;
                }
                // Build out DISTCP workplans.
                if (successful && config.getTransfer().getStorageMigration().isDistcp()) {
                    // Build distcp reports.
                    if (config.getTransfer().getIntermediateStorage() != null) {
                        // LEFT PUSH INTERMEDIATE
                        // The Transfer Table should be available.
                        String isLoc = config.getTransfer().getIntermediateStorage();
                        // Deal with extra '/'
                        isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                        isLoc = isLoc + "/" +
                                config.getTransfer().getRemoteWorkingDirectory() + "/" +
                                config.getRunMarker() + "/" +
                                dbMirror.getName() + "/" +
                                tblMirror.getName();

                        config.getTranslator().addLocation(dbMirror.getName(), Environment.LEFT,
                                TableUtils.getLocation(tblMirror.getName(), let.getDefinition()),
                                isLoc);
                        // RIGHT PULL from INTERMEDIATE
                        String fnlLoc = null;
                        if (set.getDefinition().size() > 0) {
                            fnlLoc = TableUtils.getLocation(ret.getName(), set.getDefinition());
                        } else {
                            fnlLoc = TableUtils.getLocation(tblMirror.getName(), ret.getDefinition());
                            if (fnlLoc == null && config.getResetToDefaultLocation()) {
                                StringBuilder sbDir = new StringBuilder();
                                if (config.getTransfer().getCommonStorage() != null) {
                                    sbDir.append(config.getTransfer().getCommonStorage());
                                } else {
                                    sbDir.append(config.getCluster(Environment.RIGHT).getHcfsNamespace());
                                }
                                sbDir.append(config.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                                sbDir.append(dbMirror.getName()).append(".db").append("/").append(tblMirror.getName());
                                fnlLoc = sbDir.toString();
                            }
                        }
                        config.getTranslator().addLocation(dbMirror.getName(), Environment.RIGHT,
                                isLoc,
                                fnlLoc);
                    } else if (config.getTransfer().getCommonStorage() != null) {
                        // LEFT PUSH COMMON
                        String origLoc = TableUtils.isACID(let) ?
                                TableUtils.getLocation(let.getName(), tet.getDefinition()) :
                                TableUtils.getLocation(let.getName(), let.getDefinition());
                        String newLoc = null;
                        if (TableUtils.isACID(let)) {
                            if (config.getMigrateACID().isDowngrade()) {
                                newLoc = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                            } else {
                                newLoc = TableUtils.getLocation(ret.getName(), set.getDefinition());
                            }
                        } else {
                            newLoc = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                        }
                        if (newLoc == null && config.getResetToDefaultLocation()) {
                            StringBuilder sbDir = new StringBuilder();
                            sbDir.append(config.getTransfer().getCommonStorage());
                            sbDir.append(config.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                            sbDir.append(dbMirror.getName()).append(".db").append("/").append(tblMirror.getName());
                            newLoc = sbDir.toString();
                        }
                        config.getTranslator().addLocation(dbMirror.getName(), Environment.LEFT,
                                origLoc, newLoc);
                    } else {
                        // RIGHT PULL
                        if (TableUtils.isACID(let) && !config.getMigrateACID().isDowngrade()) {
                            tblMirror.addIssue(Environment.RIGHT, "`distcp` not needed with linked clusters for ACID " +
                                    "transfers.");
                        } else if (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade()) {
                            String rLoc = TableUtils.getLocation(tblMirror.getName(), ret.getDefinition());
                            if (rLoc == null && config.getResetToDefaultLocation()) {
                                StringBuilder sbDir = new StringBuilder();
                                if (config.getTransfer().getCommonStorage() != null) {
                                    sbDir.append(config.getTransfer().getCommonStorage());
                                } else {
                                    sbDir.append(config.getCluster(Environment.RIGHT).getHcfsNamespace());
                                }
                                sbDir.append(config.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                                sbDir.append(dbMirror.getName()).append(".db").append("/").append(tblMirror.getName());
                                rLoc = sbDir.toString();
                            }
                            config.getTranslator().addLocation(dbMirror.getName(), Environment.RIGHT,
                                    TableUtils.getLocation(tblMirror.getName(), tet.getDefinition()),
                                    rLoc);
                        } else {
                            String rLoc = TableUtils.getLocation(tblMirror.getName(), ret.getDefinition());
                            if (rLoc == null && config.getResetToDefaultLocation()) {
                                StringBuilder sbDir = new StringBuilder();
                                if (config.getTransfer().getCommonStorage() != null) {
                                    sbDir.append(config.getTransfer().getCommonStorage());
                                } else {
                                    sbDir.append(config.getCluster(Environment.RIGHT).getHcfsNamespace());
                                }
                                sbDir.append(config.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                                sbDir.append(dbMirror.getName()).append(".db").append("/").append(tblMirror.getName());
                                rLoc = sbDir.toString();
                            }
                            config.getTranslator().addLocation(dbMirror.getName(), Environment.RIGHT,
                                    TableUtils.getLocation(tblMirror.getName(), let.getDefinition())
                                    , rLoc);
                        }
                    }
                }

                if (successful)
                    tblMirror.setPhaseState(PhaseState.SUCCESS);
                else
                    tblMirror.setPhaseState(PhaseState.ERROR);
            } catch (ConnectionException ce) {
                tblMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + ce.getMessage());
                LOG.error("Connection Error", ce);
                ce.printStackTrace();
                rtn.setStatus(ReturnStatus.Status.FATAL);
                rtn.setException(ce);
            } catch (RuntimeException rte) {
                tblMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + rte.getMessage());
                LOG.error("Transfer Error", rte);
                rte.printStackTrace();
                rtn.setStatus(ReturnStatus.Status.FATAL);
                rtn.setException(rte);
            }

            Date end = new Date();
            Long diff = end.getTime() - start.getTime();
            tblMirror.setStageDuration(diff);
            LOG.info("Migration complete for " + dbMirror.getName() + "." + tblMirror.getName() + " in " +
                    diff + "ms");
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

        if (tblMirror.isACIDDowngradeInPlace(config, let)) {
            rtn = doSQLACIDDowngradeInplace();
        } else if (config.getTransfer().getIntermediateStorage() != null
                || config.getTransfer().getCommonStorage() != null
                || (TableUtils.isACID(let.getName(), let.getDefinition())
                && config.getMigrateACID().isOn())) {
            if (TableUtils.isACID(let.getName(), let.getDefinition())) {
                tblMirror.setStrategy(DataStrategy.ACID);
            }
            rtn = doIntermediateTransfer();
        } else {

            EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
            EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);

            // We should not get ACID tables in this routine.
            rtn = tblMirror.buildoutSQLDefinition(config, dbMirror);

            if (rtn)
                rtn = AVROCheck();

            if (rtn)
                rtn = tblMirror.buildoutSQLSql(config, dbMirror);

            // Construct Transfer SQL
            if (rtn) {
                rtn = tblMirror.buildTransferSql(let, set, ret, config);

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

        rtn = tblMirror.buildoutIntermediateDefinition(config, dbMirror);
        if (rtn)
            rtn = tblMirror.buildoutIntermediateSql(config, dbMirror);

        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = tblMirror.getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);
        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);

        if (rtn) {
            // Construct Transfer SQL
            if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                // We need to ensure that 'tez' is the execution engine.
                let.addSql(new Pair(MirrorConf.TEZ_EXECUTION_DESC, MirrorConf.SET_TEZ_AS_EXECUTION_ENGINE));
            }

            Pair cleanUp = new Pair("Post Migration Cleanup", "-- To be run AFTER final RIGHT SQL statements.");
            let.addCleanUpSql(cleanUp);

            String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
            Pair leftUsePair = new Pair(TableUtils.USE_DESC, useLeftDb);
            let.addCleanUpSql(leftUsePair);

            rtn = tblMirror.buildTransferSql(let, set, ret, config);

            // Execute the LEFT sql if config.execute.
            if (rtn) {
                rtn = config.getCluster(Environment.LEFT).runTableSql(tblMirror);
            }

            // Execute the RIGHT sql if config.execute.
            if (rtn) {
                rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
            }

            if (rtn) {
                // Run the Cleanup Scripts
                config.getCluster(Environment.LEFT).runTableSql(let.getCleanUpSql(), tblMirror, Environment.LEFT);
            }

            // RIGHT Shadow table
//            if (set.getDefinition().size() > 0) {
//                List<Pair> rightCleanup = new ArrayList<Pair>();
//
//                String useRightDb = MessageFormat.format(MirrorConf.USE, config.getResolvedDB(dbMirror.getName()));
//                Pair rightUsePair = new Pair(TableUtils.USE_DESC, useRightDb);
//                ret.addCleanUpSql(rightUsePair);
//                String rightDropShadow = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
//                Pair rightDropPair = new Pair(TableUtils.DROP_SHADOW_TABLE, rightDropShadow);
//                ret.addCleanUpSql(rightDropPair);
//                tblMirror.addStep("RIGHT ACID Shadow SQL Cleanup", "Built");
//
//                if (rtn) {
//                    // Run the Cleanup Scripts
//                    config.getCluster(Environment.RIGHT).runTableSql(ret.getCleanUpSql(), tblMirror, Environment.RIGHT);
//                }
//            }
        }
        return rtn;
    }

    protected Boolean doStorageMigrationTransfer() {
        Boolean rtn = Boolean.FALSE;

        rtn = tblMirror.buildoutSTORAGEMIGRATIONDefinition(config, dbMirror);
        if (rtn)
            rtn = tblMirror.buildoutSTORAGEMIGRATIONSql(config, dbMirror);

        if (rtn && !config.getTransfer().getStorageMigration().isDistcp()) {
            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
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
                            let.getName(), ret.getName(), partElement);
                    String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                    ret.addSql(new Pair(transferDesc, transferSql));
                } else {
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

        // Acid tables between legacy and non-legacy are forced to intermediate
        if (TableUtils.isACID(let) && config.legacyMigration()) {
            tblMirror.setStrategy(DataStrategy.ACID);
            if (config.getMigrateACID().isOn()) {
                rtn = doIntermediateTransfer();
            } else {
                let.addIssue(TableUtils.ACID_NOT_ON);
                rtn = Boolean.FALSE;
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit() &&
                        config.getHybrid().getExportImportPartitionLimit() > 0) {
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
                    rtn = doExportImport();
                }
            } else {
                // EXPORT
                tblMirror.setStrategy(DataStrategy.EXPORT_IMPORT);
                rtn = doExportImport();
            }
        }
        return rtn;
    }

    protected Boolean doDump() {
        Boolean rtn = Boolean.FALSE;

        rtn = tblMirror.buildoutDUMPDefinition(config, dbMirror);
        if (rtn) {
            rtn = tblMirror.buildoutDUMPSql(config, dbMirror);
        }
        return rtn;
    }

    protected Boolean doSchemaOnly() {
        Boolean rtn = Boolean.FALSE;

        rtn = tblMirror.buildoutSCHEMA_ONLYDefinition(config, dbMirror);
        if (rtn) {
            rtn = tblMirror.buildoutSCHEMA_ONLYSql(config, dbMirror);
        }
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
        }
        return rtn;
    }

    protected Boolean doLinked() {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = tblMirror.getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);
        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);

        if (TableUtils.isACID(let.getName(), let.getDefinition())) {
            tblMirror.addIssue(Environment.LEFT, "You can't 'LINK' ACID tables.");
            rtn = Boolean.FALSE;
        } else {
            rtn = tblMirror.buildoutLINKEDDefinition(config, dbMirror);
        }

        if (rtn) {
            rtn = tblMirror.buildoutLINKEDSql(config, dbMirror);
        }

        // Execute the RIGHT sql if config.execute.
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
        }

        return rtn;

    }

    protected Boolean doCommon() {
        Boolean rtn = Boolean.FALSE;


        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let.getName(), let.getDefinition())) {
            rtn = Boolean.FALSE;
            tblMirror.addIssue(Environment.RIGHT,
                    "Can't transfer SCHEMA reference on COMMON storage for ACID tables.");
        } else {
            rtn = tblMirror.buildoutCOMMONDefinition(config, dbMirror);
        }

        if (rtn) {
            rtn = tblMirror.buildoutCOMMONSql(config, dbMirror);
        }
        // Execute the RIGHT sql if config.execute.
        if (rtn) {
            rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
        }

        return rtn;
    }

    protected Boolean doExportImport() {
        Boolean rtn = Boolean.FALSE;
        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
        if (tblMirror.isACIDDowngradeInPlace(config, let)) {
            rtn = doEXPORTIMPORTACIDInplaceDowngrade();
        } else {
            if (TableUtils.isACID(let.getName(), let.getDefinition())) {
                if (config.getCluster(Environment.LEFT).getLegacyHive() != config.getCluster(Environment.RIGHT).getLegacyHive()) {
                    rtn = Boolean.FALSE;
                    tblMirror.addIssue(Environment.LEFT, "ACID table EXPORTs are NOT compatible for IMPORT to clusters on a different major version of Hive.");
                } else {
                    rtn = tblMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
                }

            } else {
                rtn = tblMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);

                if (rtn)
                    rtn = AVROCheck();
            }
            // If EXPORT_IMPORT, need to run LEFT queries.
            if (rtn) {
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
                rtn = doSchemaOnly();
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

    protected Boolean doHYBRIDACIDInplaceDowngrade() {
        Boolean rtn = Boolean.TRUE;
        /*
        Check environment is Hive 3.
            if not, need to do SQLACIDInplaceDowngrade.
        If table is not partitioned
            go to export import downgrade inplace
        else if partitions <= hybrid.exportImportPartitionLimit
            go to export import downgrade inplace
        else if partitions <= hybrid.sqlPartitionLimit
            go to sql downgrade inplace
        else
            too many partitions.
         */
        if (config.getCluster(Environment.LEFT).getLegacyHive()) {
            doSQLACIDDowngradeInplace();
        } else {
            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
            if (let.getPartitioned()) {
                // Partitions less than export limit or export limit set to 0 (or less), which means ignore.
                if (let.getPartitions().size() < config.getHybrid().getExportImportPartitionLimit() ||
                        config.getHybrid().getExportImportPartitionLimit() <= 0) {
                    doEXPORTIMPORTACIDInplaceDowngrade();
                } else {
                    doSQLACIDDowngradeInplace();
                }
            } else {
                // Go with EXPORT_IMPORT
                doEXPORTIMPORTACIDInplaceDowngrade();
            }
        }
        return rtn;
    }

    protected Boolean doSQLACIDDowngradeInplace() {
        Boolean rtn = Boolean.TRUE;
        /*
        rename original table
        remove artificial bucket in new table def
        create new external table with original name
        from original_archive insert overwrite table new external (deal with partitions).
        write cleanup sql to drop original_archive.
         */
        rtn = tblMirror.buildoutSQLACIDDowngradeInplaceDefinition(config, dbMirror);

        if (rtn) {
            // Build cleanup Queries (drop archive table)
            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
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
            rtn = tblMirror.buildoutSQLACIDDowngradeInplaceSQL(config, dbMirror);
        }

        // run queries.
        if (rtn) {
            config.getCluster(Environment.LEFT).runTableSql(tblMirror);
        }

        return rtn;
    }

    protected Boolean doEXPORTIMPORTACIDInplaceDowngrade() {
        Boolean rtn = Boolean.TRUE;
        /*
        rename original to archive
        export original table
        import as external to original tablename
        write cleanup sql to drop original_archive.
         */
        // Check Partition Limits before proceeding.
        rtn = tblMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
        if (rtn) {
            // Build cleanup Queries (drop archive table)
            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
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
            config.getCluster(Environment.LEFT).runTableSql(tblMirror);
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
            LOG.info(let.getName() + ": is an AVRO table.");
            String leftPath = TableUtils.getAVROSchemaPath(let);
            String rightPath = null;
            LOG.debug(let.getName() + ": Original AVRO Schema path: " + leftPath);
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
                LOG.info(let.getName() + " protocol Matcher found.");

                // Return the whole set of groups.
                String lns = matcher.group(0);

                // Does it match the "LEFT" hcfsNamespace.
                String leftNS = config.getCluster(Environment.LEFT).getHcfsNamespace();
                if (leftNS.endsWith("/")) {
                    leftNS = leftNS.substring(0, leftNS.length() - 1);
                }
                if (lns.startsWith(leftNS)) {
                    LOG.info(let.getName() + " table namespace matches LEFT clusters namespace.");

                    // They match, so replace with RIGHT hcfs namespace.
                    String newNS = config.getCluster(Environment.RIGHT).getHcfsNamespace();
                    if (newNS.endsWith("/")) {
                        newNS = newNS.substring(0, newNS.length() - 1);
                    }
                    rightPath = leftPath.replace(leftNS, newNS);
                    LOG.info(ret.getName() + " table namespace adjusted for RIGHT clusters table to " + rightPath);
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
                LOG.info(let.getName() + ": " + rpath);
                ret.addIssue(rpath);
                rightPath = leftPath;
                relative = Boolean.TRUE;
            }

            if (leftPath != null && rightPath != null && config.isCopyAvroSchemaUrls() && config.isExecute()) {
                // Copy over.
                LOG.info(let.getName() + ": Attempting to copy AVRO schema file to target cluster.");
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
                    LOG.error(ret.getName() + ": AVRO file copy issue", t);
                    ret.addIssue(t.getMessage());
                    rtn = Boolean.FALSE;
                } finally {
                    if (session != null)
                        config.getCliPool().returnSession(session);
                }
            } else {
                LOG.info(let.getName() + ": did NOT attempt to copy AVRO schema file to target cluster.");
            }
            tblMirror.addStep("AVRO", "Checked");
        } else {
            // Not AVRO, so no action (passthrough)
            rtn = Boolean.TRUE;
        }

        return rtn;
    }

}
