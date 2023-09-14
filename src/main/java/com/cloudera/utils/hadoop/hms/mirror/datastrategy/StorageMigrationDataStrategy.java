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
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.LOCATION_NOT_MATCH_WAREHOUSE;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.DB_LOCATION;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.DB_MANAGED_LOCATION;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.*;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION_THRESHOLD;
import static com.cloudera.utils.hadoop.hms.mirror.TablePropertyVars.HMS_STORAGE_MIGRATION_FLAG;
import static com.cloudera.utils.hadoop.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;

public class StorageMigrationDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LogManager.getLogger(StorageMigrationDataStrategy.class);
    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.FALSE;
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
        try {
        /*
        If using distcp, we don't need to go through and rename/recreate the tables.  We just need to change the
        location of the current tables and partitions.
         */
            if (config.getTransfer().getStorageMigration().isDistcp()) {

                String database = dbMirror.getName();
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                let.addSql(TableUtils.USE_DESC, useDb);

                Boolean noIssues = Boolean.TRUE;
                String origLocation = TableUtils.getLocation(tableMirror.getName(), tableMirror.getTableDefinition(Environment.LEFT));
                try {
                    String newLocation = Context.getInstance().getConfig().getTranslator().
                            translateTableLocation(tableMirror, origLocation, 0, null);

                    // Build Alter Statement for Table to change location.
                    String alterTable = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, tableMirror.getEnvironmentTable(Environment.LEFT).getName(), newLocation);
                    Pair alterTablePair = new Pair(MirrorConf.ALTER_TABLE_LOCATION_DESC, alterTable);
                    let.addSql(alterTablePair);
                    if (config.getTransfer().getWarehouse().getExternalDirectory() != null &&
                            config.getTransfer().getWarehouse().getManagedDirectory() != null) {
                        if (TableUtils.isExternal(tableMirror.getEnvironmentTable(Environment.LEFT))) {
                            // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
                            if (!newLocation.startsWith(tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION))) {
                                // Set warning that even though you've specified to warehouse directories, the current configuration
                                // will NOT place it in that directory.
                                String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
                                        tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION),
                                        newLocation);
                                tableMirror.addIssue(Environment.LEFT, msg);
                            }
                        } else {
                            String location = null;
                            // Need to make adjustments for hdp3 hive 3.
                            if (config.getCluster(Environment.LEFT).isHdpHive3()) {
                                location = tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION);
                            } else {
                                location = tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION);
                            }
                            if (!newLocation.startsWith(location)) {
                                // Set warning that even though you've specified to warehouse directories, the current configuration
                                // will NOT place it in that directory.
                                String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
                                        tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION),
                                        newLocation);
                                tableMirror.addIssue(Environment.LEFT, msg);
                            }

                        }
                    }
                } catch (RuntimeException rte) {
                    noIssues = Boolean.FALSE;
                    tableMirror.addIssue(Environment.LEFT, rte.getMessage());
                    LOG.error(rte.getMessage(), rte);
                }

                // Build Alter Statement for Partitions to change location.
                if (let.getPartitioned()) {
                    // Loop through partitions in let.getPartitions and build alter statements.
                    for (Map.Entry<String, String> entry : let.getPartitions().entrySet()) {
                        String partSpec = entry.getKey();
                        int level = StringUtils.countMatches(partSpec, "/");
                        // Translate to 'partition spec'.
                        partSpec = TableUtils.toPartitionSpec(partSpec);
                        String partLocation = entry.getValue();
                        try {
                            String newPartLocation = Context.getInstance().getConfig().getTranslator().
                                    translateTableLocation(tableMirror, partLocation, ++level, entry.getKey());
                            String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_LOCATION, let.getName(), partSpec, newPartLocation);
                            String partSpecDesc = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_LOCATION_DESC, partSpec);
                            let.addSql(partSpecDesc, addPartSql);
                            if (config.getTransfer().getWarehouse().getExternalDirectory() != null &&
                                    config.getTransfer().getWarehouse().getManagedDirectory() != null) {
                                if (TableUtils.isExternal(tableMirror.getEnvironmentTable(Environment.LEFT))) {
                                    // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
                                    if (!newPartLocation.startsWith(tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION))) {
                                        // Set warning that even though you've specified to warehouse directories, the current configuration
                                        // will NOT place it in that directory.
                                        String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
                                                tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION),
                                                newPartLocation);
                                        tableMirror.addIssue(Environment.LEFT, msg);
                                    }
                                } else {
                                    String location = null;
                                    // Need to make adjustments for hdp3 hive 3.
                                    if (config.getCluster(Environment.LEFT).isHdpHive3()) {
                                        location = tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION);
                                    } else {
                                        location = tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION);
                                    }
                                    if (!newPartLocation.startsWith(location)) {
                                        // Set warning that even though you've specified to warehouse directories, the current configuration
                                        // will NOT place it in that directory.
                                        String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
                                                tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION),
                                                newPartLocation);
                                        tableMirror.addIssue(Environment.LEFT, msg);
                                    }

                                }
                            }
                        } catch (RuntimeException rte) {
                            noIssues = Boolean.FALSE;
                            tableMirror.addIssue(Environment.LEFT, rte.getMessage());
                        }
                    }
                    if (noIssues) {
                        rtn = Boolean.TRUE;
                    }
                } else {
                    rtn = Boolean.TRUE;
                }
            } else {
                rtn = tableMirror.buildoutSTORAGEMIGRATIONDefinition(config, dbMirror);
                if (rtn)
                    rtn = tableMirror.buildoutSTORAGEMIGRATIONSql(config, dbMirror);

                if (rtn) {
                    // Construct Transfer SQL
                    if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                        // We need to ensure that 'tez' is the execution engine.
                        let.addSql(new Pair(TEZ_EXECUTION_DESC, SET_TEZ_AS_EXECUTION_ENGINE));
                    }
                    // Set Override Properties.
                    if (config.getOptimization().getOverrides() != null) {
                        for (String key : config.getOptimization().getOverrides().getLeft().keySet()) {
                            let.addSql("Setting " + key, "set " + key + "=" + config.getOptimization().getOverrides().getLeft().get(key));
                        }
                    }

                    StatsCalculator.setSessionOptions(config.getCluster(Environment.LEFT), let, let);

                    // Need to see if the table has partitions.
                    if (let.getPartitioned()) {
                        // Check that the partition count doesn't exceed the configuration limit.
                        // Build Partition Elements.
                        if (config.getOptimization().getSkip()) {
                            if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                                let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                            }
                            String partElement = TableUtils.getPartitionElements(let);
                            String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                                    let.getName(), ret.getName(), partElement);
                            String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                            let.addSql(new Pair(transferDesc, transferSql));
                        } else if (config.getOptimization().getSortDynamicPartitionInserts()) {
                            // Declarative
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
                            // Prescriptive
                            if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                                let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                                if (!config.getCluster(Environment.LEFT).getHdpHive3()) {
                                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                                }
                            }
                            String partElement = TableUtils.getPartitionElements(let);
                            String distPartElement = StatsCalculator.getDistributedPartitionElements(let);
                            ;
                            String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                                    let.getName(), ret.getName(), partElement, distPartElement);
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
                }
            }
            if (rtn) {
                // Run the Transfer Scripts
                rtn = config.getCluster(Environment.LEFT).runTableSql(let.getSql(), tableMirror, Environment.LEFT);
            }
        } catch (Throwable t) {
            LOG.error("Error executing StorageMigrationDataStrategy", t);
            let.addIssue(t.getMessage());
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    @Override
    public Boolean buildOutDefinition() {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + tableMirror.getName() + " buildout SQL Definition");

        // Different transfer technique.  Staging location.
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        EnvironmentTable set = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);

        // Check that the table isn't already in the target location.
        StringBuilder sb = new StringBuilder();
        sb.append(config.getTransfer().getCommonStorage());
        String warehouseDir = null;
        if (TableUtils.isExternal(let)) {
            // External Location
            warehouseDir = config.getTransfer().getWarehouse().getExternalDirectory();
        } else {
            // Managed Location
            warehouseDir = config.getTransfer().getWarehouse().getManagedDirectory();
        }
        if (!config.getTransfer().getCommonStorage().endsWith("/") && !warehouseDir.startsWith("/")) {
            sb.append("/");
        }
        sb.append(warehouseDir);
        String lLocation = TableUtils.getLocation(tableMirror.getName(), let.getDefinition());
        if (lLocation.startsWith(sb.toString())) {
            tableMirror.addIssue(Environment.LEFT, "Table has already been migrated");
            return Boolean.FALSE;
        }
        // Add the STORAGE_MIGRATED flag to the table definition.
        DateFormat df = new SimpleDateFormat();
        TableUtils.upsertTblProperty(HMS_STORAGE_MIGRATION_FLAG, df.format(new Date()), let);

        // Create a 'target' table definition on left cluster with right definition (used only as place holder)
        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);

        if (TableUtils.isACID(let)) {
            copySpec.setStripLocation(Boolean.TRUE);
            if (config.getMigrateACID().isDowngrade()) {
                copySpec.setMakeExternal(Boolean.TRUE);
                copySpec.setTakeOwnership(Boolean.TRUE);
            }
        } else {
            copySpec.setReplaceLocation(Boolean.TRUE);
        }

        // Build Shadow from Source.
        rtn = tableMirror.buildTableSchema(copySpec);

        return rtn;
    }

    @Override
    public Boolean buildOutSql() {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + tableMirror.getName() + " buildout STORAGE_MIGRATION SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        let.getSql().clear();
        ret.getSql().clear();

        database = dbMirror.getName();
        useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);
        // Alter the current table and rename.
        // Remove property (if exists) to prevent rename from happening.
        if (TableUtils.hasTblProperty(TRANSLATED_TO_EXTERNAL, let)) {
            String unSetSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, ret.getName(), TRANSLATED_TO_EXTERNAL);
            let.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, unSetSql);
        }
        // Set unique name for old target to rename.
        let.setName(let.getName() + "_" + tableMirror.getUnique() + "_storage_migration");
        String origAlterRename = MessageFormat.format(MirrorConf.RENAME_TABLE, ret.getName(), let.getName());
        let.addSql(MirrorConf.RENAME_TABLE_DESC, origAlterRename);

        // Create table with New Location
        String createStmt2 = tableMirror.getCreateStatement(Environment.RIGHT);
        let.addSql(TableUtils.CREATE_DESC, createStmt2);
        if (!config.getCluster(Environment.LEFT).getLegacyHive() && config.getTransferOwnership() && let.getOwner() != null) {
            String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, let.getName(), let.getOwner());
            let.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
        }

        // Drop Renamed Table.
        String dropOriginalTable = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
        let.addCleanUpSql(MirrorConf.DROP_TABLE_DESC, dropOriginalTable);

        rtn = Boolean.TRUE;
        return rtn;
    }
}
