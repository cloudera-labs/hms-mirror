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
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static com.cloudera.utils.hms.mirror.MessageCode.LOCATION_NOT_MATCH_WAREHOUSE;
import static com.cloudera.utils.hms.mirror.MirrorConf.DB_LOCATION;
import static com.cloudera.utils.hms.mirror.MirrorConf.DB_MANAGED_LOCATION;
import static com.cloudera.utils.hms.mirror.SessionVars.*;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.HMS_STORAGE_MIGRATION_FLAG;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class StorageMigrationDataStrategy extends DataStrategyBase implements DataStrategy {

    private DatabaseService databaseService;
    private TableService tableService;
    //    private TranslatorService translatorService;
    private StatsCalculatorService statsCalculatorService;
    private WarehouseService warehouseService;

    @Autowired
    public void setWarehouseService(WarehouseService warehouseService) {
        this.warehouseService = warehouseService;
    }

    @Autowired
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    public StorageMigrationDataStrategy(ExecuteSessionService executeSessionService, TranslatorService translatorService) {
        this.executeSessionService = executeSessionService;
        this.translatorService = translatorService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;

        log.debug("Table: {} buildout SQL Definition", tableMirror.getName());
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        // Different transfer technique.  Staging location.
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        EnvironmentTable set = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT, tableMirror);

        // Check that the table isn't already in the target location.
        StringBuilder sb = new StringBuilder();
        sb.append(hmsMirrorConfig.getTransfer().getTargetNamespace());
        String warehouseDir = null;
        // Get the Warehouse for the database
        Warehouse dbWarehouse = null;
        try {
            dbWarehouse = warehouseService.getWarehousePlan(tableMirror.getParent().getName());
            assert dbWarehouse != null;
        } catch (MissingDataPointException e) {
            log.error(MessageCode.ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS.getDesc(), e);
            let.addIssue(MessageCode.ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS.getDesc());
            rtn = Boolean.FALSE;
            return rtn;
        }
        if (TableUtils.isExternal(let)) {
            // External Location
            warehouseDir = dbWarehouse.getExternalDirectory();
        } else {
            // Managed Location
            warehouseDir = dbWarehouse.getManagedDirectory();
        }

        if (!hmsMirrorConfig.getTransfer().getTargetNamespace().endsWith("/") && !warehouseDir.startsWith("/")) {
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
        copySpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);

        if (TableUtils.isACID(let)) {
            copySpec.setStripLocation(Boolean.TRUE);
            if (hmsMirrorConfig.getMigrateACID().isDowngrade()) {
                copySpec.setMakeExternal(Boolean.TRUE);
                copySpec.setTakeOwnership(Boolean.TRUE);
            }
        } else {
            copySpec.setReplaceLocation(Boolean.TRUE);
        }

        // Build Final from Source.
        rtn = buildTableSchema(copySpec);

        return rtn;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: {} buildout STORAGE_MIGRATION SQL", tableMirror.getName());
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        String useDb = null;
//        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        let.getSql().clear();
        ret.getSql().clear();

        String database = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);
        String originalDatabase = tableMirror.getParent().getName();

//        database = tableMirror.getParent().getName();
        useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);
        // Alter the current table and rename.
        // Remove property (if exists) to prevent rename from happening.
        if (TableUtils.hasTblProperty(TRANSLATED_TO_EXTERNAL, let)) {
            String unSetSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, ret.getName(), TRANSLATED_TO_EXTERNAL);
            let.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, unSetSql);
        }
        // Set unique name for old target to rename, IF the table is remaining in the same database.
        if (database.equals(originalDatabase)) {
//            let.setName(let.getName() + "_" + tableMirror.getUnique() + config.getTransfer().getStorageMigrationPostfix());
            // Removed the unique name from the table name. This was causing issues with
            //  users that wanted to script some comparison SQL since the name wasn't consistent.
            let.setName(let.getName() + config.getTransfer().getStorageMigrationPostfix());
            String origAlterRename = MessageFormat.format(MirrorConf.RENAME_TABLE, ret.getName(), let.getName());
            let.addSql(MirrorConf.RENAME_TABLE_DESC, origAlterRename);
        }

        // Create table with New Location
        String createStmt2 = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
        let.addSql(TableUtils.CREATE_DESC, createStmt2);
        if (!config.getCluster(Environment.LEFT).isLegacyHive() && config.getOwnershipTransfer().isTable() && let.getOwner() != null) {
            String ownerSql = MessageFormat.format(MirrorConf.SET_TABLE_OWNER, let.getName(), let.getOwner());
            let.addSql(MirrorConf.SET_TABLE_OWNER_DESC, ownerSql);
        }

        // Drop Renamed Table, if we are working within the same database.
        if (database.equals(originalDatabase)) {
            String dropOriginalTable = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
            let.addCleanUpSql(MirrorConf.DROP_TABLE_DESC, dropOriginalTable);
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        try {
        /*
        If using distcp, we don't need to go through and rename/recreate the tables.  We just need to change the
        location of the current tables and partitions.
         */
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                if (!config.getTransfer().getStorageMigration().isCreateArchive()) {
                    // No Archive, just adjust the table/partition locations and build distcp.
                    String database = tableMirror.getParent().getName();
                    String useDb = MessageFormat.format(MirrorConf.USE, database);

                    let.addSql(TableUtils.USE_DESC, useDb);

                    Boolean noIssues = Boolean.TRUE;
                    String origLocation = TableUtils.getLocation(tableMirror.getName(), tableMirror.getTableDefinition(Environment.LEFT));
                    try {
                        String newLocation = getTranslatorService().
                                translateTableLocation(tableMirror, origLocation, 1, null);

                        // Build Alter Statement for Table to change location.
                        String alterTable = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, tableMirror.getEnvironmentTable(Environment.LEFT).getName(), newLocation);
                        Pair alterTablePair = new Pair(MirrorConf.ALTER_TABLE_LOCATION_DESC, alterTable);
                        let.addSql(alterTablePair);
                        // Get the Warehouse from the Database Service.
//                    Warehouse warehouse = databaseService.getWarehousePlan(tableMirror.getParent().getName());
//                    if (nonNull(warehouse)) {
                        if (TableUtils.isExternal(tableMirror.getEnvironmentTable(Environment.LEFT))) {
                            // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
                            String lclLoc = tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
                            if (!isBlank(lclLoc) && !newLocation.startsWith(lclLoc)) {
                                // Set warning that even though you've specified to warehouse directories, the current configuration
                                // will NOT place it in that directory.
                                String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
                                        lclLoc, newLocation);
                                tableMirror.addIssue(Environment.LEFT, msg);
                                noIssues = Boolean.FALSE;
                            }
                        } else {
                            String location = null;
                            // Need to make adjustments for hdp3 hive 3.
                            if (config.getCluster(Environment.LEFT).isHdpHive3()) {
                                location = tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
                            } else {
                                location = tableMirror.getParent().getProperty(Environment.RIGHT, DB_MANAGED_LOCATION);
                            }
                            if (!isBlank(location) && !newLocation.startsWith(location)) {
                                // Set warning that even though you've specified to warehouse directories, the current configuration
                                // will NOT place it in that directory.
                                String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
                                        location, newLocation);
                                tableMirror.addIssue(Environment.LEFT, msg);
                                noIssues = Boolean.FALSE;
                            }

                        }
//                    } else {
//                        // Warehouse should NOT be null.  Issue with the Warehouse Plan.
//                        tableMirror.addIssue(Environment.LEFT, WAREHOUSE_DIRECTORIES_NOT_DEFINED.getDesc());
//                        noIssues = Boolean.FALSE;
//                    }
                    } catch (MismatchException | RequiredConfigurationException | MissingDataPointException rte) {
                        noIssues = Boolean.FALSE;
                        tableMirror.addIssue(Environment.LEFT, rte.getMessage());
                        log.error(rte.getMessage(), rte);
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
                            // If they are doing distcp, the partition location must match the partition spec in order to
                            // make a valid translation.
                            String normalizedPartSpecLocation = TableUtils.getDirectoryFromPartitionSpec(partSpec);
                            if (config.getTransfer().getStorageMigration().isDistcp()) {
                                if (!partLocation.endsWith(normalizedPartSpecLocation)) {
                                    // The partition location does not match the partition spec.  This is required for distcp to work properly.
                                    // (i.e. /user/hive/warehouse/db/table/odd does NOT match partition spec partition=1)
                                    String msg = MessageFormat.format(MessageCode.DISTCP_WITH_MISMATCHING_LOCATIONS.getDesc(),
                                            "Partition", partSpec, partLocation, normalizedPartSpecLocation);
                                    tableMirror.addIssue(Environment.LEFT, msg);
                                    noIssues = Boolean.FALSE;
                                    continue;
                                } else {
                                    // Since the partition location matches the partition spec, we also need
                                    // to verify that the directory the partition is in, matches the table name
                                    // so the distcp can work properly.
                                    String tableDir = partLocation.substring(0, partLocation.length() - normalizedPartSpecLocation.length() - 1);
                                    String tableDirName = tableDir.substring(tableDir.lastIndexOf("/") + 1);
                                    if (!tableDirName.equals(tableMirror.getName())) {
                                        // The directory name matches the table name.  This is required for distcp to work properly.
                                        String msg = MessageFormat.format(MessageCode.DISTCP_WITH_MISMATCHING_TABLE_LOCATION.getDesc(),
                                                "Partition", partSpec, partLocation, tableDirName);
                                        tableMirror.addIssue(Environment.LEFT, msg);
                                        noIssues = Boolean.FALSE;
                                        continue;
                                    }
                                }
                            }
                            try {
                                String newPartLocation = getTranslatorService().
                                        translateTableLocation(tableMirror, partLocation, ++level, entry.getKey());
                                String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_LOCATION, let.getName(), partSpec, newPartLocation);
                                String partSpecDesc = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_LOCATION_DESC, partSpec);
                                let.addSql(partSpecDesc, addPartSql);
                                // Getting an NPE here when using GLM's.
//                            if (hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() != null &&
//                                    hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory() != null) {
                                if (TableUtils.isExternal(tableMirror.getEnvironmentTable(Environment.LEFT))) {
                                    // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
                                    String lclLoc = tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
                                    if (!isBlank(lclLoc) && !newPartLocation.startsWith(lclLoc)) {
                                        // Set warning that even though you've specified to warehouse directories, the current configuration
                                        // will NOT place it in that directory.
                                        String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
                                                lclLoc, newPartLocation);
                                        tableMirror.addIssue(Environment.LEFT, msg);
                                        noIssues = Boolean.FALSE;
                                    }
                                } else {
                                    String location = null;
                                    // Need to make adjustments for hdp3 hive 3.
                                    if (config.getCluster(Environment.LEFT).isHdpHive3()) {
                                        location = tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
                                    } else {
                                        location = tableMirror.getParent().getProperty(Environment.RIGHT, DB_MANAGED_LOCATION);
                                    }
                                    if (nonNull(location) && !newPartLocation.startsWith(location)) {
                                        // Set warning that even though you've specified to warehouse directories, the current configuration
                                        // will NOT place it in that directory.
                                        String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
                                                location, newPartLocation);
                                        tableMirror.addIssue(Environment.LEFT, msg);
                                        noIssues = Boolean.FALSE;
                                    }

                                }
//                            }
                            } catch (MismatchException | RequiredConfigurationException rte) {
                                noIssues = Boolean.FALSE;
                                tableMirror.addIssue(Environment.LEFT, rte.getMessage());
                            }
                        }
                    } else {
                        rtn = Boolean.TRUE;
                    }
                    if (noIssues) {
                        rtn = Boolean.TRUE;

                        if (rtn)
                            rtn = AVROCheck(tableMirror);

                    } else if (config.getTransfer().getStorageMigration().isStrict()) {
                        log.warn("Cleaning up SQL due to issues for table: {}", tableMirror.getName());
                        let.addIssue(MessageCode.STORAGE_MIGRATION_STRICT.getDesc());
                        let.getSql().clear();
                        rtn = Boolean.FALSE;
                    }
                } else {
                    // Distcp with Archive.  The intent is to retain an archive of the table (and data)
                    //   even under the distcp movement strategy.  This allows the user access to the original
                    //   'data' under the archived table, which can be used for comparison or other purposes.
                    // Create new table
                    rtn = buildOutDefinition(tableMirror);

                    // Rename the table
                    rtn = buildOutSql(tableMirror);

                    // Need to build out SQL to recreate partitions (with new locations).
                    // Build Alter Statement for Partitions to change location.
                    boolean noIssues = Boolean.TRUE;
                    if (let.getPartitioned()) {
                        // Loop through partitions in RIGHT getPartitions (because they've already been constructed above
                        // in (buildOutDefinition) and build alter statements.
                        for (Map.Entry<String, String> entry : ret.getPartitions().entrySet()) {
                            String partSpec = entry.getKey();
//                            int level = StringUtils.countMatches(partSpec, "/");
                            // Translate to 'partition spec'.
                            partSpec = TableUtils.toPartitionSpec(partSpec);
                            String partLocation = entry.getValue();
                            // If they are doing distcp, the partition location must match the partition spec in order to
                            // make a valid translation.
                            String normalizedPartSpecLocation = TableUtils.getDirectoryFromPartitionSpec(partSpec);
                            if (config.getTransfer().getStorageMigration().isDistcp()) {
                                if (!partLocation.endsWith(normalizedPartSpecLocation)) {
                                    // The partition location does not match the partition spec.  This is required for distcp to work properly.
                                    // (i.e. /user/hive/warehouse/db/table/odd does NOT match partition spec partition=1)
                                    String msg = MessageFormat.format(MessageCode.DISTCP_WITH_MISMATCHING_LOCATIONS.getDesc(),
                                            "Partition", partSpec, partLocation, normalizedPartSpecLocation);
                                    tableMirror.addIssue(Environment.LEFT, msg);
                                    noIssues = Boolean.FALSE;
                                    continue;
                                } else {
                                    // Since the partition location matches the partition spec, we also need
                                    // to verify that the directory the partition is in, matches the table name
                                    // so the distcp can work properly.
                                    String tableDir = partLocation.substring(0, partLocation.length() - normalizedPartSpecLocation.length() - 1);
                                    String tableDirName = tableDir.substring(tableDir.lastIndexOf("/") + 1);
                                    if (!tableDirName.equals(tableMirror.getName())) {
                                        // The directory name matches the table name.  This is required for distcp to work properly.
                                        String msg = MessageFormat.format(MessageCode.DISTCP_WITH_MISMATCHING_TABLE_LOCATION.getDesc(),
                                                "Partition", partSpec, partLocation, tableDirName);
                                        tableMirror.addIssue(Environment.LEFT, msg);
                                        noIssues = Boolean.FALSE;
                                        continue;
                                    }
                                }
                            }

                            String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_ADD_PARTITION_LOCATION, let.getName(), partSpec, partLocation);
                            String partSpecDesc = MessageFormat.format(MirrorConf.ALTER_TABLE_ADD_PARTITION_LOCATION_DESC, partSpec);
                            let.addSql(partSpecDesc, addPartSql);

                            if (TableUtils.isExternal(tableMirror.getEnvironmentTable(Environment.LEFT))) {
                                // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
                                String lclLoc = tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
                                if (!isBlank(lclLoc) && !partLocation.startsWith(lclLoc)) {
                                    // Set warning that even though you've specified to warehouse directories, the current configuration
                                    // will NOT place it in that directory.
                                    String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
                                            lclLoc, partLocation);
                                    tableMirror.addIssue(Environment.LEFT, msg);
                                    noIssues = Boolean.FALSE;
                                }
                            } else {
                                String location = null;
                                // Need to make adjustments for hdp3 hive 3.
                                if (config.getCluster(Environment.LEFT).isHdpHive3()) {
                                    location = tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
                                } else {
                                    location = tableMirror.getParent().getProperty(Environment.RIGHT, DB_MANAGED_LOCATION);
                                }
                                if (nonNull(location) && !partLocation.startsWith(location)) {
                                    // Set warning that even though you've specified to warehouse directories, the current configuration
                                    // will NOT place it in that directory.
                                    String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
                                            location, partLocation);
                                    tableMirror.addIssue(Environment.LEFT, msg);
                                    noIssues = Boolean.FALSE;
                                }

                            }

                        }
                        if (noIssues) {
                            rtn = Boolean.TRUE;
                        } else if (config.getTransfer().getStorageMigration().isStrict()) {
                            log.warn("Cleaning up SQL due to issues for table: {}", tableMirror.getName());
                            let.addIssue(MessageCode.STORAGE_MIGRATION_STRICT.getDesc());
                            let.getSql().clear();
                            rtn = Boolean.FALSE;
                        }
                    }

                    // Build Distcp plan for moving table and partition data.
                }
            } else {
                // No Distcp (SQL)
                rtn = buildOutDefinition(tableMirror);

                if (rtn)
                    rtn = AVROCheck(tableMirror);

                if (rtn)
                    rtn = buildOutSql(tableMirror);

                if (rtn) {
                    // Construct Transfer SQL
                    if (config.getCluster(Environment.LEFT).isLegacyHive()) {
                        // We need to ensure that 'tez' is the execution engine.
                        let.addSql(new Pair(TEZ_EXECUTION_DESC, SET_TEZ_AS_EXECUTION_ENGINE));
                    }
                    // Set Override Properties.
                    Map<String, String> overrides = config.getOptimization().getOverrides().getFor(Environment.LEFT);
                    if (!overrides.isEmpty()) {
                        for (String key : overrides.keySet()) {
                            let.addSql("Setting " + key, "set " + key + "=" + overrides.get(key));
                        }
                    }

                    statsCalculatorService.setSessionOptions(config.getCluster(Environment.LEFT), let, let);

                    // Set the LEFT and RIGHT table names.  When the table migration is NOT to the same database, we need to
                    //  prefix the table name with the database name.
                    String database = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);
                    String originalDatabase = tableMirror.getParent().getName();
                    String leftTable = let.getName();
                    String rightTable = ret.getName();
                    if (!database.equals(originalDatabase)) {
                        leftTable = "`" + originalDatabase + "`.`" + let.getName() + "`";
                        rightTable = "`" + database + "`.`" + ret.getName() + "`";
                    }

                    // Need to see if the table has partitions.
                    if (let.getPartitioned()) {
                        // Check that the partition count doesn't exceed the configuration limit.
                        // Build Partition Elements.
                        if (config.getOptimization().isSkip()) {
                            if (!config.getCluster(Environment.LEFT).isLegacyHive()) {
                                let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                            }
                            String partElement = TableUtils.getPartitionElements(let);
                            String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                                    leftTable, rightTable, partElement);
                            String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                            let.addSql(new Pair(transferDesc, transferSql));
                        } else if (config.getOptimization().isSortDynamicPartitionInserts()) {
                            // Declarative
                            if (!config.getCluster(Environment.LEFT).isLegacyHive()) {
                                let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
                                if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
                                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                                }
                            }
                            String partElement = TableUtils.getPartitionElements(let);
                            String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                                    leftTable, rightTable, partElement);
                            String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                            let.addSql(new Pair(transferDesc, transferSql));
                        } else {
                            // Prescriptive
                            if (!config.getCluster(Environment.LEFT).isLegacyHive()) {
                                let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                                if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
                                    let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                                }
                            }
                            String partElement = TableUtils.getPartitionElements(let);
                            String distPartElement = statsCalculatorService.getDistributedPartitionElements(let);
                            String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                                    leftTable, rightTable, partElement, distPartElement);
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
                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE, leftTable, rightTable);
                        let.addSql(new Pair(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, transferSql));
                    }
                }
            }
            if (rtn) {
                // Run the Transfer Scripts
                rtn = tableService.runTableSql(tableMirror, Environment.LEFT);
            }
        } catch (MissingDataPointException | RequiredConfigurationException e) {
            log.error(MessageCode.ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS.getDesc(), e);
            let.addIssue(MessageCode.ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS.getDesc());
            rtn = Boolean.FALSE;
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

//    @Autowired
//    public void setTranslatorService(TranslatorService translatorService) {
//        this.translatorService = translatorService;
//    }
}
