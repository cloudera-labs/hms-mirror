/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hadoop.HadoopSession;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.feature.Feature;
import com.cloudera.utils.hms.mirror.feature.FeaturesEnum;
import com.cloudera.utils.hms.stage.ReturnStatus;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.Future;
import java.util.regex.Matcher;

import static com.cloudera.utils.hms.mirror.MessageCode.LOCATION_NOT_MATCH_WAREHOUSE;
import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION;
import static com.cloudera.utils.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION_THRESHOLD;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.*;
import static com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum.DUMP;
import static com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum.STORAGE_MIGRATION;

@Service
@Getter
@Setter
@Slf4j
public class TableService {
    private final DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
    private final DateFormat tdf = new SimpleDateFormat("HH:mm:ss.SSS");

    private ConfigService configService;
    private ConnectionPoolService connectionPoolService;
    private QueryDefinitionsService queryDefinitionsService;
    private TranslatorService translatorService;
    private StatsCalculatorService statsCalculatorService;

    protected Boolean buildShadowToFinalSql(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        Config config = getConfigService().getConfig();

        // if common storage, skip
        // if inplace, skip
        EnvironmentTable source = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable shadow = tableMirror.getEnvironmentTable(Environment.SHADOW);
        EnvironmentTable target = tableMirror.getEnvironmentTable(Environment.RIGHT);
        if ((!TableUtils.isACID(source) && config.getTransfer().getCommonStorage() != null) ||
                isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
            // Nothing to build.
            return rtn;
        } else {
            if (source.getPartitioned()) {
                // MSCK repair on Shadow
                String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, shadow.getName());
                target.addSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
            }
            if (config.getOptimization().isBuildShadowStatistics()) {
                // Build Shadow Stats.

            }
            // Set Override Properties.
            if (config.getOptimization().getOverrides() != null) {
                for (String key : config.getOptimization().getOverrides().getRight().keySet()) {
                    target.addSql("Setting " + key, "set " + key + "=" + config.getOptimization().getOverrides().getRight().get(key));
                }
            }
            // Sql from Shadow to Final
            statsCalculatorService.setSessionOptions(config.getCluster(Environment.RIGHT), source, target);
            if (source.getPartitioned()) {
                if (config.getOptimization().isSkip()) {
                    if (!config.getCluster(Environment.RIGHT).isLegacyHive()) {
                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                    }
                    String partElement = TableUtils.getPartitionElements(source);
                    String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                            shadow.getName(), target.getName(), partElement);
                    String shadowDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, source.getPartitions().size());
                    target.addSql(new Pair(shadowDesc, shadowSql));
                } else if (config.getOptimization().isSortDynamicPartitionInserts()) {
                    if (!config.getCluster(Environment.RIGHT).isLegacyHive()) {
                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
                        if (!config.getCluster(Environment.RIGHT).isHdpHive3()) {
                            target.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                        }
                    }
                    String partElement = TableUtils.getPartitionElements(source);
                    String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                            shadow.getName(), target.getName(), partElement);
                    String shadowDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, source.getPartitions().size());
                    target.addSql(new Pair(shadowDesc, shadowSql));
                } else {
                    // Prescriptive
                    if (!config.getCluster(Environment.RIGHT).isLegacyHive()) {
                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                        if (!config.getCluster(Environment.RIGHT).isHdpHive3()) {
                            target.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                        }
                    }
                    String partElement = TableUtils.getPartitionElements(source);
                    String distPartElement = statsCalculatorService.getDistributedPartitionElements(source);
                    String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                            shadow.getName(), target.getName(), partElement, distPartElement);
                    String shadowDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, target.getPartitions().size());
                    target.addSql(new Pair(shadowDesc, shadowSql));
                }
            } else {
                String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE,
                        shadow.getName(), target.getName());
                String shadowDesc = MessageFormat.format(TableUtils.LOAD_FROM_SHADOW_DESC, "");
                target.addSql(new Pair(shadowDesc, shadowSql));
            }
            // Drop Shadow Table.
            String dropShadowSql = MessageFormat.format(MirrorConf.DROP_TABLE, shadow.getName());
            target.getSql().add(new Pair(TableUtils.DROP_SHADOW_TABLE, dropShadowSql));

        }
        return rtn;
    }

    protected Boolean buildSourceToTransferSql(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        Config config = getConfigService().getConfig();

        EnvironmentTable source, transfer, target;
        source = tableMirror.getEnvironmentTable(Environment.LEFT);
        transfer = tableMirror.getEnvironmentTable(Environment.TRANSFER);
        target = tableMirror.getEnvironmentTable(Environment.RIGHT);
        if (TableUtils.isACID(source)) {
            if (source.getPartitions().size() > config.getMigrateACID().getPartitionLimit() && config.getMigrateACID().getPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                source.addIssue("The number of partitions: " + source.getPartitions().size() + " exceeds the configuration " +
                        "limit (migrateACID->partitionLimit) of " + config.getMigrateACID().getPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ap'.");
                rtn = Boolean.FALSE;
            }
        } else {
            if (source.getPartitions().size() > config.getHybrid().getSqlPartitionLimit() &&
                    config.getHybrid().getSqlPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                source.addIssue("The number of partitions: " + source.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->sqlPartitionLimit) of " + config.getHybrid().getSqlPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit. Review commandline option '-sp'.");
                rtn = Boolean.FALSE;
            }
        }

        if (isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
            String dropOriginalTable = MessageFormat.format(MirrorConf.DROP_TABLE,
                    source.getName());
            source.addCleanUpSql(MirrorConf.DROP_TABLE_DESC, dropOriginalTable);
        }
        if (config.getTransfer().getStorageMigration().isDistcp()) {
            source.addSql("distcp specified", "-- Run distcp commands");
        } else {
            // Set Override Properties.
            if (config.getOptimization().getOverrides() != null) {
                for (String key : config.getOptimization().getOverrides().getLeft().keySet()) {
                    source.addSql("Setting " + key, "set " + key + "=" + config.getOptimization().getOverrides().getLeft().get(key));
                }
            }

            if (transfer.isDefined()) {
                statsCalculatorService.setSessionOptions(config.getCluster(Environment.LEFT), source, source);
                if (source.getPartitioned()) {
                    if (config.getOptimization().isSkip()) {
                        if (!config.getCluster(Environment.LEFT).isLegacyHive()) {
                            source.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                        }
                        String partElement = TableUtils.getPartitionElements(source);
                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                                source.getName(), transfer.getName(), partElement);
                        String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, source.getPartitions().size());
                        source.addSql(new Pair(transferDesc, transferSql));
                    } else if (config.getOptimization().isSortDynamicPartitionInserts()) {
                        if (!config.getCluster(Environment.LEFT).isLegacyHive()) {
                            source.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
                            if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
                                source.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                            }
                        }
                        String partElement = TableUtils.getPartitionElements(source);
                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                                source.getName(), transfer.getName(), partElement);
                        String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, source.getPartitions().size());
                        source.addSql(new Pair(transferDesc, transferSql));
                    } else {
                        if (!config.getCluster(Environment.LEFT).isLegacyHive()) {
                            source.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                            if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
                                source.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                            }
                        }
                        String partElement = TableUtils.getPartitionElements(source);
                        String distPartElement = statsCalculatorService.getDistributedPartitionElements(source);
                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                                source.getName(), transfer.getName(), partElement, distPartElement);
                        String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, source.getPartitions().size());
                        source.addSql(new Pair(transferDesc, transferSql));
                    }
                } else {
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE,
                            source.getName(), transfer.getName());
                    String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, source.getPartitions().size());
                    source.addSql(new Pair(transferDesc, transferSql));
                }
                // Drop Transfer Table
                if (!isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
                    String dropTransferSql = MessageFormat.format(MirrorConf.DROP_TABLE, transfer.getName());
                    source.getCleanUpSql().add(new Pair(TableUtils.DROP_SHADOW_TABLE, dropTransferSql));
                }
            }
        }
        return rtn;
    }

    public Boolean buildTableSchema(CopySpec copySpec) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Config config = getConfigService().getConfig();
        TableMirror tableMirror = copySpec.getTableMirror();
        Boolean rtn = Boolean.TRUE;

        EnvironmentTable source = tableMirror.getEnvironmentTable(copySpec.getSource());
        EnvironmentTable target = tableMirror.getEnvironmentTable(copySpec.getTarget());
        try {
            // Set Table Name
            if (source.isExists()) {
                target.setName(source.getName());

                // Clear the target spec.
                target.getDefinition().clear();
                // Reset with Source
                target.getDefinition().addAll(source.getDefinition());
                if (config.isEvaluatePartitionLocation() &&
                        source.getPartitioned()) {
                    if (!TableUtils.isACID(source)) {
                        // New Map.  So we can modify it..
                        Map<String, String> targetPartitions = new HashMap<>();
                        targetPartitions.putAll(source.getPartitions());
                        target.setPartitions(targetPartitions);
                        if (!getTranslatorService().translatePartitionLocations(tableMirror)) {
                            rtn = Boolean.FALSE;
                        }
                    }
                }
                if (TableUtils.isHiveNative(source)) {
                    // Rules
                    // 1. Strip db from create state.  It's broken anyway with the way
                    //      the quotes are.  And we're setting the target db in the context anyways.
                    TableUtils.stripDatabase(target);

                    if (copySpec.getLocation() != null) {
                        if (!TableUtils.updateTableLocation(target, copySpec.getLocation()))
                            rtn = Boolean.FALSE;
                    }

                    if (!TableUtils.doesTableNameMatchDirectoryName(target.getDefinition())) {
                        if (config.isResetToDefaultLocation()) {
                            target.addIssue("Tablename does NOT match last directory name. Using `rdl` will change " +
                                    "the implied path from the original.  This may affect other applications that aren't " +
                                    "relying on the metastore.");
                            if (config.getTransfer().getStorageMigration().isDistcp()) {
                                // We need to FAIL the table to ensure we point out that there is a disconnect.
                                rtn = Boolean.FALSE;
                                target.addIssue("Tablename does NOT match last directory name.  Using `dc|distcp` will copy " +
                                        "the data but the table will not align with the directory.");
                            }
                        }
                    }

                    // 1. If Managed, convert to EXTERNAL
                    // When coming from legacy and going to non-legacy (Hive 3).
                    Boolean converted = Boolean.FALSE;
                    if (!TableUtils.isACID(source)) {
                        // Non ACID tables.
                        if (copySpec.isUpgrade() && TableUtils.isManaged(source)) {
                            converted = TableUtils.makeExternal(target);
                            if (converted) {
                                target.addIssue("Schema 'converted' from LEGACY managed to EXTERNAL");
                                target.addProperty(HMS_MIRROR_LEGACY_MANAGED_FLAG, converted.toString());
                                target.addProperty(HMS_MIRROR_CONVERTED_FLAG, converted.toString());
                                if (copySpec.isTakeOwnership()) {
                                    if (!config.isNoPurge()) {
                                        target.addProperty(EXTERNAL_TABLE_PURGE, "true");
                                    }
                                } else {
                                    target.addIssue("Ownership of the data not allowed in this scenario, PURGE flag NOT set.");
                                }
                            }
                        } else {
                            if (copySpec.isMakeExternal()) {
                                converted = TableUtils.makeExternal(target);
                            }
                            if (copySpec.isTakeOwnership()) {
                                if (TableUtils.isACID(source)) {
                                    if (config.getMigrateACID().isDowngrade() && !config.isNoPurge()) {
                                        target.addProperty(EXTERNAL_TABLE_PURGE, "true");
                                    }
                                } else {
                                    target.addProperty(EXTERNAL_TABLE_PURGE, "true");
                                }
                            }
                        }
                    } else {
                        // Handle ACID tables.
                        if (copySpec.isMakeNonTransactional()) {
                            TableUtils.removeTblProperty(TRANSACTIONAL, target);
                            TableUtils.removeTblProperty(TRANSACTIONAL_PROPERTIES, target);
                            TableUtils.removeTblProperty(BUCKETING_VERSION, target);
                        }

                        if (copySpec.isMakeExternal())
                            converted = TableUtils.makeExternal(target);

                        if (copySpec.isTakeOwnership()) {
                            if (TableUtils.isACID(source)) {
                                if (copySpec.getTarget() == Environment.TRANSFER) {
                                    target.addProperty(EXTERNAL_TABLE_PURGE, "true");
                                } else if (config.getMigrateACID().isDowngrade() && !config.isNoPurge()) {
                                    target.addProperty(EXTERNAL_TABLE_PURGE, "true");
                                }
                            } else {
                                target.addProperty(EXTERNAL_TABLE_PURGE, "true");
                            }
                        }

                        if (copySpec.isStripLocation()) {
                            if (config.getMigrateACID().isDowngrade()) {
                                target.addIssue("Location Stripped from 'Downgraded' ACID definition.  Location will be the default " +
                                        "external location as configured by the database/environment.");
                            } else {
                                target.addIssue("Location Stripped from ACID definition.  Location element in 'CREATE' " +
                                        "not allowed in Hive3+");
                            }
                            TableUtils.stripLocation(target);
                        }

                        if (config.getMigrateACID().isDowngrade() && copySpec.isMakeExternal()) {
                            converted = TableUtils.makeExternal(target);
                            if (!config.isNoPurge()) {
                                target.addProperty(EXTERNAL_TABLE_PURGE, Boolean.TRUE.toString());
                            }
                            target.addProperty(DOWNGRADED_FROM_ACID, Boolean.TRUE.toString());
                        }

                        if (TableUtils.removeBuckets(target, config.getMigrateACID().getArtificialBucketThreshold())) {
                            target.addIssue("Bucket Definition removed (was " + TableUtils.numOfBuckets(source) + ") because it was EQUAL TO or BELOW " +
                                    "the configured 'artificialBucketThreshold' of " +
                                    config.getMigrateACID().getArtificialBucketThreshold());
                        }
                    }

                    // 2. Set mirror stage one flag
                    if (copySpec.getTarget() == Environment.RIGHT) {
                        target.addProperty(HMS_MIRROR_METADATA_FLAG, df.format(new Date()));
                    }

                    // 3. Rename table
                    if (copySpec.renameTable()) {
                        TableUtils.changeTableName(target, copySpec.getTableNamePrefix() + tableMirror.getName());
                    }

                    // 4. identify this table as being converted by hms-mirror
//        TableUtils.upsertTblProperty(MirrorConf.HMS_MIRROR_CONVERTED_FLAG, Boolean.TRUE.toString(), upperTD);

                    // 5. Strip stat properties
                    TableUtils.removeTblProperty("COLUMN_STATS_ACCURATE", target);
                    TableUtils.removeTblProperty("numFiles", target);
                    TableUtils.removeTblProperty("numRows", target);
                    TableUtils.removeTblProperty("rawDataSize", target);
                    TableUtils.removeTblProperty("totalSize", target);
                    TableUtils.removeTblProperty("discover.partitions", target);
                    TableUtils.removeTblProperty("transient_lastDdlTime", target);
                    // This is control by the create, so we don't want any legacy values to
                    // interrupt that.
                    TableUtils.removeTblProperty("external", target);
                    TableUtils.removeTblProperty("last_modified_by", target);
                    TableUtils.removeTblProperty("last_modified_time", target);

                    // 6. Set 'discover.partitions' if config and non-acid
                    if (config.getCluster(copySpec.getTarget()).getPartitionDiscovery().isAuto() && TableUtils.isPartitioned(target)) {
                        if (converted) {
                            target.addProperty(DISCOVER_PARTITIONS, Boolean.TRUE.toString());
                        } else if (TableUtils.isExternal(target)) {
                            target.addProperty(DISCOVER_PARTITIONS, Boolean.TRUE.toString());
                        }
                    }

                    // 5. Location Adjustments
                    //    Since we are looking at the same data as the original, we're not changing this now.
                    //    Any changes to data location are a part of stage-2 (STORAGE).
                    switch (copySpec.getTarget()) {
                        case LEFT:
                        case RIGHT:
                            if (copySpec.isReplaceLocation() && (!TableUtils.isACID(source) || config.getMigrateACID().isDowngrade())) {
                                String sourceLocation = TableUtils.getLocation(tableMirror.getName(), tableMirror.getTableDefinition(copySpec.getSource()));
                                int level = 1;
                                if (config.getFilter().isTableFiltering()) {
                                    level = 0;
                                }
                                String targetLocation = getTranslatorService().
                                        translateTableLocation(tableMirror, sourceLocation, level, null);
                                if (!TableUtils.updateTableLocation(target, targetLocation)) {
                                    rtn = Boolean.FALSE;
                                }
                                // Check if the locations align.  If not, warn.
                                if (config.getTransfer().getWarehouse().getExternalDirectory() != null &&
                                        config.getTransfer().getWarehouse().getManagedDirectory() != null) {
                                    if (TableUtils.isExternal(target)) {
                                        // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
                                        if (!targetLocation.startsWith(tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION))) {
                                            // Set warning that even though you've specified to warehouse directories, the current configuration
                                            // will NOT place it in that directory.
                                            String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
                                                    tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION),
                                                    targetLocation);
                                            tableMirror.addIssue(Environment.RIGHT, msg);
                                        }
                                    } else {
                                        if (!targetLocation.startsWith(tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION))) {
                                            // Set warning that even though you've specified to warehouse directories, the current configuration
                                            // will NOT place it in that directory.
                                            String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
                                                    tableMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION),
                                                    targetLocation);
                                            tableMirror.addIssue(Environment.RIGHT, msg);
                                        }

                                    }
                                }

                            }
//                        if (copySpec.getStripLocation()) {
//                            TableUtils.stripLocation(target);
//                        }
                            if (tableMirror.isReMapped()) {
                                target.addIssue(MessageCode.TABLE_LOCATION_REMAPPED.getDesc());
                            } else if (config.getTranslator().isForceExternalLocation()) {
                                target.addIssue(MessageCode.TABLE_LOCATION_FORCED.getDesc());
                            } else if (config.isResetToDefaultLocation()) {
                                TableUtils.stripLocation(target);
                                target.addIssue(MessageCode.RESET_TO_DEFAULT_LOCATION_WARNING.getDesc());
                            }
                            break;
                        case SHADOW:
                        case TRANSFER:
                            if (copySpec.getLocation() != null) {
                                if (!TableUtils.updateTableLocation(target, copySpec.getLocation())) {
                                    rtn = Boolean.FALSE;
                                }
                            } else if (copySpec.isReplaceLocation()) {
                                if (config.getTransfer().getIntermediateStorage() != null) {
                                    String isLoc = config.getTransfer().getIntermediateStorage();
                                    // Deal with extra '/'
                                    isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                                    isLoc = isLoc + "/" + config.getTransfer().getRemoteWorkingDirectory() + "/" +
                                            config.getRunMarker() + "/" +
//                                        config.getTransfer().getTransferPrefix() + this.getUnique() + "_" +
                                            tableMirror.getParent().getName() + "/" +
                                            tableMirror.getName();
                                    if (!TableUtils.updateTableLocation(target, isLoc)) {
                                        rtn = Boolean.FALSE;
                                    }
                                } else if (config.getTransfer().getCommonStorage() != null) {
                                    String sourceLocation = TableUtils.getLocation(tableMirror.getName(), tableMirror.getTableDefinition(copySpec.getSource()));
                                    String targetLocation = getTranslatorService().
                                            translateTableLocation(tableMirror, sourceLocation, 1, null);
                                    if (!TableUtils.updateTableLocation(target, targetLocation)) {
                                        rtn = Boolean.FALSE;
                                    }
                                } else if (copySpec.isStripLocation()) {
                                    TableUtils.stripLocation(target);
                                } else if (config.isReplace()) {
                                    String sourceLocation = TableUtils.getLocation(tableMirror.getName(), tableMirror.getTableDefinition(copySpec.getSource()));
                                    String replacementLocation = sourceLocation + "_replacement";
                                    if (!TableUtils.updateTableLocation(target, replacementLocation)) {
                                        rtn = Boolean.FALSE;
                                    }
                                } else {
                                    // Need to use export location
                                    String isLoc = config.getTransfer().getExportBaseDirPrefix();
                                    // Deal with extra '/'
                                    isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                                    isLoc = config.getCluster(Environment.LEFT).getHcfsNamespace() +
                                            isLoc + tableMirror.getParent().getName() + "/" + tableMirror.getName();
                                    if (!TableUtils.updateTableLocation(target, isLoc)) {
                                        rtn = Boolean.FALSE;
                                    }
                                }
                            }
                            break;
                    }

                    switch (copySpec.getTarget()) {
                        case TRANSFER:
                            TableUtils.upsertTblProperty(HMS_MIRROR_TRANSFER_TABLE, "true", target);
                            break;
                        case SHADOW:
                            TableUtils.upsertTblProperty(HMS_MIRROR_SHADOW_TABLE, "true", target);
                            break;
                    }
                    // 6. Go through the features, if any.
                    if (!config.isSkipFeatures()) {
                        for (FeaturesEnum features : FeaturesEnum.values()) {
                            Feature feature = features.getFeature();
                            log.debug("Table: " + tableMirror.getName() + " - Checking Feature: " + features);
                            if (feature.fixSchema(target)) {
                                log.debug("Table: " + tableMirror.getName() + " - Feature Applicable: " + features);
                                target.addIssue("Feature (" + features + ") was found applicable and adjustments applied. " +
                                        feature.getDescription());
                            } else {
                                log.debug("Table: " + tableMirror.getName() + " - Feature NOT Applicable: " + features);
                            }
                        }
                    } else {
                        log.debug("Table: " + tableMirror.getName() + " - Skipping Features Check...");
                    }

                    if (config.isTranslateLegacy()) {
                        if (config.getLegacyTranslations().fixSchema(target)) {
                            log.info("Legacy Translation applied to: " + tableMirror.getParent().getName() + ":" + target.getName());
                        }
                    }

                    // Add props to definition.
                    if (tableMirror.whereTherePropsAdded(copySpec.getTarget())) {
                        Set<String> keys = target.getAddProperties().keySet();
                        for (String key : keys) {
                            TableUtils.upsertTblProperty(key, target.getAddProperties().get(key), target);
                        }
                    }

                    if (!copySpec.isTakeOwnership() && config.getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION) {
                        TableUtils.removeTblProperty(EXTERNAL_TABLE_PURGE, target);
                    }

                    if (config.getCluster(copySpec.getTarget()).isLegacyHive() && config.getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION) {
                        // remove newer flags;
                        TableUtils.removeTblProperty(EXTERNAL_TABLE_PURGE, target);
                        TableUtils.removeTblProperty(DISCOVER_PARTITIONS, target);
                        TableUtils.removeTblProperty(BUCKETING_VERSION, target);
                    }

                } else if (TableUtils.isView(target)) {
                    source.addIssue("This is a VIEW.  It will be translated AS-IS.  View transitions will NOT honor " +
                            "target db name changes For example: `-dbp`.  VIEW creation depends on the referenced tables existing FIRST. " +
                            "VIEW creation failures may mean that all referenced tables don't exist yet.");
                } else {
                    // This is a connector table.  IE: HBase, Kafka, JDBC, etc.  We just past it through.
                    source.addIssue("This is not a NATIVE Hive table.  It will be translated 'AS-IS'.  If the libraries or dependencies required for this table definition are not available on the target cluster, the 'create' statement may fail.");
                }

                TableUtils.fixTableDefinition(target);
            }
        } catch (Exception e) {
            log.error("Error building table schema: " + e.getMessage(), e);
            source.addIssue("Error building table schema: " + e.getMessage());
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    public Boolean buildTransferSql(TableMirror tableMirror, Environment source, Environment shadow, Environment target) {
        Boolean rtn = Boolean.TRUE;
        Config config = getConfigService().getConfig();

        EnvironmentTable sourceTable = tableMirror.getEnvironmentTable(source);
        EnvironmentTable shadowTable = tableMirror.getEnvironmentTable(shadow);
        EnvironmentTable targetTable = tableMirror.getEnvironmentTable(target);

        // Build Source->Transfer SQL
        rtn = buildSourceToTransferSql(tableMirror);

        // Build Shadow->Final SQL
        if (rtn) {
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                tableMirror.getEnvironmentTable(Environment.RIGHT).addSql("distcp specified", "-- Run the Distcp output to migrate data.");
                if (sourceTable.getPartitioned()) {
                    String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, targetTable.getName());
                    targetTable.addCleanUpSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
                }
            } else if (config.getTransfer().getCommonStorage() == null) {
                rtn = buildShadowToFinalSql(tableMirror);
            }
        }
        return rtn;
    }

    protected void checkTableFilter(TableMirror tableMirror, Environment environment) {
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        Config config = getConfigService().getConfig();

        if (environment == Environment.LEFT) {
            if (config.getMigrateVIEW().isOn() && config.getDataStrategy() != DUMP) {
                if (!TableUtils.isView(et)) {
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("VIEW's only processing selected.");
                }
            } else {
                // Check if ACID for only the LEFT cluster.  If it's the RIGHT cluster, other steps will deal with
                // the conflict.  IE: Rename or exists already.
                if (TableUtils.isManaged(et)
                        && environment == Environment.LEFT) {
                    if (TableUtils.isACID(et)) {
                        // For ACID tables, check that Migrate is ON.
                        if (config.getMigrateACID().isOn()) {
                            tableMirror.addStep("TRANSACTIONAL", Boolean.TRUE);
                        } else {
                            tableMirror.setRemove(Boolean.TRUE);
                            tableMirror.setRemoveReason("ACID table and ACID processing not selected (-ma|-mao).");
                        }
                    } else if (config.getMigrateACID().isOnly()) {
                        // Non ACID Tables should NOT be process if 'isOnly' is set.
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("Non-ACID table and ACID only processing selected `-mao`");
                    }
                } else if (TableUtils.isHiveNative(et)) {
                    // Non ACID Tables should NOT be process if 'isOnly' is set.
                    if (config.getMigrateACID().isOnly()) {
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("Non-ACID table and ACID only processing selected `-mao`");
                    }
                } else if (TableUtils.isView(et)) {
                    if (config.getDataStrategy() != DUMP) {
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("This is a VIEW and VIEW processing wasn't selected.");
                    }
                } else {
                    // Non-Native Tables.
                    if (!config.isMigratedNonNative()) {
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("This is a Non-Native hive table and non-native process wasn't " +
                                "selected.");
                    }
                }
            }
        }

        // Check for tables migration flag, to avoid 're-migration'.
        String smFlag = TableUtils.getTblProperty(HMS_STORAGE_MIGRATION_FLAG, et);
        if (smFlag != null) {
            tableMirror.setRemove(Boolean.TRUE);
            tableMirror.setRemoveReason("The table has already gone through the STORAGE_MIGRATION process on " +
                    smFlag + " If this isn't correct, remove the TBLPROPERTY '" + HMS_STORAGE_MIGRATION_FLAG + "' " +
                    "from the table and try again.");
        }

        // Check for table size filter
        if (config.getFilter().getTblSizeLimit() != null && config.getFilter().getTblSizeLimit() > 0) {
            Long dataSize = (Long) et.getStatistics().get(DATA_SIZE);
            if (dataSize != null) {
                if (config.getFilter().getTblSizeLimit() * (1024 * 1024) < dataSize) {
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("The table dataset size exceeds the specified table filter size limit: " +
                            config.getFilter().getTblSizeLimit() + "Mb < " + dataSize);
                }
            }
        }

    }

    public String getCreateStatement(TableMirror tableMirror, Environment environment) {
        StringBuilder createStatement = new StringBuilder();
        Config config = getConfigService().getConfig();

        Boolean cine = config.getCluster(environment).isCreateIfNotExists();

        List<String> tblDef = tableMirror.getTableDefinition(environment);
        if (tblDef != null) {
            Iterator<String> iter = tblDef.iterator();
            while (iter.hasNext()) {
                String line = iter.next();
                if (cine && line.startsWith("CREATE TABLE")) {
                    line = line.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                } else if (cine && line.startsWith("CREATE EXTERNAL TABLE")) {
                    line = line.replace("CREATE EXTERNAL TABLE", "CREATE EXTERNAL TABLE IF NOT EXISTS");
                }
                createStatement.append(line);
                if (iter.hasNext()) {
                    createStatement.append("\n");
                }
            }
        } else {
            throw new RuntimeException("Couldn't location definition for table: " + tableMirror.getName() +
                    " in environment: " + environment.toString());
        }
        return createStatement.toString();
    }

    public void getTableDefinition(TableMirror tableMirror, Environment environment) throws SQLException {
        // The connection should already be in the database;
        Config config = getConfigService().getConfig();

        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        // Fetch Table Definition.
        if (config.isLoadingTestData()) {
            // Already loaded from before.
        } else {
            loadSchemaFromCatalog(tableMirror, environment);
        }

        checkTableFilter(tableMirror, environment);

        if (!tableMirror.isRemove() && !config.isLoadingTestData()) {
            switch (config.getDataStrategy()) {
                case SCHEMA_ONLY:
                case CONVERT_LINKED:
                case DUMP:
                case LINKED:
                    // These scenario don't require stats.
                    break;
                case SQL:
                case HYBRID:
                case EXPORT_IMPORT:
                case STORAGE_MIGRATION:
                case COMMON:
                case ACID:
                    if (!TableUtils.isView(et) && TableUtils.isHiveNative(et)) {
                        loadTableStats(tableMirror, environment);
                    }
                    break;
            }
        }

        Boolean partitioned = TableUtils.isPartitioned(et);
        if (partitioned && !tableMirror.isRemove() && !config.isLoadingTestData()) {
            /*
            If we are -epl, we need to load the partition metadata for the table. And we need to use the
            metastore_direct connection to do so. Trying to load this through the standard Hive SQL process
            is 'extremely' slow.
             */
            if (config.isEvaluatePartitionLocation() ||
                    (config.getDataStrategy() == STORAGE_MIGRATION && config.getTransfer().getStorageMigration().isDistcp())) {
                loadTablePartitionMetadataDirect(tableMirror, environment);
            } else {
                loadTablePartitionMetadata(tableMirror, environment);
            }

        }

        // Check for table partition count filter
        if (config.getFilter().getTblPartitionLimit() != null && config.getFilter().getTblPartitionLimit() > 0) {
            Integer partLimit = config.getFilter().getTblPartitionLimit();
            if (et.getPartitions().size() > partLimit) {
                tableMirror.setRemove(Boolean.TRUE);
                tableMirror.setRemoveReason("The table partition count exceeds the specified table filter partition limit: " +
                        config.getFilter().getTblPartitionLimit() + " < " + et.getPartitions().size());

            }
        }

        log.debug(environment + ":" + tableMirror.getParent().getName() + "." + et.getName() +
                ": Loaded Table Definition");
    }

    @Async("metadataThreadPool")
    public Future<ReturnStatus> getTableMetadata(TableMirror tableMirror) {
        ReturnStatus rtn = new ReturnStatus();
        rtn.setTableMirror(tableMirror);
        Config config = getConfigService().getConfig();

        log.info("Getting table definition for: " + tableMirror.getParent().getName() + "." + tableMirror.getName());
        try {
            getTableDefinition(tableMirror, Environment.LEFT);

            switch (config.getDataStrategy()) {
                case DUMP:
                    rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
                    break;
                default:
                    getTableDefinition(tableMirror, Environment.RIGHT);
                    rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
            }
        } catch (SQLException throwables) {
            log.error(throwables.getMessage(), throwables);
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(throwables);
        }
        log.info("Completed table definition for: " + tableMirror.getParent().getName() + "." + tableMirror.getName());
        return new AsyncResult<>(rtn);
    }

    @Async("metadataThreadPool")
    public Future<ReturnStatus> getTables(DBMirror dbMirror) {
        ReturnStatus rtn = new ReturnStatus();
        Config config = getConfigService().getConfig();
        log.debug("Getting tables for: " + dbMirror.getName());
        try {
            getTables(dbMirror, Environment.LEFT);
            if (config.isSync()) {
                // Get the tables on the RIGHT side.  Used to determine if a table has been dropped on the LEFT
                // and later needs to be removed on the RIGHT.
                try {
                    getTables(dbMirror, Environment.RIGHT);
                } catch (SQLException se) {
                    // OK, if the db doesn't exist yet.
                }
            }
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } catch (SQLException throwables) {
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(throwables);
        }
        return new AsyncResult<>(rtn);
    }

    public void getTables(DBMirror dbMirror, Environment environment) throws SQLException {
        Connection conn = null;
        Config config = getConfigService().getConfig();

        try {
            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
            if (conn != null) {
                String database = (environment == Environment.LEFT ?
                        dbMirror.getName() : getConfigService().getResolvedDB(dbMirror.getName()));

                log.info(environment + ":" + database + ": Loading tables for database");

                Statement stmt = null;
                ResultSet resultSet = null;
                // Stub out the tables
                try {
                    stmt = conn.createStatement();
                    log.debug(environment + ":" + database + ": Setting Hive DB Session Context");
                    stmt.execute(MessageFormat.format(MirrorConf.USE, database));
                    log.info(environment + ":" + database + ": Getting Table List");
                    List<String> shows = new ArrayList<String>();
                    if (!config.getCluster(environment).isLegacyHive()) {
                        if (config.getMigrateVIEW().isOn()) {
                            shows.add(MirrorConf.SHOW_VIEWS);
                            if (config.getDataStrategy() == DUMP) {
                                shows.add(MirrorConf.SHOW_TABLES);
                            }
                        } else {
                            shows.add(MirrorConf.SHOW_TABLES);
                        }
                    } else {
                        shows.add(MirrorConf.SHOW_TABLES);
                    }
                    for (String show : shows) {
                        resultSet = stmt.executeQuery(show);
                        while (resultSet.next()) {
                            String tableName = resultSet.getString(1);
                            if (tableName.startsWith(config.getTransfer().getTransferPrefix())) {
                                log.info("Database: " + database + " Table: " + tableName + " was NOT added to list.  " +
                                        "The name matches the transfer prefix and is most likely a remnant of a previous " +
                                        "event. If this is a mistake, change the 'transferPrefix' to something more unique.");
                            } else if (tableName.endsWith("storage_migration")) {
                                log.info("Database: " + database + " Table: " + tableName + " was NOT added to list.  " +
                                        "The name is the result of a previous STORAGE_MIGRATION attempt that has not been " +
                                        "cleaned up.");
                            } else {
                                if (config.getFilter().getTblRegEx() == null && config.getFilter().getTblExcludeRegEx() == null) {
                                    TableMirror tableMirror = dbMirror.addTable(tableName);
                                    tableMirror.setUnique(df.format(config.getInitDate()));
                                    tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                } else if (config.getFilter().getTblRegEx() != null) {
                                    // Filter Tables
                                    assert (config.getFilter().getTblFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblFilterPattern().matcher(tableName);
                                    if (matcher.matches()) {
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setUnique(df.format(config.getInitDate()));
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    } else {
                                        log.info(database + ":" + tableName + " didn't match table regex filter and " +
                                                "will NOT be added to processing list.");
                                    }
                                } else if (config.getFilter().getTblExcludeRegEx() != null) {
                                    assert (config.getFilter().getTblExcludeFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblExcludeFilterPattern().matcher(tableName);
                                    if (!matcher.matches()) { // ANTI-MATCH
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setUnique(df.format(config.getInitDate()));
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    } else {
                                        log.info(database + ":" + tableName + " matched exclude table regex filter and " +
                                                "will NOT be added to processing list.");
                                    }
                                }
                            }
                        }

                    }
                } catch (SQLException se) {
                    log.error(environment + ":" + database + " ", se);
                    // This is helpful if the user running the process doesn't have permissions.
                    dbMirror.addIssue(environment, database + " " + se.getMessage());
                } finally {
                    if (resultSet != null) {
                        try {
                            resultSet.close();
                        } catch (SQLException sqlException) {
                            // ignore
                        }
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlException) {
                            // ignore
                        }
                    }
                }
            }
        } catch (SQLException se) {
            throw se;
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    public Boolean isACIDDowngradeInPlace(TableMirror tableMirror, Environment environment) {
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        if (TableUtils.isACID(et) && getConfigService().getConfig().getMigrateACID().isDowngradeInPlace()) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public void loadSchemaFromCatalog(TableMirror tableMirror, Environment environment) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        String database = null;
        if (environment == Environment.LEFT) {
            database = tableMirror.getParent().getName();
        } else {
            database = getConfigService().getResolvedDB(tableMirror.getParent().getName());
        }
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        Config config = getConfigService().getConfig();

        try {

            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);

            if (conn != null) {
                stmt = conn.createStatement();
                log.info(environment + ":" + database + "." + tableMirror.getName() +
                        ": Loading Table Definition");
                String useStatement = MessageFormat.format(MirrorConf.USE, database);
                stmt.execute(useStatement);
                String showStatement = MessageFormat.format(MirrorConf.SHOW_CREATE_TABLE, tableMirror.getName());
                resultSet = stmt.executeQuery(showStatement);
                List<String> tblDef = new ArrayList<String>();
                ResultSetMetaData meta = resultSet.getMetaData();
                if (meta.getColumnCount() >= 1) {
                    while (resultSet.next()) {
                        try {
                            tblDef.add(resultSet.getString(1).trim());
                        } catch (NullPointerException npe) {
                            // catch and continue.
                            log.error(environment + ":" + database + "." + tableMirror.getName() +
                                    ": Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. " +
                                    "ResultSet record(line) is null. Skipping.");
                        }
                    }
                } else {
                    log.error(environment + ":" + database + "." + tableMirror.getName() +
                            ": Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. No Metadata.");
                }
                et.setDefinition(tblDef);
                et.setName(tableMirror.getName());
                // Identify that the table existed in the Database before other activity.
                et.setExists(Boolean.TRUE);
                tableMirror.addStep(environment.toString(), "Fetched Schema");

                // TODO: Don't do this is table removed from list.
                if (config.isTransferOwnership()) {
                    try {
                        String ownerStatement = MessageFormat.format(MirrorConf.SHOW_TABLE_EXTENDED, tableMirror.getName());
                        resultSet = stmt.executeQuery(ownerStatement);
                        String owner = null;
                        while (resultSet.next()) {

                            if (resultSet.getString(1).startsWith("owner")) {
                                String[] ownerLine = resultSet.getString(1).split(":");
                                try {
                                    owner = ownerLine[1];
                                } catch (Throwable t) {
                                    // Parsing issue.
                                    log.error("Couldn't parse 'owner' value from: " + resultSet.getString(1) +
                                            " for table: " + tableMirror.getParent().getName() + "." + tableMirror.getName());
                                }
                                break;
                            }
                        }
                        if (owner != null) {
                            et.setOwner(owner);
                        }
                    } catch (SQLException sed) {
                        // Failed to gather owner details.
                    }
                }

            }
        } catch (SQLException throwables) {
            if (throwables.getMessage().contains("Table not found") || throwables.getMessage().contains("Database does not exist")) {
                // This is ok in the upper cluster where we don't expect the table to exist if the process hadn't run before.
                tableMirror.addStep(environment.toString(), "No Schema");
            } else {
                throwables.printStackTrace();
                et.addIssue(throwables.getMessage());
            }
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }

        }
    }

    protected void loadTableOwnership(TableMirror tableMirror, Environment environment) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        Config config = getConfigService().getConfig();

        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        if (config.isTransferOwnership()) {
            try {
                conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
                if (conn != null) {
                    stmt = conn.createStatement();

                    try {
                        String ownerStatement = MessageFormat.format(MirrorConf.SHOW_TABLE_EXTENDED, tableMirror.getName());
                        resultSet = stmt.executeQuery(ownerStatement);
                        String owner = null;
                        while (resultSet.next()) {

                            if (resultSet.getString(1).startsWith("owner")) {
                                String[] ownerLine = resultSet.getString(1).split(":");
                                try {
                                    owner = ownerLine[1];
                                } catch (Throwable t) {
                                    // Parsing issue.
                                    log.error("Couldn't parse 'owner' value from: " + resultSet.getString(1) +
                                            " for table: " + tableMirror.getParent().getName() + "." + tableMirror.getName());
                                }
                                break;
                            }
                        }
                        if (owner != null) {
                            et.setOwner(owner);
                        }
                    } catch (SQLException sed) {
                        // Failed to gather owner details.
                    }

                }
            } catch (SQLException throwables) {
                if (throwables.getMessage().contains("Table not found") || throwables.getMessage().contains("Database does not exist")) {
                    // This is ok in the upper cluster where we don't expect the table to exist if the process hadn't run before.
                    tableMirror.addStep(environment.toString(), "No Schema");
                } else {
                    throwables.printStackTrace();
                    et.addIssue(throwables.getMessage());
                }
            } finally {
                if (resultSet != null) {
                    try {
                        resultSet.close();
                    } catch (SQLException sqlException) {
                        // ignore
                    }
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlException) {
                        // ignore
                    }
                }
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
            }
        }
    }

    protected void loadTablePartitionMetadata(TableMirror tableMirror, Environment environment) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        String database = tableMirror.getParent().getName();
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        try {
            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
            if (conn != null) {

                stmt = conn.createStatement();
                log.debug(environment + ":" + database + "." + et.getName() +
                        ": Loading Partitions");

                resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_PARTITIONS, database, et.getName()));
                Map<String, String> partDef = new HashMap<String, String>();
                while (resultSet.next()) {
                    partDef.put(resultSet.getString(1), NOT_SET);
                }
                et.setPartitions(partDef);

            }
        } catch (SQLException throwables) {
            et.addIssue(throwables.getMessage());
            log.error(environment + ":" + database + "." + et.getName() +
                    ": Issue loading Partitions.", throwables);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    protected void loadTablePartitionMetadataDirect(TableMirror tableMirror, Environment environment) {
        /*
        1. Get Metastore Direct Connection
        2. Get Query Definitions
        3. Get Query for 'part_locations'
        4. Execute Query
        5. Load Partition Data
         */
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        Config config = getConfigService().getConfig();
        String database = tableMirror.getParent().getName();
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        try {
            conn = getConnectionPoolService().getMetastoreDirectEnvironmentConnection(environment);
            log.debug(environment + ":" + database + "." + et.getName() +
                    ": Loading Partitions from Metastore Direct Connection.");
            QueryDefinitions queryDefinitions = getQueryDefinitionsService().getQueryDefinitions(environment);
            if (queryDefinitions != null) {
                String partLocationQuery = queryDefinitions.getQueryDefinition("part_locations").getStatement();
                pstmt = conn.prepareStatement(partLocationQuery);
                pstmt.setString(1, database);
                pstmt.setString(2, et.getName());
                resultSet = pstmt.executeQuery();
                Map<String, String> partDef = new HashMap<String, String>();
                while (resultSet.next()) {
                    partDef.put(resultSet.getString(1), resultSet.getString(2));
                }
                et.setPartitions(partDef);
            }
            log.debug(environment + ":" + database + "." + et.getName() +
                    ": Loaded Partitions from Metastore Direct Connection.");
        } catch (SQLException throwables) {
            et.addIssue(throwables.getMessage());
            log.error(environment + ":" + database + "." + et.getName() +
                    ": Issue loading Partitions from Metastore Direct Connection.", throwables);
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    protected void loadTableStats(TableMirror tableMirror, Environment environment) throws SQLException {
        // Considered only gathering stats for partitioned tables, but decided to gather for all tables to support
        //  smallfiles across the board.
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        Config config = getConfigService().getConfig();

        if (config.getOptimization().isSkipStatsCollection()) {
            log.debug(environment + ":" + et.getName() + ": Skipping Stats Collection.");
            return;
        }
        switch (config.getDataStrategy()) {
            case DUMP:
            case SCHEMA_ONLY:
                // We don't need stats for these.
                return;
            case STORAGE_MIGRATION:
                if (config.getTransfer().getStorageMigration().isDistcp()) {
                    // We don't need stats for this.
                    return;
                }
                break;
            default:
                break;
        }

        // Determine File sizes in table or partitions.
        /*
        - Get Base location for table
        - Get HadoopSession
        - Do a 'count' of the location.
         */
        String location = TableUtils.getLocation(et.getName(), et.getDefinition());
        // Only run checks against hdfs and ozone namespaces.
        String[] locationParts = location.split(":");
        String protocol = locationParts[0];
        if (config.getSupportFileSystems().contains(protocol)) {
            HadoopSession cli = null;
            try {
                cli = config.getCliPool().borrow();
                String countCmd = "count " + location;
                CommandReturn cr = cli.processInput(countCmd);
                if (!cr.isError() && cr.getRecords().size() == 1) {
                    // We should only get back one record.
                    List<Object> countRecord = cr.getRecords().get(0);
                    // 0 = Folder Count
                    // 1 = File Count
                    // 2 = Size Summary
                    try {
                        Double avgFileSize = (double) (Long.valueOf(countRecord.get(2).toString()) /
                                Integer.valueOf(countRecord.get(1).toString()));
                        et.getStatistics().put(DIR_COUNT, Integer.valueOf(countRecord.get(0).toString()));
                        et.getStatistics().put(FILE_COUNT, Integer.valueOf(countRecord.get(1).toString()));
                        et.getStatistics().put(DATA_SIZE, Long.valueOf(countRecord.get(2).toString()));
                        et.getStatistics().put(AVG_FILE_SIZE, avgFileSize);
                        et.getStatistics().put(TABLE_EMPTY, Boolean.FALSE);
                    } catch (ArithmeticException ae) {
                        // Directory is probably empty.
                        et.getStatistics().put(TABLE_EMPTY, Boolean.TRUE);
                    }
                } else {
                    // Issue getting count.

                }
            } finally {
                if (cli != null) {
                    config.getCliPool().returnSession(cli);
                }
            }
        }
        // Determine Table File Format
        TableUtils.getSerdeType(et);
    }

    /**
     * From this cluster, run the SQL built up in the tblMirror(environment)
     *
     * @param tblMirror
     * @param environment Allows to override cluster environment
     * @return
     */
    public Boolean runTableSql(TableMirror tblMirror, Environment environment) {
        Connection conn = null;
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable et = tblMirror.getEnvironmentTable(environment);

        rtn = runTableSql(et.getSql(), tblMirror, environment);

        return rtn;
    }

    public Boolean runTableSql(List<Pair> sqlList, TableMirror tblMirror, Environment environment) {
        Connection conn = null;
        Boolean rtn = Boolean.TRUE;
        Config config = getConfigService().getConfig();

        // Skip this if using test data.
        if (!getConfigService().getConfig().isLoadingTestData()) {

            try {
                // conn will be null if config.execute != true.
                conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);

                if (conn == null && config.isExecute() && !config.getCluster(environment).getHiveServer2().isDisconnected()) {
                    // this is a problem.
                    rtn = Boolean.FALSE;
                    tblMirror.addIssue(environment, "Connection missing. This is a bug.");
                }

                if (conn == null && config.getCluster(environment).getHiveServer2().isDisconnected()) {
                    tblMirror.addIssue(environment, "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                            "The scripts will need to be run 'manually'.");
                }

                if (conn != null) {
                    Statement stmt = null;
                    try {
                        stmt = conn.createStatement();
                        for (Pair pair : sqlList) {
                            log.debug(environment + ":SQL:" + pair.getDescription() + ":" + pair.getAction());
                            tblMirror.setMigrationStageMessage("Executing SQL: " + pair.getDescription());
                            if (config.isExecute()) {
                                stmt.execute(pair.getAction());
                                tblMirror.addStep(config.toString(), "Sql Run Complete for: " + pair.getDescription());
                            } else {
                                tblMirror.addStep(config.toString(), "Sql Run SKIPPED (DRY-RUN) for: " + pair.getDescription());
                            }
                        }
                    } catch (SQLException throwables) {
                        log.error(environment.toString() + ":" + throwables.getMessage(), throwables);
                        String message = throwables.getMessage();
                        if (throwables.getMessage().contains("HiveAccessControlException Permission denied")) {
                            message = message + " See [Hive SQL Exception / HDFS Permissions Issues](https://github.com/cloudera-labs/hms-mirror#hive-sql-exception--hdfs-permissions-issues)";
                        }
                        if (throwables.getMessage().contains("AvroSerdeException")) {
                            message = message + ". It's possible the `avro.schema.url` referenced file doesn't exist at the target. " +
                                    "Use the `-asm` option and hms-mirror will attempt to copy it to the new cluster.";
                        }
                        tblMirror.getEnvironmentTable(environment).addIssue(message);
                        rtn = Boolean.FALSE;
                    } finally {
                        if (stmt != null) {
                            try {
                                stmt.close();
                            } catch (SQLException sqlException) {
                                // ignore
                            }
                        }
                    }
                }
            } catch (SQLException throwables) {
                tblMirror.getEnvironmentTable(environment).addIssue("Connecting: " + throwables.getMessage());
                log.error(environment.toString() + ":" + throwables.getMessage(), throwables);
                rtn = Boolean.FALSE;
            } finally {
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
            }
        }
        return rtn;
    }

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    @Autowired
    public void setQueryDefinitionsService(QueryDefinitionsService queryDefinitionsService) {
        this.queryDefinitionsService = queryDefinitionsService;
    }

    @Autowired
    public void setStatsCalculatorService(StatsCalculatorService statsCalculatorService) {
        this.statsCalculatorService = statsCalculatorService;
    }

    @Autowired
    public void setTranslatorService(TranslatorService translatorService) {
        this.translatorService = translatorService;
    }

}
