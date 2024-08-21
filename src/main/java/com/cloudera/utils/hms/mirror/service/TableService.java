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

//import com.cloudera.utils.hadoop.HadoopSession;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.cloudera.utils.hms.mirror.CopySpec;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
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

import java.sql.Connection;
import java.sql.*;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.Future;
import java.util.regex.Matcher;

import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION;
import static com.cloudera.utils.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION_THRESHOLD;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.*;
import static com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.DUMP;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Getter
@Setter
@Slf4j
public class TableService {
    private final DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
    private final DateFormat tdf = new SimpleDateFormat("HH:mm:ss.SSS");

    private ConfigService configService;
    private ExecuteSessionService executeSessionService;
    private ConnectionPoolService connectionPoolService;
    private QueryDefinitionsService queryDefinitionsService;
    private TranslatorService translatorService;
    private StatsCalculatorService statsCalculatorService;

//    protected HmsMirrorConfig getConfig() {
//        return getExecuteSessionService().getHmsMirrorConfig();
//    }

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    protected Boolean buildShadowToFinalSql(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        // if common storage, skip
        // if inplace, skip
        EnvironmentTable source = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable shadow = tableMirror.getEnvironmentTable(Environment.SHADOW);
        EnvironmentTable target = tableMirror.getEnvironmentTable(Environment.RIGHT);
        if ((!TableUtils.isACID(source) && !isBlank(hmsMirrorConfig.getTransfer().getTargetNamespace())) ||
                isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
            // Nothing to build.
            return rtn;
        } else {
            if (source.getPartitioned()) {
                // MSCK repair on Shadow
                String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, shadow.getName());
                target.addSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
            }
            if (hmsMirrorConfig.getOptimization().isBuildShadowStatistics()) {
                // Build Shadow Stats.

            }
            // Set Override Properties.
            if (hmsMirrorConfig.getOptimization().getOverrides() != null) {
                for (String key : hmsMirrorConfig.getOptimization().getOverrides().getRight().keySet()) {
                    target.addSql("Setting " + key, "set " + key + "=" + hmsMirrorConfig.getOptimization().getOverrides().getRight().get(key));
                }
            }
            // Sql from Shadow to Final
            statsCalculatorService.setSessionOptions(hmsMirrorConfig.getCluster(Environment.RIGHT), source, target);
            if (source.getPartitioned()) {
                if (hmsMirrorConfig.getOptimization().isSkip()) {
                    if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                    }
                    String partElement = TableUtils.getPartitionElements(source);
                    String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                            shadow.getName(), target.getName(), partElement);
                    String shadowDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, source.getPartitions().size());
                    target.addSql(new Pair(shadowDesc, shadowSql));
                } else if (hmsMirrorConfig.getOptimization().isSortDynamicPartitionInserts()) {
                    if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
                        if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
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
                    if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                        if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
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
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable source, transfer, target;
        source = tableMirror.getEnvironmentTable(Environment.LEFT);
        transfer = tableMirror.getEnvironmentTable(Environment.TRANSFER);
        target = tableMirror.getEnvironmentTable(Environment.RIGHT);
        if (TableUtils.isACID(source)) {
            if (source.getPartitions().size() > hmsMirrorConfig.getMigrateACID().getPartitionLimit() && hmsMirrorConfig.getMigrateACID().getPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                source.addIssue("The number of partitions: " + source.getPartitions().size() + " exceeds the configuration " +
                        "limit (migrateACID->partitionLimit) of " + hmsMirrorConfig.getMigrateACID().getPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ap'.");
                rtn = Boolean.FALSE;
            }
        } else {
            if (source.getPartitions().size() > hmsMirrorConfig.getHybrid().getSqlPartitionLimit() &&
                    hmsMirrorConfig.getHybrid().getSqlPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                source.addIssue("The number of partitions: " + source.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->sqlPartitionLimit) of " + hmsMirrorConfig.getHybrid().getSqlPartitionLimit() +
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
//        if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
//            source.addSql("distcp specified", "-- Run distcp commands");
//        } else {
        // Set Override Properties.
        if (hmsMirrorConfig.getOptimization().getOverrides() != null) {
            for (String key : hmsMirrorConfig.getOptimization().getOverrides().getLeft().keySet()) {
                source.addSql("Setting " + key, "set " + key + "=" + hmsMirrorConfig.getOptimization().getOverrides().getLeft().get(key));
            }
        }

        if (transfer.isDefined()) {
            statsCalculatorService.setSessionOptions(hmsMirrorConfig.getCluster(Environment.LEFT), source, source);
            if (source.getPartitioned()) {
                if (hmsMirrorConfig.getOptimization().isSkip()) {
                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                        source.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                    }
                    String partElement = TableUtils.getPartitionElements(source);
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                            source.getName(), transfer.getName(), partElement);
                    String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, source.getPartitions().size());
                    source.addSql(new Pair(transferDesc, transferSql));
                } else if (hmsMirrorConfig.getOptimization().isSortDynamicPartitionInserts()) {
                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                        source.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
                        if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                            source.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                        }
                    }
                    String partElement = TableUtils.getPartitionElements(source);
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                            source.getName(), transfer.getName(), partElement);
                    String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, source.getPartitions().size());
                    source.addSql(new Pair(transferDesc, transferSql));
                } else {
                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                        source.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                        if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
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
//        }
        if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
            source.addSql("distcp specified", "-- Run distcp commands");
        }
        return rtn;
    }

    public Boolean buildTableSchema(CopySpec copySpec) throws RequiredConfigurationException {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        TableMirror tableMirror = copySpec.getTableMirror();
        Boolean rtn = Boolean.TRUE;

        EnvironmentTable source = tableMirror.getEnvironmentTable(copySpec.getSource());
        EnvironmentTable target = tableMirror.getEnvironmentTable(copySpec.getTarget());

        // Since we are building the cluster on request, we need to build the transfer cluster if they don't exist.
        if (isNull(config.getCluster(copySpec.getTarget()))) {
            Cluster intermediateCluster = null;
            if (copySpec.getTarget() == Environment.SHADOW) {
                intermediateCluster = config.getCluster(Environment.LEFT).clone();
            } else if (copySpec.getTarget() == Environment.TRANSFER) {
                intermediateCluster = config.getCluster(Environment.RIGHT).clone();
            } else if (copySpec.getTarget() == Environment.RIGHT) {
                // This can happen when the strategy is a single cluster effort BUT we're using the RIGHT
                // Cluster as an intermediate holder.
                intermediateCluster = config.getCluster(Environment.LEFT).clone();
            }
            config.getClusters().put(copySpec.getTarget(), intermediateCluster);
        }

        try {
            // Set Table Name
            if (source.isExists()) {
                target.setName(source.getName());

                // Clear the target spec.
                target.getDefinition().clear();
                // Reset with Source
                target.getDefinition().addAll(source.getDefinition());
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
                        if (config.loadMetadataDetails()) {
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
//                                if (config.getFilter().isTableFiltering()) {
//                                    level = 0;
//                                }
                                String targetLocation = null;
                                targetLocation = getTranslatorService().
                                        translateLocation(tableMirror, sourceLocation, level, null);
                                if (!TableUtils.updateTableLocation(target, targetLocation)) {
                                    rtn = Boolean.FALSE;
                                }
                                // Check if the locations align.  If not, warn.
//                                if (config.getTransfer().getWarehouse().getExternalDirectory() != null &&
//                                        config.getTransfer().getWarehouse().getManagedDirectory() != null) {

//     THESE CHECKS ARE DONE ALREADY in the TranslatorService.translateTableLocation method.
//                                if (TableUtils.isExternal(target)) {
//                                    // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
//                                    String lclLoc = tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
//                                    if (!isBlank(lclLoc) && !targetLocation.startsWith(lclLoc)) {
//                                        // Set warning that even though you've specified to warehouse directories, the current configuration
//                                        // will NOT place it in that directory.
//                                        String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
//                                                lclLoc, targetLocation);
//                                        tableMirror.addIssue(Environment.RIGHT, msg);
//                                    }
//                                } else {
//                                    String lclLoc = tableMirror.getParent().getProperty(Environment.RIGHT, DB_MANAGED_LOCATION);
//                                    if (!isBlank(lclLoc) && !targetLocation.startsWith(lclLoc)) {
//                                        // Set warning that even though you've specified to warehouse directories, the current configuration
//                                        // will NOT place it in that directory.
//                                        String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
//                                                lclLoc, targetLocation);
//                                        tableMirror.addIssue(Environment.RIGHT, msg);
//                                    }
//
//                                }
//                                }

                            }

                            if (tableMirror.isReMapped()) {
                                target.addIssue(MessageCode.TABLE_LOCATION_REMAPPED.getDesc());
                            } else if (config.getTranslator().isForceExternalLocation()) {
                                target.addIssue(MessageCode.TABLE_LOCATION_FORCED.getDesc());
                            } else if (config.loadMetadataDetails()) {
                                TableUtils.stripLocation(target);
                                target.addIssue(MessageCode.ALIGN_LOCATIONS_WARNING.getDesc());
                            }
                            break;
                        case SHADOW:
                        case TRANSFER:
                            if (nonNull(copySpec.getLocation())) {
                                if (!TableUtils.updateTableLocation(target, copySpec.getLocation())) {
                                    rtn = Boolean.FALSE;
                                }
                            } else if (copySpec.isReplaceLocation()) {
                                if (nonNull(config.getTransfer().getIntermediateStorage())) {
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
                                } else if (nonNull(config.getTransfer().getTargetNamespace())) {
                                    String sourceLocation = TableUtils.getLocation(tableMirror.getName(), tableMirror.getTableDefinition(copySpec.getSource()));
                                    String targetLocation = getTranslatorService().
                                            translateLocation(tableMirror, sourceLocation, 1, null);
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

                    // Check and convert Partition Locations.
                    if (config.loadMetadataDetails() &&
                            source.getPartitioned()) {
                        if (!TableUtils.isACID(source)) {
                            // New Map.  So we can modify it..
                            Map<String, String> targetPartitions = new HashMap<>(source.getPartitions());
                            target.setPartitions(targetPartitions);
                            if (!getTranslatorService().translatePartitionLocations(tableMirror)) {
                                rtn = Boolean.FALSE;
                            }
                        }
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
                            log.debug("Table: {} - Checking Feature: {}", tableMirror.getName(), features);
                            if (feature.fixSchema(target)) {
                                log.debug("Table: {} - Feature Applicable: {}", tableMirror.getName(), features);
                                target.addIssue("Feature (" + features + ") was found applicable and adjustments applied. " +
                                        feature.getDescription());
                            } else {
                                log.debug("Table: {} - Feature NOT Applicable: {}", tableMirror.getName(), features);
                            }
                        }
                    } else {
                        log.debug("Table: {} - Skipping Features Check...", tableMirror.getName());
                    }

                    if (config.isTranslateLegacy()) {
                        if (config.getLegacyTranslations().fixSchema(target)) {
                            log.info("Legacy Translation applied to: {}:{}", tableMirror.getParent().getName(), target.getName());
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
        } catch (MismatchException e) {
            log.error("Error building table schema: {}", e.getMessage(), e);
            source.addIssue("Error building table schema: " + e.getMessage());
            rtn = Boolean.FALSE;
        } catch (MissingDataPointException e) {
            log.error(MessageCode.ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS.getDesc(), e);
            source.addIssue(MessageCode.ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS.getDesc());
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    public Boolean buildTransferSql(TableMirror tableMirror, Environment source, Environment shadow, Environment target) {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable sourceTable = tableMirror.getEnvironmentTable(source);
        EnvironmentTable shadowTable = tableMirror.getEnvironmentTable(shadow);
        EnvironmentTable targetTable = tableMirror.getEnvironmentTable(target);

        // Build Source->Transfer SQL
        rtn = buildSourceToTransferSql(tableMirror);

        // Build Shadow->Final SQL
        if (rtn) {
            if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                tableMirror.getEnvironmentTable(Environment.RIGHT).addSql("distcp specified", "-- Run the Distcp output to migrate data.");
                if (sourceTable.getPartitioned()) {
                    String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, targetTable.getName());
                    targetTable.addCleanUpSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
                }
            } else if (isBlank(hmsMirrorConfig.getTransfer().getTargetNamespace())) {
                rtn = buildShadowToFinalSql(tableMirror);
            }
        }
        return rtn;
    }

    protected void checkTableFilter(TableMirror tableMirror, Environment environment) {
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig hmsMirrorConfig = session.getConfig();
        RunStatus runStatus = session.getRunStatus();
        OperationStatistics stats = runStatus.getOperationStatistics();

        if (environment == Environment.LEFT) {
            if (hmsMirrorConfig.getMigrateVIEW().isOn() && hmsMirrorConfig.getDataStrategy() != DUMP) {
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
                        if (hmsMirrorConfig.getMigrateACID().isOn()) {
                            tableMirror.addStep("TRANSACTIONAL", Boolean.TRUE);
                        } else {
                            tableMirror.setRemove(Boolean.TRUE);
                            tableMirror.setRemoveReason("ACID table and ACID processing not selected (-ma|-mao).");
                        }
                    } else if (hmsMirrorConfig.getMigrateACID().isOnly()) {
                        // Non ACID Tables should NOT be process if 'isOnly' is set.
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("Non-ACID table and ACID only processing selected `-mao`");
                    }
                } else if (TableUtils.isHiveNative(et)) {
                    // Non ACID Tables should NOT be process if 'isOnly' is set.
                    if (hmsMirrorConfig.getMigrateACID().isOnly()) {
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("Non-ACID table and ACID only processing selected `-mao`");
                    }
                } else if (TableUtils.isView(et)) {
                    if (hmsMirrorConfig.getDataStrategy() != DUMP) {
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("This is a VIEW and VIEW processing wasn't selected.");
                    }
                } else {
                    // Non-Native Tables.
                    if (!hmsMirrorConfig.isMigratedNonNative()) {
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
        if (hmsMirrorConfig.getFilter().getTblSizeLimit() != null && hmsMirrorConfig.getFilter().getTblSizeLimit() > 0) {
            Long dataSize = (Long) et.getStatistics().get(DATA_SIZE);
            if (dataSize != null) {
                if (hmsMirrorConfig.getFilter().getTblSizeLimit() * (1024 * 1024) < dataSize) {
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("The table dataset size exceeds the specified table filter size limit: " +
                            hmsMirrorConfig.getFilter().getTblSizeLimit() + "Mb < " + dataSize);
                }
            }
        }

    }

    public String getCreateStatement(TableMirror tableMirror, Environment environment) {
        StringBuilder createStatement = new StringBuilder();
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        Boolean cine = hmsMirrorConfig.getCluster(environment).isCreateIfNotExists();

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
        // The connections should already be in the database;
        log.debug("Getting table definition for {}:{}.{}",
                environment, tableMirror.getParent().getName(), tableMirror.getName());
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        // Fetch Table Definition.
        if (hmsMirrorConfig.isLoadingTestData()) {
            // Already loaded from before.
        } else {
            loadSchemaFromCatalog(tableMirror, environment);
        }

        checkTableFilter(tableMirror, environment);

        if (!tableMirror.isRemove() && !hmsMirrorConfig.isLoadingTestData()) {
            switch (hmsMirrorConfig.getDataStrategy()) {
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
                        try {
                            loadTableStats(tableMirror, environment);
                        } catch (DisabledException e) {
                            log.warn("Stats collection is disabled because the CLI Interface has been disabled. " +
                                    " Skipping stats collection for table: {}", et.getName());
//                            throw new RuntimeException(e);
                        }
                    }
                    break;
            }
        }

        Boolean partitioned = TableUtils.isPartitioned(et);
        if (environment == Environment.LEFT && partitioned
                && !tableMirror.isRemove() && !hmsMirrorConfig.isLoadingTestData()) {
            /*
            If we are -epl, we need to load the partition metadata for the table. And we need to use the
            metastore_direct connections to do so. Trying to load this through the standard Hive SQL process
            is 'extremely' slow.
             */
            if (hmsMirrorConfig.loadMetadataDetails()) {
                loadTablePartitionMetadataDirect(tableMirror, environment);
            }
        }

        // Check for table partition count filter
        if (hmsMirrorConfig.getFilter().getTblPartitionLimit() != null && hmsMirrorConfig.getFilter().getTblPartitionLimit() > 0) {
            Integer partLimit = hmsMirrorConfig.getFilter().getTblPartitionLimit();
            if (et.getPartitions().size() > partLimit) {
                tableMirror.setRemove(Boolean.TRUE);
                tableMirror.setRemoveReason("The table partition count exceeds the specified table filter partition limit: " +
                        hmsMirrorConfig.getFilter().getTblPartitionLimit() + " < " + et.getPartitions().size());

            }
        }
        log.info("Completed table definition for {}:{}.{}",
                environment, tableMirror.getParent().getName(), tableMirror.getName());
    }

    @Async("metadataThreadPool")
    public Future<ReturnStatus> getTableMetadata(TableMirror tableMirror) {
        ReturnStatus rtn = new ReturnStatus();
        rtn.setTableMirror(tableMirror);
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();

        try {
            getTableDefinition(tableMirror, Environment.LEFT);
            if (tableMirror.isRemove()) {
                rtn.setStatus(ReturnStatus.Status.SKIP);
//                runStatus.getOperationStatistics().getSkipped().incrementTables();
                return new AsyncResult<>(rtn);
            } else {
                switch (hmsMirrorConfig.getDataStrategy()) {
                    case DUMP:
                    case STORAGE_MIGRATION:
                        // Make a clone of the left as a working copy.
                        try {
                            tableMirror.getEnvironments().put(Environment.RIGHT, tableMirror.getEnvironmentTable(Environment.LEFT).clone());
                        } catch (CloneNotSupportedException e) {
                            throw new RuntimeException(e);
                        }
                        rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
                        break;
                    default:
                        getTableDefinition(tableMirror, Environment.RIGHT);
                        rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
                }
//                runStatus.getOperationStatistics().getSuccesses().incrementTables();
            }
        } catch (SQLException throwables) {
            log.error(throwables.getMessage(), throwables);
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(throwables);
//            runStatus.getOperationStatistics().getFailures().incrementTables();
        }
        return new AsyncResult<>(rtn);
    }

    @Async("metadataThreadPool")
    public Future<ReturnStatus> getTables(DBMirror dbMirror) {
        ReturnStatus rtn = new ReturnStatus();
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig hmsMirrorConfig = session.getConfig();
        RunStatus runStatus = session.getRunStatus();
        log.debug("Getting tables for Database {}", dbMirror.getName());
        try {
            getTables(dbMirror, Environment.LEFT);
            if (hmsMirrorConfig.isSync()) {
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
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        RunStatus runStatus = session.getRunStatus();
        OperationStatistics stats = runStatus.getOperationStatistics();

        try {
            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
            if (conn != null) {
                String database = (environment == Environment.LEFT ?
                        dbMirror.getName() : HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config));

                log.info("Loading tables for {}:{}", environment, database);

                Statement stmt = null;
                ResultSet resultSet = null;
                // Stub out the tables
                try {
                    stmt = conn.createStatement();
                    log.debug("Setting Hive DB Session Context {}:{}", environment, database);
                    stmt.execute(MessageFormat.format(MirrorConf.USE, database));
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
                                TableMirror tableMirror = dbMirror.addTable(tableName);
                                tableMirror.setRemove(Boolean.TRUE);
                                tableMirror.setRemoveReason("Table name matches the transfer prefix.  " +
                                        "This is most likely a remnant of a previous event.  If this is a mistake, " +
                                        "change the 'transferPrefix' to something more unique.");
                                log.info("{}.{} was NOT added to list.  " +
                                        "The name matches the transfer prefix and is most likely a remnant of a previous " +
                                        "event. If this is a mistake, change the 'transferPrefix' to something more unique.", database, tableName);
                            } else if (tableName.endsWith("storage_migration")) {
                                TableMirror tableMirror = dbMirror.addTable(tableName);
                                tableMirror.setRemove(Boolean.TRUE);
                                tableMirror.setRemoveReason("Table name matches the storage_migration suffix.  " +
                                        "This is most likely a remnant of a previous event.  If this is a mistake, " +
                                        "change the 'transferPrefix' to something more unique.");
                                log.info("{}.{} was NOT added to list.  " +
                                        "The name is the result of a previous STORAGE_MIGRATION attempt that has not been " +
                                        "cleaned up.", database, tableName);
                            } else {
                                if (isBlank(config.getFilter().getTblRegEx()) && isBlank(config.getFilter().getTblExcludeRegEx())) {
                                    TableMirror tableMirror = dbMirror.addTable(tableName);
//                                    stats.getCounts().incrementTables();
                                    tableMirror.setUnique(df.format(config.getInitDate()));
                                    tableMirror.setMigrationStageMessage("Added to evaluation inventory");
//                                    runStatus.getOperationStatistics().getCounts().incrementTables();
                                } else if (!isBlank(config.getFilter().getTblRegEx())) {
                                    // Filter Tables
                                    assert (config.getFilter().getTblFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblFilterPattern().matcher(tableName);
//                                    stats.getCounts().incrementTables();
                                    if (matcher.matches()) {
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setUnique(df.format(config.getInitDate()));
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    } else {
//                                        stats.getSkipped().incrementTables();
                                        log.info("{}.{} didn't match table regex filter and " +
                                                "will NOT be added to processing list.", database, tableName);
                                    }
                                } else if (config.getFilter().getTblExcludeRegEx() != null) {
                                    assert (config.getFilter().getTblExcludeFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblExcludeFilterPattern().matcher(tableName);
//                                    stats.getCounts().incrementTables();
                                    if (!matcher.matches()) { // ANTI-MATCH
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setUnique(df.format(config.getInitDate()));
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    } else {
//                                        stats.getSkipped().incrementTables();
                                        log.info("{}.{} matched exclude table regex filter and " +
                                                "will NOT be added to processing list.", database, tableName);
                                    }
                                }
                            }
                        }
                    }
                } catch (SQLException se) {
                    log.error("{}:{} ", environment, database, se);
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
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        if (TableUtils.isACID(et) && hmsMirrorConfig.getMigrateACID().isDowngradeInPlace()) {
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
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        if (environment == Environment.LEFT) {
            database = tableMirror.getParent().getName();
        } else {
            database = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);
        }
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);

        try {

            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);

            if (conn != null) {
                stmt = conn.createStatement();
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
                            log.error("Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. " +
                                            "ResultSet record(line) is null. Skipping. {}:{}.{}",
                                    environment, database, tableMirror.getName());
                        }
                    }
                } else {
                    log.error("Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. No Metadata. {}:{}.{}"
                            , environment, database, tableMirror.getName());
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
                                    log.error("Couldn't parse 'owner' value from: {} for table {}:{}.{}"
                                            , resultSet.getString(1), environment, database, tableMirror.getName());
//                                            " for table: " + tableMirror.getParent().getName() + "." + tableMirror.getName());
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
                log.error(throwables.getMessage(), throwables);
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
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        if (hmsMirrorConfig.isTransferOwnership()) {
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
                                    log.error("Couldn't parse 'owner' value from: {} for table: {}.{}", resultSet.getString(1), tableMirror.getParent().getName(), tableMirror.getName());
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
                    log.error(throwables.getMessage(), throwables);
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
                log.debug("{}:{}.{}: Loading Partitions", environment, database, et.getName());

                resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_PARTITIONS, database, et.getName()));
                Map<String, String> partDef = new HashMap<String, String>();
                while (resultSet.next()) {
                    partDef.put(resultSet.getString(1), NOT_SET);
                }
                et.setPartitions(partDef);

            }
        } catch (SQLException throwables) {
            et.addIssue(throwables.getMessage());
            log.error("{}:{}.{}: Issue loading Partitions.", environment, database, et.getName(), throwables);
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
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        String database = tableMirror.getParent().getName();
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        try {
            conn = getConnectionPoolService().getMetastoreDirectEnvironmentConnection(environment);
            log.info("Loading Partitions from Metastore Direct Connection {}:{}.{}", environment, database, et.getName());
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
            log.info("Loaded Partitions from Metastore Direct Connection {}:{}.{}", environment, database, et.getName());
        } catch (SQLException throwables) {
            et.addIssue(throwables.getMessage());
            log.error("Issue loading Partitions from Metastore Direct Connection. {}:{}.{}", environment, database, et.getName());
            log.error(throwables.getMessage(), throwables);
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    protected void loadTableStats(TableMirror tableMirror, Environment environment) throws DisabledException {
        // Considered only gathering stats for partitioned tables, but decided to gather for all tables to support
        //  smallfiles across the board.
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        if (hmsMirrorConfig.getOptimization().isSkipStatsCollection()) {
            log.debug("{}:{}: Skipping Stats Collection.", environment, et.getName());
            return;
        }
        switch (hmsMirrorConfig.getDataStrategy()) {
            case DUMP:
            case SCHEMA_ONLY:
                // We don't need stats for these.
                return;
            case STORAGE_MIGRATION:
                if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
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
        if (hmsMirrorConfig.getSupportFileSystems().contains(protocol)) {
            CliEnvironment cli = executeSessionService.getCliEnvironment();

            String countCmd = "count " + location;
            CommandReturn cr = cli.processInput(countCmd);
            if (!cr.isError() && cr.getRecords().size() == 1) {
                // We should only get back one record.
                List<Object> countRecord = cr.getRecords().get(0);
                // 0 = Folder Count
                // 1 = File Count
                // 2 = Size Summary
                try {
                    Double avgFileSize = (double) (Long.parseLong(countRecord.get(2).toString()) /
                            Integer.parseInt(countRecord.get(1).toString()));
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
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        // Skip this if using test data.
        if (!hmsMirrorConfig.isLoadingTestData()) {

            try {
                // conn will be null if config.execute != true.
                conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);

                if (isNull(conn) && hmsMirrorConfig.isExecute() && !hmsMirrorConfig.getCluster(environment).getHiveServer2().isDisconnected()) {
                    // this is a problem.
                    rtn = Boolean.FALSE;
                    tblMirror.addIssue(environment, "Connection missing. This is a bug.");
                }

                if (isNull(conn) && hmsMirrorConfig.getCluster(environment).getHiveServer2().isDisconnected()) {
                    tblMirror.addIssue(environment, "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                            "The scripts will need to be run 'manually'.");
                }

                if (nonNull(conn)) {
                    Statement stmt = null;
                    try {
                        stmt = conn.createStatement();
                        for (Pair pair : sqlList) {
                            log.debug("{}:SQL:{}:{}", environment, pair.getDescription(), pair.getAction());
                            tblMirror.setMigrationStageMessage("Executing SQL: " + pair.getDescription());
                            if (hmsMirrorConfig.isExecute()) {
                                stmt.execute(pair.getAction());
                                tblMirror.addStep(environment.toString(), "Sql Run Complete for: " + pair.getDescription());
                            } else {
                                tblMirror.addStep(environment.toString(), "Sql Run SKIPPED (DRY-RUN) for: " + pair.getDescription());
                            }
                        }
                    } catch (SQLException throwables) {
                        log.error("{}:{}", environment.toString(), throwables.getMessage(), throwables);
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
                log.error("{}:{}", environment.toString(), throwables.getMessage(), throwables);
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
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
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
