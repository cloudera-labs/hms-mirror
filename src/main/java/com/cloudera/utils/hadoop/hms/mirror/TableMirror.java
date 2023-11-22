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

package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hadoop.hms.mirror.feature.Feature;
import com.cloudera.utils.hadoop.hms.mirror.feature.FeaturesEnum;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.DB_LOCATION;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.DB_MANAGED_LOCATION;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION_THRESHOLD;
import static com.cloudera.utils.hadoop.hms.mirror.TablePropertyVars.*;

public class TableMirror {
    private static final Logger LOG = LoggerFactory.getLogger(TableMirror.class);
    /*
    Use to indicate the tblMirror should be removed from processing, post setup.
     */
    private static DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
    @JsonIgnore
    private final String unique = UUID.randomUUID().toString().replaceAll("-", "");
    private final DateFormat tdf = new SimpleDateFormat("HH:mm:ss.SSS");
    @JsonIgnore
    private final List<Marker> steps = new ArrayList<Marker>();
    private String name;
    @JsonIgnore
    private DBMirror parent;
    private Date start = new Date();
    @JsonIgnore
    private boolean remove = Boolean.FALSE;
    @JsonIgnore
    private String removeReason = null;
    private Boolean reMapped = Boolean.FALSE;

    private DataStrategyEnum strategy = null;

    // An ordinal value that we'll increment at each phase of the process
    private AtomicInteger currentPhase = new AtomicInteger(0);
    // An ordinal value, assign when we start processing, that indicates how many phase there will be.
    private AtomicInteger totalPhaseCount = new AtomicInteger(0);

    // Caption to help identify the current phase of the effort.
    @JsonIgnore
    private String migrationStageMessage = null;

    private PhaseState phaseState = PhaseState.INIT;

    @JsonIgnore
    private Long stageDuration = 0l;

    private Map<Environment, EnvironmentTable> environments = null;

    public TableMirror() {
        addStep("init", null);
    }

    public void addIssue(Environment environment, String issue) {
        if (issue != null) {
            String scrubbedIssue = issue.replace("\n", "<br/>");
            getIssues(environment).add(scrubbedIssue);
        }
    }

    public void addStep(String key, Object value) {
        Date now = new Date();
        Long elapsed = now.getTime() - start.getTime();
        start = now; // reset
        BigDecimal secs = new BigDecimal(elapsed).divide(new BigDecimal(1000));///1000
        DecimalFormat decf = new DecimalFormat("#,###.00");
        String secStr = decf.format(secs);
        steps.add(new Marker(secStr, key, value));
    }

    public void addTableAction(Environment environment, String action) {
        List<String> tableActions = getTableActions(environment);
        tableActions.add(action);
    }

    protected Boolean buildShadowToFinalSql(Config config) {
        Boolean rtn = Boolean.TRUE;
        // if common storage, skip
        // if inplace, skip
        EnvironmentTable source = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable shadow = getEnvironmentTable(Environment.SHADOW);
        EnvironmentTable target = getEnvironmentTable(Environment.RIGHT);
        if ((!TableUtils.isACID(source) && config.getTransfer().getCommonStorage() != null) ||
                isACIDDowngradeInPlace(config, source)) {
            // Nothing to build.
            return rtn;
        } else {
            if (source.getPartitioned()) {
                // MSCK repair on Shadow
                String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, shadow.getName());
                target.addSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
            }
            if (config.getOptimization().getBuildShadowStatistics()) {
                // Build Shadow Stats.

            }
            // Set Override Properties.
            if (config.getOptimization().getOverrides() != null) {
                for (String key : config.getOptimization().getOverrides().getRight().keySet()) {
                    target.addSql("Setting " + key, "set " + key + "=" + config.getOptimization().getOverrides().getRight().get(key));
                }
            }
            // Sql from Shadow to Final
            StatsCalculator.setSessionOptions(config.getCluster(Environment.RIGHT), source, target);
            if (source.getPartitioned()) {
                if (config.getOptimization().getSkip()) {
                    if (!config.getCluster(Environment.RIGHT).getLegacyHive()) {
                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                    }
                    String partElement = TableUtils.getPartitionElements(source);
                    String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                            shadow.getName(), target.getName(), partElement);
                    String shadowDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, source.getPartitions().size());
                    target.addSql(new Pair(shadowDesc, shadowSql));
                } else if (config.getOptimization().getSortDynamicPartitionInserts()) {
                    if (!config.getCluster(Environment.RIGHT).getLegacyHive()) {
                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
                        if (!config.getCluster(Environment.RIGHT).getHdpHive3()) {
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
                    if (!config.getCluster(Environment.RIGHT).getLegacyHive()) {
                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                        if (!config.getCluster(Environment.RIGHT).getHdpHive3()) {
                            target.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                        }
                    }
                    String partElement = TableUtils.getPartitionElements(source);
                    String distPartElement = StatsCalculator.getDistributedPartitionElements(source);
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

//    public String getDbName() {
//        return parent.getName();
//    }
//
//    public String getResolvedDbName() {
//        if (resolvedDbName == null)
//            return dbName;
//        else
//            return resolvedDbName;
//    }

//    public void setResolvedDbName(String resolvedDbName) {
//        this.resolvedDbName = resolvedDbName;
//    }

    protected Boolean buildSourceToTransferSql(Config config) {
        Boolean rtn = Boolean.TRUE;
        EnvironmentTable source, transfer, target;
        source = getEnvironmentTable(Environment.LEFT);
        transfer = getEnvironmentTable(Environment.TRANSFER);
        target = getEnvironmentTable(Environment.RIGHT);
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

        if (isACIDDowngradeInPlace(config, source)) {
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
                StatsCalculator.setSessionOptions(config.getCluster(Environment.LEFT), source, source);
                if (source.getPartitioned()) {
                    if (config.getOptimization().getSkip()) {
                        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                            source.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                        }
                        String partElement = TableUtils.getPartitionElements(source);
                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                                source.getName(), transfer.getName(), partElement);
                        String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, source.getPartitions().size());
                        source.addSql(new Pair(transferDesc, transferSql));
                    } else if (config.getOptimization().getSortDynamicPartitionInserts()) {
                        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                            source.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
                            if (!config.getCluster(Environment.LEFT).getHdpHive3()) {
                                source.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                            }
                        }
                        String partElement = TableUtils.getPartitionElements(source);
                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                                source.getName(), transfer.getName(), partElement);
                        String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, source.getPartitions().size());
                        source.addSql(new Pair(transferDesc, transferSql));
                    } else {
                        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                            source.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                            if (!config.getCluster(Environment.LEFT).getHdpHive3()) {
                                source.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                            }
                        }
                        String partElement = TableUtils.getPartitionElements(source);
                        String distPartElement = StatsCalculator.getDistributedPartitionElements(source);
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
                if (!isACIDDowngradeInPlace(config, source)) {
                    String dropTransferSql = MessageFormat.format(MirrorConf.DROP_TABLE, transfer.getName());
                    source.getCleanUpSql().add(new Pair(TableUtils.DROP_SHADOW_TABLE, dropTransferSql));
                }
//        } else {
//            StatsCalculator.setSessionOptions(config.getCluster(Environment.LEFT), source, target);
            }
        }
        return rtn;
    }

    /*

     */
    public Boolean buildTableSchema(CopySpec copySpec) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Config config = copySpec.getConfig();
        Boolean rtn = Boolean.TRUE;

        EnvironmentTable source = getEnvironmentTable(copySpec.getSource());
        EnvironmentTable target = getEnvironmentTable(copySpec.getTarget());
        try {
            // Set Table Name
            if (source.getExists()) {
                target.setName(source.getName());

                // Clear the target spec.
                target.getDefinition().clear();
                // Reset with Source
                target.getDefinition().addAll(getTableDefinition(copySpec.getSource()));
                if (Context.getInstance().getConfig().getEvaluatePartitionLocation() &&
                        source.getPartitioned()) {
                    if (!TableUtils.isACID(source)) {
                        // New Map.  So we can modify it..
                        Map<String, String> targetPartitions = new HashMap<>();
                        targetPartitions.putAll(source.getPartitions());
                        target.setPartitions(targetPartitions);
                        if (!config.getTranslator().translatePartitionLocations(this)) {
                            rtn = Boolean.FALSE;
                        }
//                } else {
//                    target.addIssue(SQL_ACID_W_DC.getDesc());
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
                        if (config.getResetToDefaultLocation()) {
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
                        if (copySpec.getUpgrade() && TableUtils.isManaged(source)) {
                            converted = TableUtils.makeExternal(target);
                            if (converted) {
                                target.addIssue("Schema 'converted' from LEGACY managed to EXTERNAL");
                                target.addProperty(HMS_MIRROR_LEGACY_MANAGED_FLAG, converted.toString());
                                target.addProperty(HMS_MIRROR_CONVERTED_FLAG, converted.toString());
                                if (copySpec.getTakeOwnership()) {
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
                            if (copySpec.getTakeOwnership()) {
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

                        if (copySpec.getTakeOwnership()) {
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

                        if (copySpec.getStripLocation()) {
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
//                        TableUtils.upsertTblProperty(MirrorConf.DOWNGRADED_FROM_ACID, Boolean.TRUE.toString(), target);
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
                        TableUtils.changeTableName(target, copySpec.getTableNamePrefix() + getName());
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
                    if (config.getCluster(copySpec.getTarget()).getPartitionDiscovery().getAuto() && TableUtils.isPartitioned(target)) {
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
                            if (copySpec.getReplaceLocation() && (!TableUtils.isACID(source) || config.getMigrateACID().isDowngrade())) {
                                String sourceLocation = TableUtils.getLocation(getName(), getTableDefinition(copySpec.getSource()));
                                int level = 1;
                                if (config.getFilter().isTableFiltering()) {
                                    level = 0;
                                }
                                String targetLocation = copySpec.getConfig().getTranslator().
                                        translateTableLocation(this, sourceLocation, level, null);
                                if (!TableUtils.updateTableLocation(target, targetLocation)) {
                                    rtn = Boolean.FALSE;
                                }
                                // Check if the locations align.  If not, warn.
                                if (config.getTransfer().getWarehouse().getExternalDirectory() != null &&
                                        config.getTransfer().getWarehouse().getManagedDirectory() != null) {
                                    if (TableUtils.isExternal(target)) {
                                        // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
                                        if (!targetLocation.startsWith(getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION))) {
                                            // Set warning that even though you've specified to warehouse directories, the current configuration
                                            // will NOT place it in that directory.
                                            String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
                                                    getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION),
                                                    targetLocation);
                                            addIssue(Environment.RIGHT, msg);
                                        }
                                    } else {
                                        if (!targetLocation.startsWith(getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION))) {
                                            // Set warning that even though you've specified to warehouse directories, the current configuration
                                            // will NOT place it in that directory.
                                            String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
                                                    getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION),
                                                    targetLocation);
                                            addIssue(Environment.RIGHT, msg);
                                        }

                                    }
                                }

                            }
//                        if (copySpec.getStripLocation()) {
//                            TableUtils.stripLocation(target);
//                        }
                            if (getReMapped()) {
                                target.addIssue(MessageCode.TABLE_LOCATION_REMAPPED.getDesc());
                            } else if (config.getTranslator().getForceExternalLocation()) {
                                target.addIssue(MessageCode.TABLE_LOCATION_FORCED.getDesc());
                            } else if (config.getResetToDefaultLocation()) {
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
                            } else if (copySpec.getReplaceLocation()) {
                                if (config.getTransfer().getIntermediateStorage() != null) {
                                    String isLoc = config.getTransfer().getIntermediateStorage();
                                    // Deal with extra '/'
                                    isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                                    isLoc = isLoc + "/" + config.getTransfer().getRemoteWorkingDirectory() + "/" +
                                            config.getRunMarker() + "/" +
//                                        config.getTransfer().getTransferPrefix() + this.getUnique() + "_" +
                                            this.getParent().getName() + "/" +
                                            this.getName();
                                    if (!TableUtils.updateTableLocation(target, isLoc)) {
                                        rtn = Boolean.FALSE;
                                    }
                                } else if (config.getTransfer().getCommonStorage() != null) {
                                    String sourceLocation = TableUtils.getLocation(getName(), getTableDefinition(copySpec.getSource()));
                                    String targetLocation = copySpec.getConfig().getTranslator().
                                            translateTableLocation(this, sourceLocation, 1, null);
                                    if (!TableUtils.updateTableLocation(target, targetLocation)) {
                                        rtn = Boolean.FALSE;
                                    }
                                } else if (copySpec.getStripLocation()) {
                                    TableUtils.stripLocation(target);
                                } else if (config.isReplace()) {
                                    String sourceLocation = TableUtils.getLocation(getName(), getTableDefinition(copySpec.getSource()));
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
                                            isLoc + this.getParent().getName() + "/" + this.getName();
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
                    if (!config.getSkipFeatures()) {
                        for (FeaturesEnum features : FeaturesEnum.values()) {
                            Feature feature = features.getFeature();
                            LOG.debug("Table: " + getName() + " - Checking Feature: " + features);
                            if (feature.fixSchema(target)) {
                                LOG.debug("Table: " + getName() + " - Feature Applicable: " + features);
                                target.addIssue("Feature (" + features + ") was found applicable and adjustments applied. " +
                                        feature.getDescription());
                            } else {
                                LOG.debug("Table: " + getName() + " - Feature NOT Applicable: " + features);
                            }
                        }
                    } else {
                        LOG.debug("Table: " + getName() + " - Skipping Features Check...");
                    }

                    if (config.isTranslateLegacy()) {
                        if (config.getLegacyTranslations().fixSchema(target)) {
                            LOG.info("Legacy Translation applied to: " + getParent().getName() + target.getName());
                        }
                    }

                    // Add props to definition.
                    if (whereTherePropsAdded(copySpec.getTarget())) {
                        Set<String> keys = target.getAddProperties().keySet();
                        for (String key : keys) {
                            TableUtils.upsertTblProperty(key, target.getAddProperties().get(key), target);
                        }
                    }

                    if (!copySpec.getTakeOwnership() && config.getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION) {
                        TableUtils.removeTblProperty(EXTERNAL_TABLE_PURGE, target);
                    }

                    if (config.getCluster(copySpec.getTarget()).getLegacyHive() && config.getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION) {
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
            LOG.error("Error building table schema: " + e.getMessage(), e);
            source.addIssue("Error building table schema: " + e.getMessage());
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    public Boolean buildTransferSql(EnvironmentTable source, EnvironmentTable shadow, EnvironmentTable target, Config config) {
        Boolean rtn = Boolean.TRUE;
        // Build Source->Transfer SQL
        rtn = buildSourceToTransferSql(config);

        // Build Shadow->Final SQL
        if (rtn) {
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                getEnvironmentTable(Environment.RIGHT).addSql("distcp specified", "-- Run the Distcp output to migrate data.");
                if (source.getPartitioned()) {
                    String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, target.getName());
                    target.addCleanUpSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
                }
            } else if (config.getTransfer().getCommonStorage() == null) {
                rtn = buildShadowToFinalSql(config);
            }
        }
        return rtn;
    }

    public Boolean buildoutCOMMONDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout COMMON Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
            // Swap out the namespace of the LEFT with the RIGHT.
            copySpec.setReplaceLocation(Boolean.FALSE);
            if (config.convertManaged())
                copySpec.setUpgrade(Boolean.TRUE);
            // COMMON owns the data unless readonly specified.
            if (!config.isReadOnly())
                copySpec.setTakeOwnership(Boolean.TRUE);
            if (config.isNoPurge())
                copySpec.setTakeOwnership(Boolean.FALSE);

            if (config.isSync()) {
                // We assume that the 'definitions' are only there if the
                //     table exists.
                if (!let.getExists() && ret.getExists()) {
                    // If left is empty and right is not, DROP RIGHT.
                    ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else if (let.getExists() && !ret.getExists()) {
                    // If left is defined and right is not, CREATE RIGHT.
                    ret.addIssue("Schema missing, will be CREATED");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else if (let.getExists() && ret.getExists()) {
                    // If left and right, check schema change and replace if necessary.
                    // Compare Schemas.
                    if (schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                        ret.addIssue(SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                    } else {
                        if (TableUtils.isExternalPurge(ret)) {
                            ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                    "We can NOT safely replace the table without compromising the data. No action will be taken.");
                            ret.setCreateStrategy(CreateStrategy.LEAVE);
                            return Boolean.FALSE;
                        } else {
                            ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                            ret.setCreateStrategy(CreateStrategy.REPLACE);
                        }
                    }
                }
                // With sync, don't own data.
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                if (ret.getExists()) {
                    // Already exists, no action.
                    ret.addIssue(SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                    return Boolean.FALSE;
                } else {
                    ret.addIssue("Schema will be created");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                }
            }
            // Rebuild Target from Source.
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue("Can't use COMMON for ACID tables");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
        }
        return rtn;
    }

    public Boolean buildoutCOMMONSql(Config config, DBMirror dbMirror) {
        return buildoutSCHEMA_ONLYSql(config, dbMirror);
    }

    public Boolean buildoutDUMPDefinition(Config config, DBMirror dbMirror) {
//        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout DUMP Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        // Standardize the LEFT def.
        // Remove DB from CREATE
        TableUtils.stripDatabase(let.getName(), let.getDefinition());

        // If not legacy, remove location from ACID tables.
        if (!config.getCluster(Environment.LEFT).getLegacyHive() &&
                TableUtils.isACID(let)) {
            TableUtils.stripLocation(let.getName(), let.getDefinition());
        }
        return Boolean.TRUE;
    }

    public Boolean buildoutDUMPSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout DUMP SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        let.getSql().clear();
        useDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
        let.addSql(TableUtils.USE_DESC, useDb);

        createTbl = this.getCreateStatement(Environment.LEFT);
        let.addSql(TableUtils.CREATE_DESC, createTbl);
        if (!config.getCluster(Environment.LEFT).getLegacyHive() && config.getTransferOwnership() && let.getOwner() != null) {
            String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, let.getName(), let.getOwner());
            let.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
        }

        // If partitioned, !ACID, repair
        if (let.getPartitioned() && !TableUtils.isACID(let)) {
            if (config.getEvaluatePartitionLocation()) {
                String tableParts = config.getTranslator().buildPartitionAddStatement(let);
                String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION, let.getName(), tableParts);
                let.addSql(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION_DESC, addPartSql);
            } else if (config.getCluster(Environment.LEFT).getPartitionDiscovery().getInitMSCK()) {
                String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, let.getName());
                if (config.getTransfer().getStorageMigration().isDistcp()) {
                    let.addCleanUpSql(TableUtils.REPAIR_DESC, msckStmt);
                } else {
                    let.addSql(TableUtils.REPAIR_DESC, msckStmt);
                }
            }
        }

        rtn = Boolean.TRUE;

        return rtn;

    }

    /*
     */
    public Boolean buildoutEXPORT_IMPORTDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout EXPORT_IMPORT Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        if (TableUtils.isACID(let) && !config.getCluster(Environment.LEFT).getLegacyHive().equals(config.getCluster(Environment.RIGHT).getLegacyHive())) {
            let.addIssue("Can't process ACID tables with EXPORT_IMPORT between 'legacy' and 'non-legacy' hive environments.  The processes aren't compatible.");
            return Boolean.FALSE;
        }

        if (!TableUtils.isHiveNative(let)) {
            let.addIssue("Can't process ACID tables, VIEWs, or Non Native Hive Tables with this strategy.");
            return Boolean.FALSE;
        }

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Swap out the namespace of the LEFT with the RIGHT.
        copySpec.setReplaceLocation(Boolean.TRUE);
        if (config.convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (!config.isReadOnly() || !config.isSync()) {
            copySpec.setTakeOwnership(Boolean.TRUE);
        }
        if (config.isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }
        if (config.isNoPurge()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }

        if (ret.getExists()) {
            // Already exists, no action.
            ret.addIssue("Schema exists already, no action.  If you wish to rebuild the schema, " +
                    "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
            ret.setCreateStrategy(CreateStrategy.LEAVE);
        } else {
            ret.addIssue("Schema will be created");
            ret.setCreateStrategy(CreateStrategy.CREATE);
            rtn = Boolean.TRUE;
        }

        if (rtn)
            // Build Target from Source.
            rtn = buildTableSchema(copySpec);

        return rtn;
    }

    public Boolean buildoutEXPORT_IMPORTSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Database: " + dbMirror.getName() + " buildout EXPORT_IMPORT SQL");

        String database = null;
        database = config.getResolvedDB(dbMirror.getName());

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        try {
            // LEFT Export to directory
            String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
            let.addSql(TableUtils.USE_DESC, useLeftDb);
            String exportLoc = null;

            if (config.getTransfer().getIntermediateStorage() != null) {
                String isLoc = config.getTransfer().getIntermediateStorage();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" +
                        config.getTransfer().getRemoteWorkingDirectory() + "/" +
                        config.getRunMarker() + "/" +
//                    config.getTransfer().getTransferPrefix() + this.getUnique() + "_" +
                        this.getParent().getName() + "/" +
                        this.getName();
            } else if (config.getTransfer().getCommonStorage() != null) {
                String isLoc = config.getTransfer().getCommonStorage();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" + config.getTransfer().getRemoteWorkingDirectory() + "/" +
                        config.getRunMarker() + "/" +
//                    config.getTransfer().getTransferPrefix() + this.getUnique() + "_" +
                        this.getParent().getName() + "/" +
                        this.getName();
            } else {
                exportLoc = config.getTransfer().getExportBaseDirPrefix() + dbMirror.getName() + "/" + let.getName();
            }
            String origTableName = let.getName();
            if (isACIDDowngradeInPlace(config, let)) {
                // Rename original table.
                // Remove property (if exists) to prevent rename from happening.
                if (TableUtils.hasTblProperty(TRANSLATED_TO_EXTERNAL, let)) {
                    String unSetSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, origTableName, TRANSLATED_TO_EXTERNAL);
                    let.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, unSetSql);
                }
                String newTblName = let.getName() + "_archive";
                String renameSql = MessageFormat.format(MirrorConf.RENAME_TABLE, origTableName, newTblName);
                TableUtils.changeTableName(let, newTblName);
                let.addSql(TableUtils.RENAME_TABLE, renameSql);
            }

            String exportSql = MessageFormat.format(MirrorConf.EXPORT_TABLE, let.getName(), exportLoc);
            let.addSql(TableUtils.EXPORT_TABLE, exportSql);

            // RIGHT IMPORT from Directory
            if (!isACIDDowngradeInPlace(config, let)) {
                String useRightDb = MessageFormat.format(MirrorConf.USE, database);
                ret.addSql(TableUtils.USE_DESC, useRightDb);
            }

            String importLoc = null;
            if (config.getTransfer().getIntermediateStorage() != null || config.getTransfer().getCommonStorage() != null) {
                importLoc = exportLoc;
            } else {
                importLoc = config.getCluster(Environment.LEFT).getHcfsNamespace() + exportLoc;
            }

            String sourceLocation = TableUtils.getLocation(let.getName(), let.getDefinition());
            String targetLocation = config.getTranslator().translateTableLocation(this, sourceLocation, 1, null);
            String importSql;
            if (TableUtils.isACID(let)) {
                if (!config.getMigrateACID().isDowngrade()) {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_TABLE, let.getName(), importLoc);
                } else {
                    if (config.getMigrateACID().isDowngradeInPlace()) {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, origTableName, importLoc);
                    } else {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
                    }
                }
            } else {
                if (config.getResetToDefaultLocation()) {
                    if (config.getTransfer().getWarehouse().getExternalDirectory() != null) {
                        // Build default location, because in some cases when location isn't specified, it will use the "FROM"
                        // location in the IMPORT statement.
                        targetLocation = config.getCluster(Environment.RIGHT).getHcfsNamespace() + config.getTransfer().getWarehouse().getExternalDirectory() +
                                "/" + config.getResolvedDB(dbMirror.getName()) + ".db/" + getName();
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                    } else {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
                    }
                } else {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                }
            }

            if (ret.getExists()) {
                if (config.isSync()) {
                    // Need to Drop table first.
                    String dropExistingTable = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
                    ;
                    if (isACIDDowngradeInPlace(config, let)) {
                        let.addSql(MirrorConf.DROP_TABLE_DESC, dropExistingTable);
                        let.addIssue(EXPORT_IMPORT_SYNC.getDesc());
                    } else {
                        ret.addSql(MirrorConf.DROP_TABLE_DESC, dropExistingTable);
                        ret.addIssue(EXPORT_IMPORT_SYNC.getDesc());
                    }
                }
            }
            if (isACIDDowngradeInPlace(config, let)) {
                let.addSql(TableUtils.IMPORT_TABLE, importSql);
            } else {
                ret.addSql(TableUtils.IMPORT_TABLE, importSql);
                if (!config.getCluster(Environment.RIGHT).getLegacyHive() && config.getTransferOwnership() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, let.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                }
            }

            if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit() &&
                    config.getHybrid().getExportImportPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->exportImportPartitionLimit) of " + config.getHybrid().getExportImportPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ep'.");
                rtn = Boolean.FALSE;
            } else {
                rtn = Boolean.TRUE;
            }
        } catch (Throwable t) {
            LOG.error("Error building EXPORT_IMPORT SQL: " + t.getMessage(), t);
            let.addIssue("Error building EXPORT_IMPORT SQL: " + t.getMessage());
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    /*
    TODO: buildoutHYBRIDDefinition
     */
    private Boolean buildoutHYBRIDDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout HYBRID Definition");
        EnvironmentTable let = null;

        let = getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let)) {
            if (config.getMigrateACID().isOn()) {
                rtn = buildoutIntermediateDefinition(config, dbMirror);
            } else {
                let.addIssue(TableUtils.ACID_NOT_ON);
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit()) {
                    rtn = buildoutSQLDefinition(config, dbMirror);
                } else {
                    rtn = buildoutEXPORT_IMPORTDefinition(config, dbMirror);
                }

            }
        }

        return rtn;
    }

    /*
     */
    public Boolean buildoutIntermediateDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout Intermediate Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        CopySpec rightSpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);

        if (ret.getExists()) {
            if (config.getCluster(Environment.RIGHT).getCreateIfNotExists() && config.isSync()) {
                ret.addIssue(CINE_WITH_EXIST.getDesc());
                ret.setCreateStrategy(CreateStrategy.CREATE);
            } else {
                // Already exists, no action.
                ret.addIssue(SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                ret.setCreateStrategy(CreateStrategy.NOTHING);
                return Boolean.FALSE;
            }
        } else {
            ret.setCreateStrategy(CreateStrategy.CREATE);
        }

        if (!TableUtils.isACID(let) && TableUtils.isManaged(let)) {
            // Managed to EXTERNAL
            rightSpec.setUpgrade(Boolean.TRUE);
            rightSpec.setReplaceLocation(Boolean.TRUE);
        } else if (TableUtils.isACID(let)) {
            // ACID
            if (config.getMigrateACID().isDowngrade()) {
                if (config.getTransfer().getCommonStorage() == null) {
                    if (config.getTransfer().getStorageMigration().isDistcp()) {
                        rightSpec.setReplaceLocation(Boolean.TRUE);
                    } else {
                        rightSpec.setStripLocation(Boolean.TRUE);
                    }
                } else {
                    rightSpec.setReplaceLocation(Boolean.TRUE);
                }
                rightSpec.setMakeExternal(Boolean.TRUE);
                // Strip the Transactional Elements
                rightSpec.setMakeNonTransactional(Boolean.TRUE);
                // Set Purge Flag
                rightSpec.setTakeOwnership(Boolean.TRUE);
            } else {
                // Use the system default location when converting.
                rightSpec.setStripLocation(Boolean.TRUE);
            }
        } else {
            // External
            rightSpec.setReplaceLocation(Boolean.TRUE);
        }

        // Build Target from Source.
        rtn = buildTableSchema(rightSpec);

        // Build Transfer Spec.
        CopySpec transferSpec = new CopySpec(config, Environment.LEFT, Environment.TRANSFER);
        if (config.getTransfer().getCommonStorage() == null) {
            if (config.getCluster(Environment.LEFT).getLegacyHive() != config.getCluster(Environment.RIGHT).getLegacyHive()) {
                if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                    transferSpec.setMakeNonTransactional(Boolean.TRUE);
                } else {
                    // Don't support non-legacy to legacy conversions.
                    let.addIssue("Don't support Non-Legacy to Legacy conversions.");
                    return Boolean.FALSE;
                }
            } else {
                if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                    // Legacy to Legacy
                    // Non-Transactional, Managed (for ownership)
                    transferSpec.setMakeNonTransactional(Boolean.TRUE);
                } else {
                    // Non Legacy to Non Legacy
                    // External w/ Ownership
                    transferSpec.setMakeExternal(Boolean.TRUE);
                    transferSpec.setTakeOwnership(Boolean.TRUE);
                }
            }
        } else {
            // Storage will be used by right.  So don't let Transfer table own data.
            transferSpec.setMakeNonTransactional(Boolean.TRUE);
            if (config.getCluster(Environment.LEFT).getLegacyHive() != config.getCluster(Environment.RIGHT).getLegacyHive()) {
                if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                    // Don't support non-legacy to legacy conversions.
                    let.addIssue("Don't support Non-Legacy to Legacy conversions.");
                    return Boolean.FALSE;
                } else {
                    if (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade()) {
                        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                            transferSpec.setMakeExternal(Boolean.TRUE);
                            transferSpec.setTakeOwnership(Boolean.FALSE);
                        } else {
                            transferSpec.setMakeExternal(Boolean.TRUE);
                        }
                    } else {
                        transferSpec.setMakeExternal(Boolean.TRUE);
                    }
                }
            } else {
                if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                    // Non Legacy to Non Legacy
                    // External w/ Ownership
                    transferSpec.setMakeExternal(Boolean.TRUE);
                    if (config.getMigrateACID().isDowngrade()) {
                        // The location will be used by the right cluster, so do not own the data.
                        transferSpec.setTakeOwnership(Boolean.FALSE);
                    } else {
                        // Common Storage is used just like Intermediate Storage in this case.
                        transferSpec.setTakeOwnership(Boolean.TRUE);
                    }
                }
            }
        }

        transferSpec.setTableNamePrefix(config.getTransfer().getTransferPrefix());
        transferSpec.setReplaceLocation(Boolean.TRUE);

        if (rtn)
            // Build transfer table.
            rtn = buildTableSchema(transferSpec);

        // Build Shadow Spec (don't build when using commonStorage)
        // If acid and ma.isOn
        // if not downgrade
        if ((config.getTransfer().getIntermediateStorage() != null && !TableUtils.isACID(let)) ||
                (TableUtils.isACID(let) && config.getMigrateACID().isOn())) {
            if (!config.getMigrateACID().isDowngrade() ||
                    // Is Downgrade but the downgraded location isn't available to the right.
                    (config.getMigrateACID().isDowngrade() && config.getTransfer().getCommonStorage() == null)) {
                if (!config.getTransfer().getStorageMigration().isDistcp()) { // ||
//                (config.getMigrateACID().isOn() && TableUtils.isACID(let)
//                        && !config.getMigrateACID().isDowngrade())) {
                    CopySpec shadowSpec = new CopySpec(config, Environment.LEFT, Environment.SHADOW);
                    shadowSpec.setUpgrade(Boolean.TRUE);
                    shadowSpec.setMakeExternal(Boolean.TRUE);
                    shadowSpec.setTakeOwnership(Boolean.FALSE);
                    shadowSpec.setReplaceLocation(Boolean.TRUE);
                    shadowSpec.setTableNamePrefix(config.getTransfer().getShadowPrefix());

                    if (rtn)
                        rtn = buildTableSchema(shadowSpec);
                }
            }
        }
        return rtn;
    }

//    public TableMirror(String tablename) {
//        this.name = tablename;
//        addStep("init", null);
//    }

    public Boolean buildoutIntermediateSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout Intermediate SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        if (TableUtils.isACID(let) || !config.getTransfer().getStorageMigration().isDistcp()) {
            // LEFT Transfer Table
            database = dbMirror.getName();
            useDb = MessageFormat.format(MirrorConf.USE, database);

            let.addSql(TableUtils.USE_DESC, useDb);
            // Drop any previous TRANSFER table, if it exists.
            String transferDropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, tet.getName());
            let.addSql(TableUtils.DROP_DESC, transferDropStmt);

            // Create Transfer Table
            String transferCreateStmt = getCreateStatement(Environment.TRANSFER);
            let.addSql(TableUtils.CREATE_TRANSFER_DESC, transferCreateStmt);

            database = config.getResolvedDB(dbMirror.getName());
            useDb = MessageFormat.format(MirrorConf.USE, database);
            ret.addSql(TableUtils.USE_DESC, useDb);

            // RIGHT SHADOW Table
            if (set.getDefinition().size() > 0) { //config.getTransfer().getCommonStorage() == null || !config.getMigrateACID().isDowngrade()) {

                // Drop any previous SHADOW table, if it exists.
                String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                // Create Shadow Table
                String shadowCreateStmt = getCreateStatement(Environment.SHADOW);
                ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
                // Repair Partitions
//            if (let.getPartitioned()) {
//                String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
//                ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
//            }
            }
        }

        // RIGHT Final Table
        String rightDrop = null;
        switch (ret.getCreateStrategy()) {
            case NOTHING:
            case LEAVE:
                // Do Nothing.
                break;
            case DROP:
                rightDrop = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, rightDrop);
                break;
            case REPLACE:
                rightDrop = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, rightDrop);
                String createStmt = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                String createStmt2 = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                if (!config.getCluster(Environment.RIGHT).getLegacyHive() && config.getTransferOwnership() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, ret.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                }
                if (let.getPartitioned()) {
                    if (config.getTransfer().getCommonStorage() != null) {
                        if (!TableUtils.isACID(let) || (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade())) {
                            String rightMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, ret.getName());
                            ret.addSql(TableUtils.REPAIR_DESC, rightMSCKStmt);
                        }
                    }
                }

                break;
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    public Boolean buildoutLINKEDDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout LINKED Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
            // Swap out the namespace of the LEFT with the RIGHT.
            copySpec.setReplaceLocation(Boolean.FALSE);
            if (config.convertManaged())
                copySpec.setUpgrade(Boolean.TRUE);
            // LINKED doesn't own the data.
            copySpec.setTakeOwnership(Boolean.FALSE);

            if (config.isSync()) {
                // We assume that the 'definitions' are only there is the
                //     table exists.
                if (!let.getExists() && ret.getExists()) {
                    // If left is empty and right is not, DROP RIGHT.
                    ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else if (let.getExists() && !ret.getExists()) {
                    // If left is defined and right is not, CREATE RIGHT.
                    ret.addIssue("Schema missing, will be CREATED");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else if (let.getExists() && ret.getExists()) {
                    // If left and right, check schema change and replace if necessary.
                    // Compare Schemas.
                    if (schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                        ret.addIssue(SCHEMA_EXISTS_NO_ACTION.getDesc());
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                    } else {
                        if (TableUtils.isExternalPurge(ret)) {
                            ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                    "We can NOT safely replace the table without compromising the data. No action will be taken.");
                            ret.setCreateStrategy(CreateStrategy.LEAVE);
                            return Boolean.FALSE;
                        } else {
                            ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                            ret.setCreateStrategy(CreateStrategy.REPLACE);
                        }
                    }
                }
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                if (ret.getExists()) {
                    // Already exists, no action.
                    ret.addIssue("Schema exists already, no action. If you wish to rebuild the schema, " +
                            "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                    return Boolean.FALSE;
                } else {
                    ret.addIssue("Schema will be created");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                }
            }
            // Rebuild Target from Source.
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue("Can't LINK ACID tables");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
        }
        return rtn;
    }

    public Boolean buildoutLINKEDSql(Config config, DBMirror dbMirror) {
        return buildoutSCHEMA_ONLYSql(config, dbMirror);
    }

    public Boolean buildoutSCHEMA_ONLYDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SCHEMA_ONLY Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Swap out the namespace of the LEFT with the RIGHT.
        copySpec.setReplaceLocation(Boolean.TRUE);
        if (config.convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (!config.isReadOnly() || !config.isSync()) {
            if ((TableUtils.isExternal(let) && config.getCluster(Environment.LEFT).getLegacyHive()) ||
                    // Don't add purge for non-legacy environments...
                    // https://github.com/cloudera-labs/hms-mirror/issues/5
                    (TableUtils.isExternal(let) && !config.getCluster(Environment.LEFT).getLegacyHive())) {
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                copySpec.setTakeOwnership(Boolean.TRUE);
            }
        } else if (copySpec.getUpgrade()) {
            ret.addIssue("Ownership (PURGE Option) not set because of either: `sync` or `ro|read-only` was specified in the config.");
        }
        if (config.isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }
        if (config.isNoPurge()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }

        if (config.isSync()) {
            // We assume that the 'definitions' are only there is the
            //     table exists.
            if (!let.getExists() && ret.getExists()) {
                // If left is empty and right is not, DROP RIGHT.
                ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                ret.setCreateStrategy(CreateStrategy.DROP);
            } else if (let.getExists() && !ret.getExists()) {
                // If left is defined and right is not, CREATE RIGHT.
                ret.addIssue("Schema missing, will be CREATED");
                ret.setCreateStrategy(CreateStrategy.CREATE);
            } else if (let.getExists() && ret.getExists()) {
                // If left and right, check schema change and replace if necessary.
                // Compare Schemas.
                if (schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                    if (let.getPartitioned() && config.getEvaluatePartitionLocation()) {
                        ret.setCreateStrategy(CreateStrategy.AMEND_PARTS);
                        ret.addIssue(SCHEMA_EXISTS_SYNC_PARTS.getDesc());
                    } else {
                        ret.addIssue(SCHEMA_EXISTS_NO_ACTION.getDesc());
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                    }
                } else {
                    if (TableUtils.isExternalPurge(ret)) {
                        ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                "We can NOT safely replace the table without compromising the data. No action will be taken.");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                        return Boolean.FALSE;
                    } else {
                        ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                        ret.setCreateStrategy(CreateStrategy.REPLACE);
                    }
                }
            }
            copySpec.setTakeOwnership(Boolean.FALSE);
        } else {
            if (ret.getExists()) {
                if (TableUtils.isView(ret)) {
                    ret.addIssue("View exists already.  Will REPLACE.");
                    ret.setCreateStrategy(CreateStrategy.REPLACE);
                } else {
                    if (config.getCluster(Environment.RIGHT).getCreateIfNotExists()) {
                        ret.addIssue("Schema exists already.  But you've specified 'createIfNotExist', which will attempt to create " +
                                "(possibly fail, softly) and continue with the remainder sql statements for the table/partitions.");
                        ret.setCreateStrategy(CreateStrategy.CREATE);
                    } else {
                        // Already exists, no action.
                        ret.addIssue("Schema exists already, no action. If you wish to rebuild the schema, " +
                                "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                        return Boolean.FALSE;
                    }
                }
            } else {
                ret.addIssue("Schema will be created");
                ret.setCreateStrategy(CreateStrategy.CREATE);
            }
        }

        // For ACID tables, we need to remove the location.
        // Hive 3 doesn't allow this to be set via SQL Create.
        if (TableUtils.isACID(let)) {
            copySpec.setStripLocation(Boolean.TRUE);
        }

        // Rebuild Target from Source.
        if (!TableUtils.isACID(let) || (TableUtils.isACID(let) && config.getMigrateACID().isOn())) {
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue(TableUtils.ACID_NOT_ON);
            ret.setCreateStrategy(CreateStrategy.NOTHING);
            rtn = Boolean.FALSE;
        }

        // If not legacy, remove location from ACID tables.
        if (rtn && !config.getCluster(Environment.LEFT).getLegacyHive() &&
                TableUtils.isACID(let)) {
            TableUtils.stripLocation(let.getName(), let.getDefinition());
        }
        return rtn;
    }

    public Boolean buildoutSCHEMA_ONLYSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SCHEMA_ONLY SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        //ret.getSql().clear();

        database = config.getResolvedDB(dbMirror.getName());
        useDb = MessageFormat.format(MirrorConf.USE, database);

        switch (ret.getCreateStrategy()) {
            case NOTHING:
            case LEAVE:
                // Do Nothing.
                break;
            case DROP:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                break;
            case REPLACE:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String dropStmt2 = null;
                if (TableUtils.isView(ret)) {
                    dropStmt2 = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                } else {
                    dropStmt2 = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                }
                ret.addSql(TableUtils.DROP_DESC, dropStmt2);
                String createStmt = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String createStmt2 = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                if (!config.getCluster(Environment.RIGHT).getLegacyHive() && config.getTransferOwnership() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, ret.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                }
                break;
            case AMEND_PARTS:
                ret.addSql(TableUtils.USE_DESC, useDb);
                break;
        }

        // If partitioned, !ACID, repair
        if (let.getPartitioned() && !TableUtils.isACID(let) &&
                (ret.getCreateStrategy() == CreateStrategy.REPLACE || ret.getCreateStrategy() == CreateStrategy.CREATE
                        || ret.getCreateStrategy() == CreateStrategy.AMEND_PARTS)) {
            if (config.getEvaluatePartitionLocation()) {
                // TODO: Write out the SQL to build the partitions.  NOTE: We need to get the partition locations and modify them
                //       to the new namespace.
                String tableParts = config.getTranslator().buildPartitionAddStatement(ret);
                String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION, ret.getName(), tableParts);
                ret.addSql(MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION_DESC, addPartSql);
            } else if (config.getCluster(Environment.RIGHT).getPartitionDiscovery().getInitMSCK()) {
                String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, ret.getName());
                if (config.getTransfer().getStorageMigration().isDistcp()) {
                    ret.addCleanUpSql(TableUtils.REPAIR_DESC, msckStmt);
                } else {
                    ret.addSql(TableUtils.REPAIR_DESC, msckStmt);
                }
            }
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    public Boolean buildoutSQLACIDDowngradeInplaceDefinition(Config config, DBMirror dbMirror) {
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
        String newCreateTable = this.getCreateStatement(Environment.RIGHT);
        let.addSql(TableUtils.CREATE_DESC, newCreateTable);

        rtn = Boolean.TRUE;

        return rtn;
    }

    /*
    In this case for the inplace downgrade, the 'left' has already been renamed to an
    'archive' and the 'right' table is the new definition (but will be played on the left).
    We're using the 'right' as a placeholder in this case, since we're not using a RIGHT
    environment.
     */
    public Boolean buildoutSQLACIDDowngradeInplaceSQL(Config config, DBMirror dbMirror) {
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

    /*
    The SQL Strategy uses LINKED clusters and is only valid against Legacy Managed and EXTERNAL
    tables.  NO ACID tables.

    - We create the same schema in the 'target' cluster for the TARGET.
    - We need the create and LINKED a shadow table to the LOWER clusters data.

     */
    public Boolean buildoutSQLDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SQL Definition");


        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        EnvironmentTable set = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        // Different transfer technique.  Staging location.
        if (config.getTransfer().getIntermediateStorage() != null ||
                config.getTransfer().getCommonStorage() != null ||
                TableUtils.isACID(let)) {
            return buildoutIntermediateDefinition(config, dbMirror);
        }

        if (ret.getExists()) {
            if (config.isSync() && config.getCluster(Environment.RIGHT).getCreateIfNotExists()) {
                // sync with overwrite.
                ret.addIssue(SQL_SYNC_W_CINE.getDesc());
                ret.setCreateStrategy(CreateStrategy.CREATE);
            } else {
                ret.addIssue(SQL_SYNC_WO_CINE.getDesc());
                return Boolean.FALSE;
            }
        } else {
            ret.addIssue(SCHEMA_WILL_BE_CREATED.getDesc());
            ret.setCreateStrategy(CreateStrategy.CREATE);
        }

        if (config.getTransfer().getCommonStorage() == null) {
            CopySpec shadowSpec = null;

            // Create a 'shadow' table definition on right cluster pointing to the left data.
            shadowSpec = new CopySpec(config, Environment.LEFT, Environment.SHADOW);

            if (config.convertManaged())
                shadowSpec.setUpgrade(Boolean.TRUE);

            // Don't claim data.  It will be in the LEFT cluster, so the LEFT owns it.
            shadowSpec.setTakeOwnership(Boolean.FALSE);

            // Create table with alter name in RIGHT cluster.
            shadowSpec.setTableNamePrefix(config.getTransfer().getShadowPrefix());

            // Build Shadow from Source.
            rtn = buildTableSchema(shadowSpec);
        }

        // Create final table in right.
        CopySpec rightSpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);

        // Swap out the namespace of the LEFT with the RIGHT.
        rightSpec.setReplaceLocation(Boolean.TRUE);
        if (TableUtils.isManaged(let) && config.convertManaged()) {
            rightSpec.setUpgrade(Boolean.TRUE);
        } else {
            rightSpec.setMakeExternal(Boolean.TRUE);
        }
        if (config.isReadOnly()) {
            rightSpec.setTakeOwnership(Boolean.FALSE);
        } else if (TableUtils.isManaged(let)) {
            rightSpec.setTakeOwnership(Boolean.TRUE);
        }
        if (config.isNoPurge()) {
            rightSpec.setTakeOwnership(Boolean.FALSE);
        }

        // Rebuild Target from Source.
        rtn = buildTableSchema(rightSpec);

        return rtn;
    }

    public Boolean buildoutSQLSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SQL SQL");

        if (config.getTransfer().getIntermediateStorage() != null ||
                config.getTransfer().getCommonStorage() != null) {
            return buildoutIntermediateSql(config, dbMirror);
        }

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        if (TableUtils.isACID(let)) {
            rtn = Boolean.FALSE;
            // TODO: Hum... Not sure this is right.
            addIssue(Environment.LEFT, "Shouldn't get an ACID table here.");
        } else {
//        if (!isACIDDowngradeInPlace(config, let)) {
            database = config.getResolvedDB(dbMirror.getName());
            useDb = MessageFormat.format(MirrorConf.USE, database);

            ret.addSql(TableUtils.USE_DESC, useDb);

            String dropStmt = null;
            // Create RIGHT Shadow Table
            if (set.getDefinition().size() > 0) {
                // Drop any previous SHADOW table, if it exists.
                dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);

                String shadowCreateStmt = getCreateStatement(Environment.SHADOW);
                ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
                // Repair Partitions
//                if (let.getPartitioned()) {
//                    String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
//                    ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
//                }
            }

            // RIGHT Final Table
            switch (ret.getCreateStrategy()) {
                case NOTHING:
                case LEAVE:
                    // Do Nothing.
                    break;
                case DROP:
                    dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, dropStmt);
                    break;
                case REPLACE:
                    dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, dropStmt);
                    String createStmt = getCreateStatement(Environment.RIGHT);
                    ret.addSql(TableUtils.CREATE_DESC, createStmt);
                    break;
                case CREATE:
                    String createStmt2 = getCreateStatement(Environment.RIGHT);
                    ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                    if (!config.getCluster(Environment.RIGHT).getLegacyHive() && config.getTransferOwnership() && let.getOwner() != null) {
                        String ownerSql = MessageFormat.format(MirrorConf.SET_OWNER, ret.getName(), let.getOwner());
                        ret.addSql(MirrorConf.SET_OWNER_DESC, ownerSql);
                    }
                    break;
            }

            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    /*
    We'll only work with the LEFT cluster in this configuration.  But we will use the RIGHT clusters object to store
    the new information.

     */
    public Boolean buildoutSTORAGEMIGRATIONDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SQL Definition");

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
        String lLocation = TableUtils.getLocation(this.getName(), let.getDefinition());
        if (lLocation.startsWith(sb.toString())) {
            addIssue(Environment.LEFT, "Table has already been migrated");
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
        rtn = buildTableSchema(copySpec);

        return rtn;
    }

    public Boolean buildoutSTORAGEMIGRATIONSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout STORAGE_MIGRATION SQL");

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
        let.setName(let.getName() + "_" + getUnique() + "_storage_migration");
        String origAlterRename = MessageFormat.format(MirrorConf.RENAME_TABLE, ret.getName(), let.getName());
        let.addSql(MirrorConf.RENAME_TABLE_DESC, origAlterRename);

        // Create table with New Location
        String createStmt2 = getCreateStatement(Environment.RIGHT);
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

    public List<Pair> getCleanUpSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getCleanUpSql();
    }

    public String getCreateStatement(Environment environment) {
        StringBuilder createStatement = new StringBuilder();
        Boolean cine = Context.getInstance().getConfig().getCluster(environment).getCreateIfNotExists();
        List<String> tblDef = this.getTableDefinition(environment);
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
            throw new RuntimeException("Couldn't location definition for table: " + getName() +
                    " in environment: " + environment.toString());
        }
        return createStatement.toString();
    }

    public EnvironmentTable getEnvironmentTable(Environment environment) {
        EnvironmentTable et = getEnvironments().get(environment);
        if (et == null) {
            et = new EnvironmentTable(this);
            getEnvironments().put(environment, et);
        }
        return et;
    }

    public Map<Environment, EnvironmentTable> getEnvironments() {
        if (environments == null) {
            environments = new TreeMap<Environment, EnvironmentTable>();
        }
        return environments;
    }

    public void setEnvironments(Map<Environment, EnvironmentTable> environments) {
        this.environments = environments;
        // Need to connect after deserialization.
        for (EnvironmentTable environmentTable : environments.values()) {
            environmentTable.setParent(this);
        }
    }

    public List<String> getIssues(Environment environment) {
        return getEnvironmentTable(environment).getIssues();
    }

    public String getMigrationStageMessage() {
        return migrationStageMessage;
    }

    public void setMigrationStageMessage(String migrationStageMessage) {
        this.migrationStageMessage = migrationStageMessage;
        incPhase();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getName();
    }

    public DBMirror getParent() {
        return parent;
    }

    public void setParent(DBMirror parent) {
        this.parent = parent;
    }

    public Map<String, String> getPartitionDefinition(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getPartitions();
    }

    public PhaseState getPhaseState() {
        return phaseState;
    }

    public void setPhaseState(PhaseState phaseState) {
        this.phaseState = phaseState;
    }

    public String getProgressIndicator(int width) {
        StringBuilder sb = new StringBuilder();
        int progressLength = Math.floorDiv(Math.multiplyExact(width, currentPhase.get()), totalPhaseCount.get());
        LOG.info(this.getParent().getName() + ":" + this.getName() + " CurrentPhase: " + currentPhase.get() +
                " -> TotalPhaseCount: " + totalPhaseCount.get());
        LOG.info(this.getParent().getName() + ":" + this.getName() + " Progress: " + progressLength + " of " + width);
        sb.append("\u001B[32m");
        sb.append(StringUtils.rightPad("=", progressLength - 1, "="));
        sb.append("\u001B[33m");
        sb.append(StringUtils.rightPad("-", width - progressLength, "-"));
        sb.append("\u001B[0m|");
        return sb.toString();
    }

    public Map<String, String> getPropAdd(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getAddProperties();
    }

    public Boolean getReMapped() {
        return reMapped;
    }

    public void setReMapped(Boolean reMapped) {
        this.reMapped = reMapped;
    }

    public String getRemoveReason() {
        return removeReason;
    }

    public void setRemoveReason(String removeReason) {
        this.removeReason = removeReason;
    }

    public List<Pair> getSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getSql();
    }

    public Long getStageDuration() {
        return stageDuration;
    }

    public void setStageDuration(Long stageDuration) {
        this.stageDuration = stageDuration;
    }

    public List<Marker> getSteps() {
        return steps;
    }

    public DataStrategyEnum getStrategy() {
        return strategy;
    }

    public void setStrategy(DataStrategyEnum strategy) {
        this.strategy = strategy;
    }

    public List<String> getTableActions(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getActions();
    }

    public List<String> getTableDefinition(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getDefinition();
    }

    public String getUnique() {
        return df.format(Context.getInstance().getConfig().getInitDate());
//        return unique;
    }

    public Boolean hasActions() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : getEnvironments().entrySet()) {
            if (entry.getValue().getActions().size() > 0)
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean hasAddedProperties() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : getEnvironments().entrySet()) {
            if (entry.getValue().getAddProperties().size() > 0)
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean hasIssues() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : getEnvironments().entrySet()) {
            if (entry.getValue().getIssues().size() > 0)
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean hasStatistics() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : getEnvironments().entrySet()) {
            if (entry.getValue().getStatistics().size() > 0)
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public void incPhase() {
        currentPhase.getAndIncrement();
        if (currentPhase.get() >= totalPhaseCount.get()) {
            totalPhaseCount.set(currentPhase.get() + 1);
        }
    }

    public void incTotalPhaseCount() {
        totalPhaseCount.getAndIncrement();
    }

    public Boolean isACIDDowngradeInPlace(Config config, EnvironmentTable tbl) {
        if (TableUtils.isACID(tbl) && config.getMigrateACID().isDowngradeInPlace()) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public Boolean isPartitioned(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getPartitioned();
    }

    public boolean isRemove() {
        return remove;
    }

    public void setRemove(boolean remove) {
        this.remove = remove;
    }

    @JsonIgnore
    public boolean isThereAnIssue(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getIssues().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean isThereCleanupSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getCleanUpSql().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean isThereSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getSql().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public void nextPhase(String msg) {
        incPhase();
        setMigrationStageMessage(msg);
    }

    public void processingDone() {
        totalPhaseCount = currentPhase;
        // Clear message
        setMigrationStageMessage(null);
    }

    public boolean schemasEqual(Environment one, Environment two) {
        List<String> schemaOne = getTableDefinition(one);
        List<String> schemaTwo = getTableDefinition(two);
        if (schemaOne != null && schemaTwo != null) {
            String fpOne = TableUtils.tableFieldsFingerPrint(schemaOne);
            String fpTwo = TableUtils.tableFieldsFingerPrint(schemaTwo);
            if (fpOne.equals(fpTwo)) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        } else {
            return Boolean.FALSE;
        }
    }

    public void setTableDefinition(Environment environment, List<String> tableDefList) {
        EnvironmentTable et = getEnvironmentTable(environment);
        et.setDefinition(tableDefList);
    }

    public boolean whereTherePropsAdded(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getAddProperties().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

}
