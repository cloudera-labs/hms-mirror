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

//import com.cloudera.utils.hadoop.HadoopSession;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hms.mirror.CopySpec;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.feature.Feature;
import com.cloudera.utils.hms.mirror.feature.FeaturesEnum;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.StatsCalculatorService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.cloudera.utils.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION;
import static com.cloudera.utils.hms.mirror.SessionVars.SORT_DYNAMIC_PARTITION_THRESHOLD;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.*;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.BUCKETING_VERSION;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
@Getter
public abstract class DataStrategyBase implements DataStrategy {

    public static final Pattern protocolNSPattern = Pattern.compile("(^.*://)([a-zA-Z0-9](?:(?:[a-zA-Z0-9-]*|(?<!-)\\.(?![-.]))*[a-zA-Z0-9]+)?)(:\\d{4})?");
    // Pattern to find the value of the last directory in a url.
    public static final Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");
    protected StatsCalculatorService statsCalculatorService;
    protected ExecuteSessionService executeSessionService;
    protected TranslatorService translatorService;

    protected Boolean AVROCheck(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        Boolean relative = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        // Check for AVRO
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        if (TableUtils.isAVROSchemaBased(let)) {
            log.info("{}: is an AVRO table.", let.getName());
            String leftPath = TableUtils.getAVROSchemaPath(let);
            String rightPath = null;
            log.debug("{}: Original AVRO Schema path: {}", let.getName(), leftPath);
                /* Checks:
                - Is Path prefixed with a protocol?
                    - (Y) Does it match the LEFT's hcfsNamespace.
                        - (Y) Replace prefix with RIGHT 'hcfsNamespace' prefix.
                        - (N) Throw WARNING and set return to FALSE.  We don't recognize the prefix and
                                 can't guarantee that we can retrieve the file.
                    - (N) Leave it and copy the file to the same relative path on the RIGHT
                 */
            String leftNamespace = NamespaceUtils.getNamespace(leftPath);
            try {
                if (nonNull(leftNamespace)) {
                    log.info("{}: Namespace found: {}", let.getName(), leftNamespace);
                    rightPath = NamespaceUtils.replaceNamespace(leftPath, hmsMirrorConfig.getTargetNamespace());
//            } else {
//                log.info("{}: Namespace NOT found.", let.getName());
//            }

//            Matcher matcher = protocolNSPattern.matcher(leftPath);
//            // ProtocolNS Found.
//            String cpCmd = null;
//            if (matcher.find()) {
//                log.info("{} protocol Matcher found.", let.getName());
//
//                // Return the whole set of groups.
//                String lns = matcher.group(0);
//
//                // Does it match the "LEFT" hcfsNamespace.
//                String leftNS = hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace();
//                if (leftNS.endsWith("/")) {
//                    leftNS = leftNS.substring(0, leftNS.length() - 1);
//                }
//                if (lns.startsWith(leftNS)) {
//                    log.info("{} table namespace matches LEFT clusters namespace.", let.getName());
//
//                    // They match, so replace with RIGHT hcfs namespace.
//                    String newNS = hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace();
//                    if (newNS.endsWith("/")) {
//                        newNS = newNS.substring(0, newNS.length() - 1);
//                    }
//                    rightPath = leftPath.replace(leftNS, newNS);
//                    log.info("{} table namespace adjusted for RIGHT clusters table to {}", ret.getName(), rightPath);
//                    TableUtils.updateAVROSchemaLocation(ret, rightPath);
//                } else {
//                    // Protocol found doesn't match configured hcfs namespace for LEFT.
//                    String warning = "AVRO Schema URL was NOT adjusted. Current (LEFT) path did NOT match the " +
//                            "LEFT hcfsnamespace. " + leftPath + " is NOT in the " + hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace() +
//                            ". Can't determine change, so we'll not do anything.";
//                    ret.addIssue(warning);
//                    ret.addIssue("Schema creation may fail if location isn't available to RIGHT cluster.");
//                    log.warn(warning);
//                }
                } else {
                    // No Protocol defined.  So we're assuming that its a relative path to the
                    // defaultFS
                    String rpath = "AVRO Schema URL appears to be relative: " + leftPath + ". No table definition adjustments.";
                    log.info("{}: {}", let.getName(), rpath);
                    ret.addIssue(rpath);
                    rightPath = leftPath;
                    relative = Boolean.TRUE;
                }

                if (nonNull(leftPath) && nonNull(rightPath) && hmsMirrorConfig.isCopyAvroSchemaUrls() && hmsMirrorConfig.isExecute()) {
                    // Copy over.
                    log.info("{}: Attempting to copy AVRO schema file to target cluster.", let.getName());
                    CliEnvironment cli = executeSessionService.getCliEnvironment();
                    try {
                        CommandReturn cr = null;
                        if (relative) {
                            // checked..
                            // TODO: We won't have the left namespace.
//                            leftPath = hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace() + leftPath;
                            rightPath = hmsMirrorConfig.getTargetNamespace() + rightPath;
                        }
                        log.info("AVRO Schema COPY from: {} to {}", leftPath, rightPath);
                        // Ensure the path for the right exists.
                        String parentDirectory = NamespaceUtils.getParentDirectory(rightPath);
                        if (nonNull(parentDirectory)) {
//                        String pathEnd = matcher.group(1);
//                        String mkdir = rightPath.substring(0, rightPath.length() - pathEnd.length());
                            cr = cli.processInput("mkdir -p " + parentDirectory);
                            if (cr.isError()) {
                                ret.addIssue("Problem creating directory " + parentDirectory + ". " + cr.getError());
                                rtn = Boolean.FALSE;
                            } else {
                                cr = cli.processInput("cp -f " + leftPath + " " + rightPath);
                                if (cr.isError()) {
                                    ret.addIssue("Problem copying AVRO schema file from " + leftPath + " to " + parentDirectory + ".\n```" + cr.getError() + "```");
                                    rtn = Boolean.FALSE;
                                }
                            }
                        }
                    } catch (Throwable t) {
                        log.error("{}: AVRO file copy issue", ret.getName(), t);
                        ret.addIssue(t.getMessage());
                        rtn = Boolean.FALSE;
//                } finally {
//                    if (session != null)
//                        config.getCliEnv().returnSession(session);
                    }
                } else {
                    log.info("{}: did NOT attempt to copy AVRO schema file to target cluster.", let.getName());
                }
                tableMirror.addStep("AVRO", "Checked");
            } catch (RequiredConfigurationException e) {
                log.error("Required Configuration Exception", e);
                ret.addIssue(e.getMessage());
                rtn = Boolean.FALSE;
            }
        } else {
            // Not AVRO, so no action (passthrough)
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    @Autowired
    public void setStatsCalculatorService(StatsCalculatorService statsCalculatorService) {
        this.statsCalculatorService = statsCalculatorService;
    }

    @Override
    public BuildWhat whatToBuild(HmsMirrorConfig config, TableMirror tableMirror) {
        return null;
    }

    public EnvironmentTable getEnvironmentTable(Environment environment, TableMirror tableMirror) {
        EnvironmentTable et = tableMirror.getEnvironments().get(environment);
        if (isNull(et)) {
            et = new EnvironmentTable(tableMirror);
            tableMirror.getEnvironments().put(environment, et);
        }
        return et;
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
            //
            if (source.isExists() || copySpec.getSource() != Environment.RIGHT) {
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
                                if (!isBlank(config.getTransfer().getIntermediateStorage())) {
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
//                                } else if (!isBlank(config.getTransfer().getTargetNamespace())) {
//                                    String sourceLocation = TableUtils.getLocation(tableMirror.getName(), tableMirror.getTableDefinition(copySpec.getSource()));
//                                    String targetLocation = getTranslatorService().
//                                            translateLocation(tableMirror, sourceLocation, 1, null);
//                                    if (!TableUtils.updateTableLocation(target, targetLocation)) {
//                                        rtn = Boolean.FALSE;
//                                    }
//                                } else if (copySpec.isStripLocation()) {
//                                    TableUtils.stripLocation(target);
//                                } else if (config.isReplace()) {
//                                    String sourceLocation = TableUtils.getLocation(tableMirror.getName(), tableMirror.getTableDefinition(copySpec.getSource()));
//                                    String replacementLocation = sourceLocation + "_replacement";
//                                    if (!TableUtils.updateTableLocation(target, replacementLocation)) {
//                                        rtn = Boolean.FALSE;
//                                    }
                                } else {
                                    // Need to use export location
                                    String isLoc = config.getTransfer().getExportBaseDirPrefix();
                                    // Deal with extra '/'
                                    isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                                    // Get Namespace of Original Table
                                    String origNamespace = NamespaceUtils.getNamespace(TableUtils.getLocation(source.getName(), source.getDefinition()));
                                    isLoc = origNamespace +
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

//    public Boolean buildTransferSql(TableMirror tableMirror, Environment origEnv, Environment sourceEnv, Environment targetEnv) {
//        Boolean rtn = Boolean.TRUE;
//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
//
//        EnvironmentTable originalTable = tableMirror.getEnvironmentTable(origEnv);
//        EnvironmentTable sourceTable = tableMirror.getEnvironmentTable(sourceEnv);
//        EnvironmentTable targetTable = tableMirror.getEnvironmentTable(targetEnv);
//
//        // Build Source->Transfer SQL
//        rtn = buildMigrationSql(tableMirror, origEnv, sourceEnv, targetEnv);
//
//        // Build Shadow->Final SQL
//        if (originalTable.getPartitioned()) {
//            String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, targetTable.getName());
//            targetTable.addCleanUpSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
//        }
//        rtn = buildMigrateSql(tableMirror, origEnv, sourceEnv, targetEnv);
//
////        if (rtn) {
////            if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
////                tableMirror.getEnvironmentTable(Environment.RIGHT).addSql("distcp specified", "-- Run the Distcp output to migrate data.");
////                if (sourceTable.getPartitioned()) {
////                    String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, targetTable.getName());
////                    targetTable.addCleanUpSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
////                }
////            } else if (isBlank(hmsMirrorConfig.getTransfer().getTargetNamespace())) {
////                rtn = buildShadowToFinalSql(tableMirror);
////            }
////        }
//        return rtn;
//    }

//    protected Boolean buildShadowToFinalSql(TableMirror tableMirror) {
//        Boolean rtn = Boolean.TRUE;
//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
//
//        // if common storage, skip
//        // if inplace, skip
//        EnvironmentTable source = tableMirror.getEnvironmentTable(Environment.LEFT);
//        EnvironmentTable shadow = tableMirror.getEnvironmentTable(Environment.SHADOW);
//        EnvironmentTable target = tableMirror.getEnvironmentTable(Environment.RIGHT);
//        if ((!TableUtils.isACID(source) && !isBlank(hmsMirrorConfig.getTransfer().getTargetNamespace())) ||
//                isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
//            // Nothing to build.
//            return rtn;
//        } else {
//            if (source.getPartitioned()) {
//                // MSCK repair on Shadow
//                String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, shadow.getName());
//                target.addSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
//            }
//            if (hmsMirrorConfig.getOptimization().isBuildShadowStatistics()) {
//                // Build Shadow Stats.
//
//            }
//            // Set Override Properties.
//            if (hmsMirrorConfig.getOptimization().getOverrides() != null) {
//                for (String key : hmsMirrorConfig.getOptimization().getOverrides().getRight().keySet()) {
//                    target.addSql("Setting " + key, "set " + key + "=" + hmsMirrorConfig.getOptimization().getOverrides().getRight().get(key));
//                }
//            }
//            // Sql from Shadow to Final
//            statsCalculatorService.setSessionOptions(hmsMirrorConfig.getCluster(Environment.RIGHT), source, target);
//            if (source.getPartitioned()) {
//                if (hmsMirrorConfig.getOptimization().isSkip()) {
//                    if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
//                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
//                    }
//                    String partElement = TableUtils.getPartitionElements(source);
//                    String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
//                            shadow.getName(), target.getName(), partElement);
//                    String shadowDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, source.getPartitions().size());
//                    target.addSql(new Pair(shadowDesc, shadowSql));
//                } else if (hmsMirrorConfig.getOptimization().isSortDynamicPartitionInserts()) {
//                    if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
//                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
//                        if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
//                            target.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
//                        }
//                    }
//                    String partElement = TableUtils.getPartitionElements(source);
//                    String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
//                            shadow.getName(), target.getName(), partElement);
//                    String shadowDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, source.getPartitions().size());
//                    target.addSql(new Pair(shadowDesc, shadowSql));
//                } else {
//                    // Prescriptive
//                    if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
//                        target.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
//                        if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
//                            target.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
//                        }
//                    }
//                    String partElement = TableUtils.getPartitionElements(source);
//                    String distPartElement = statsCalculatorService.getDistributedPartitionElements(source);
//                    String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
//                            shadow.getName(), target.getName(), partElement, distPartElement);
//                    String shadowDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, target.getPartitions().size());
//                    target.addSql(new Pair(shadowDesc, shadowSql));
//                }
//            } else {
//                String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE,
//                        shadow.getName(), target.getName());
//                String shadowDesc = MessageFormat.format(TableUtils.LOAD_FROM_SHADOW_DESC, "");
//                target.addSql(new Pair(shadowDesc, shadowSql));
//            }
//            // Drop Shadow Table.
//            String dropShadowSql = MessageFormat.format(MirrorConf.DROP_TABLE, shadow.getName());
//            target.getSql().add(new Pair(TableUtils.DROP_SHADOW_TABLE, dropShadowSql));
//
//        }
//        return rtn;
//    }

    protected Boolean buildMigrationSql(TableMirror tableMirror, Environment originalEnv, Environment sourceEnv, Environment targetEnv) {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable original, source, target;
        original = tableMirror.getEnvironmentTable(originalEnv);
        source = tableMirror.getEnvironmentTable(sourceEnv);
        target = tableMirror.getEnvironmentTable(targetEnv);

        // We need to establish what 'environment' LEFT|RIGHT that we are running this in.
        EnvironmentTable targetEnvTable;
        switch (targetEnv) {
            case SHADOW:
                targetEnvTable = tableMirror.getEnvironmentTable(Environment.RIGHT);
                break;
            case TRANSFER:
                targetEnvTable = tableMirror.getEnvironmentTable(Environment.LEFT);
                break;
            case RIGHT:
                targetEnvTable = tableMirror.getEnvironmentTable(Environment.RIGHT);
                break;
            default:
                targetEnvTable = null;
        }


//        BuildWhat buildWhat = whatToBuild(hmsMirrorConfig, tableMirror);

        if (TableUtils.isACID(original)) {
            if (original.getPartitions().size() > hmsMirrorConfig.getMigrateACID().getPartitionLimit() && hmsMirrorConfig.getMigrateACID().getPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                original.addIssue("The number of partitions: " + original.getPartitions().size() + " exceeds the configuration " +
                        "limit (migrateACID->partitionLimit) of " + hmsMirrorConfig.getMigrateACID().getPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ap'.");
                rtn = Boolean.FALSE;
            }
        } else {
            if (original.getPartitions().size() > hmsMirrorConfig.getHybrid().getSqlPartitionLimit() &&
                    hmsMirrorConfig.getHybrid().getSqlPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                original.addIssue("The number of partitions: " + original.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->sqlPartitionLimit) of " + hmsMirrorConfig.getHybrid().getSqlPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit. Review commandline option '-sp'.");
                rtn = Boolean.FALSE;
            }
        }

        if (isACIDDowngradeInPlace(tableMirror, originalEnv)) {
            String dropOriginalTable = MessageFormat.format(MirrorConf.DROP_TABLE,
                    original.getName());
            targetEnvTable.addCleanUpSql(MirrorConf.DROP_TABLE_DESC, dropOriginalTable);
        }
//        if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
//            source.addSql("distcp specified", "-- Run distcp commands");
//        } else {
        // Set Override Properties.
        if (hmsMirrorConfig.getOptimization().getOverrides() != null) {
            for (String key : hmsMirrorConfig.getOptimization().getOverrides().getLeft().keySet()) {
                targetEnvTable.addSql("Setting " + key, "set " + key + "=" + hmsMirrorConfig.getOptimization().getOverrides().getLeft().get(key));
            }
        }

        if (source.isDefined()) {
            statsCalculatorService.setSessionOptions(hmsMirrorConfig.getCluster(Environment.LEFT), original, original);
            if (original.getPartitioned()) {
                if (hmsMirrorConfig.getOptimization().isSkip()) {
                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                        targetEnvTable.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                    }
                    String partElement = TableUtils.getPartitionElements(original);
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                            source.getName(), target.getName(), partElement);
                    String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, original.getPartitions().size());
                    targetEnvTable.addSql(new Pair(transferDesc, transferSql));
                } else if (hmsMirrorConfig.getOptimization().isSortDynamicPartitionInserts()) {
                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                        targetEnvTable.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
                        if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                            targetEnvTable.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
                        }
                    }
                    String partElement = TableUtils.getPartitionElements(original);
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
                            source.getName(), target.getName(), partElement);
                    String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, original.getPartitions().size());
                    targetEnvTable.addSql(new Pair(transferDesc, transferSql));
                } else {
                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                        targetEnvTable.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
                        if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                            targetEnvTable.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
                        }
                    }
                    String partElement = TableUtils.getPartitionElements(original);
                    String distPartElement = statsCalculatorService.getDistributedPartitionElements(original);
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                            source.getName(), target.getName(), partElement, distPartElement);
                    String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, original.getPartitions().size());
                    targetEnvTable.addSql(new Pair(transferDesc, transferSql));
                }
            } else {
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE,
                        source.getName(), target.getName());
//                String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_DESC);
                targetEnvTable.addSql(new Pair(TableUtils.STAGE_TRANSFER_DESC, transferSql));
            }
            // Drop Transfer Table
            if (!isACIDDowngradeInPlace(tableMirror, Environment.LEFT)) {
                String dropTransferSql = MessageFormat.format(MirrorConf.DROP_TABLE, source.getName());
                original.getCleanUpSql().add(new Pair(TableUtils.DROP_SHADOW_TABLE, dropTransferSql));
            }
        }
//        }
        if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
            targetEnvTable.addSql("distcp specified", "-- Run distcp commands");
        }
        return rtn;
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

}
