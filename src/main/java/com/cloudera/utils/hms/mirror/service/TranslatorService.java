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

import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.TableUtils;
import com.cloudera.utils.hms.util.UrlUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.*;

import static com.cloudera.utils.hms.mirror.MessageCode.LOCATION_NOT_MATCH_WAREHOUSE;
import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
public class TranslatorService {

    @Getter
    private ExecuteSessionService executeSessionService = null;
    private WarehouseService warehouseService;

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setWarehouseService(WarehouseService warehouseService) {
        this.warehouseService = warehouseService;
    }

//    private DatabaseService databaseService;

    public String buildPartitionAddStatement(EnvironmentTable environmentTable) {
        StringBuilder sbPartitionDetails = new StringBuilder();
        Map<String, String> partitions = new HashMap<String, String>();
        // Fix formatting of partition names.
        for (Map.Entry<String, String> item : environmentTable.getPartitions().entrySet()) {
            String partitionName = item.getKey();
            String partSpec = TableUtils.toPartitionSpec(partitionName);
            partitions.put(partSpec, item.getValue());
        }
        // Transfer partitions map to a string using streaming
        partitions.entrySet().stream().forEach(e -> sbPartitionDetails.append("\tPARTITION (")
                .append(e.getKey()).append(") LOCATION '").append(e.getValue()).append("' \n"));
        return sbPartitionDetails.toString();
    }

    @Getter
    @Setter
    public class GLMResult {
        private boolean mapped = Boolean.FALSE;
        private String originalDir;
        private String mappedDir;
    }

    public GLMResult processGlobalLocationMap(String originalLocation, Boolean externalTable) {
        // Set to original, so we capture the original location if we don't find a match.
        GLMResult glmResult = new GLMResult();
        glmResult.setOriginalDir(originalLocation);

        String newLocation = originalLocation;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        if (!hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap().isEmpty()) {
            log.debug("Checking location: {} for replacement element in global location map.", originalLocation);
            for (String key : hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap().keySet()) {
                if (originalLocation.startsWith(key)) {
                    Map<TableType, String> rLocMap = hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap().get(key);
                    String rLoc = null;
                    if (externalTable) {
                        rLoc = rLocMap.get(TableType.EXTERNAL_TABLE);
                        newLocation = rLoc + originalLocation.replace(key, "");
                        glmResult.setMapped(Boolean.TRUE);
                    } else {
                        rLoc = rLocMap.get(TableType.MANAGED_TABLE);
                        if (nonNull(rLoc)) {
                            newLocation = rLoc + originalLocation.replace(key, "");
                            glmResult.setMapped(Boolean.TRUE);
                        }
                    }
                    log.info("Location Map Found. {}:{} New Location: {}", key, rLoc, newLocation);
                    break;
                }
            }
        }
        glmResult.setMappedDir(newLocation);
        return glmResult;
    }

    public Boolean translatePartitionLocations(TableMirror tblMirror) throws RequiredConfigurationException, MissingDataPointException, MismatchException {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        if (tblMirror.getEnvironmentTable(Environment.LEFT).getPartitioned()) {
            // Only Translate for SCHEMA_ONLY.  Leave the DUMP location as is.
            EnvironmentTable target = tblMirror.getEnvironmentTable(Environment.RIGHT);
            boolean isExternal = TableUtils.isExternal(target);

            String originalDatabase = tblMirror.getParent().getName();
            String targetDatabase = HmsMirrorConfigUtil.getResolvedDB(originalDatabase, config);

            /*
            Review the target partition locations and replace the namespace with the new namespace.
            Check whether any global location maps match the location and adjust.
             */
            Map<String, String> partitionLocationMap = target.getPartitions();
            if (partitionLocationMap != null && !partitionLocationMap.isEmpty()) {
                for (Map.Entry<String, String> entry : partitionLocationMap.entrySet()) {
                    String partitionLocation = entry.getValue();
                    String partSpec = entry.getKey();
                    // First level partitions are +2 from the DB level.  So we need to add 2 to the level.
                    // EG: The spec for a single partition will have level 1.
                    String spec[] = partSpec.split("/");
                    int level = spec.length + 1;
                    // Increase level to the table, since we're not filter any tables.  It's assumed that
                    //   we're pulling the whole DB.
//                    if (!config.getFilter().isTableFiltering()) {
//                        level++;
//                    }
                    if (isBlank(partitionLocation) || partitionLocation.isEmpty() ||
                            partitionLocation.equals(NOT_SET)) {
                        rtn = Boolean.FALSE;
                        continue;
                    }

                    String newPartitionLocation = translateTableLocation(tblMirror, partitionLocation, level, partSpec);

                    entry.setValue(newPartitionLocation);
                }
            }
            // end partitions location conversion.
        }
        return rtn;
    }

    public String translateTableLocation(TableMirror tableMirror, String originalLocation,
                                         int level, String partitionSpec)
            throws MismatchException, MissingDataPointException, RequiredConfigurationException {
        String rtn = originalLocation;

        String tableName = tableMirror.getName();
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        String originalDatabase = tableMirror.getParent().getName();
        String targetDatabase = HmsMirrorConfigUtil.getResolvedDB(originalDatabase, config);
        String targetDatabaseDir = nonNull(tableMirror.getParent().getLocationDirectory())?
                tableMirror.getParent().getLocationDirectory() : targetDatabase + ".db";
        String targetDatabaseManagedDir = nonNull(tableMirror.getParent().getManagedLocationDirectory())?
                tableMirror.getParent().getManagedLocationDirectory() : targetDatabase + ".db";
        String originalTableLocation = TableUtils.getLocation(tableName, tableMirror.getEnvironmentTable(Environment.LEFT).getDefinition());

        String targetNamespace = config.getTargetNamespace();

        String relativeDir = NamespaceUtils.stripNamespace(rtn);

        // Get the target database location db directory.  It's usually the same as the database name + ".db"
        // But if this has been redefined, we need to use that.
        // This is the default setting.  We'll override it later if needed.
//        String targetDatabaseDir = targetDatabase + ".db";

        // Check the Global Location Map for a match.
        GLMResult glmMapping = processGlobalLocationMap(relativeDir, TableUtils.isExternal(ret));
//        String mappedDir = processGlobalLocationMap(relativeDir, TableUtils.isExternal(ret));
        // If they don't match, it was reMapped!
//        boolean reMapped = !relativeDir.equals(mappedDir);
        if (glmMapping.isMapped()) {
            tableMirror.setReMapped(Boolean.TRUE);
        } else {

            // No GLM was applied, so we need to check the location to ensure it was originally
            //   under the default table location.  If it's not and we're asking for distcp,
            //   we need to fail the process because a distcp can't be determined.
            if (!originalLocation.startsWith(originalTableLocation)) {
                // This means the location isn't standard.
                if (config.getTransfer().getStorageMigration().isDistcp()) {
                    // We need to throw an error here.  The location isn't standard and we can't determine the mapping.
                    tableMirror.setPhaseState(PhaseState.ERROR);
                    throw new MismatchException("Location Mapping can't be determined.  No matching `glm` entry to make translation." +
                            "Original Location: " + originalLocation + "which doesn't align with the original table location " +
                            originalTableLocation + " and ALIGNED with DISTCP can't be determined.");
                }
            }


            // under conditions like, STORAGE_MIGRATION, same namespace, !rdl and glm we need to ensure ALL locations are
            //   mapped...  If they aren't, they won't be moved as the translation wouldn't change.  So we need to throw
            //   an error that ensures the table fails to process.

            // Get the Namespace from the original table location.
            String origNamespace = NamespaceUtils.getNamespace(originalLocation);

            if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                    && origNamespace.equals(config.getTransfer().getTargetNamespace())) {
                tableMirror.setPhaseState(PhaseState.ERROR);
                throw new MissingDataPointException("Location Mapping can't be determined.  No matching `glm` entry to make translation." +
                        "Original Location: " + originalLocation);
            }
        }
        // Feature Off.  Basic translation which includes any GlobalLocationMaps.
        String newLocation = null;
        StringBuilder sbDir = new StringBuilder();

        Warehouse warehouse = warehouseService.getWarehousePlan(originalDatabase);
        EnvironmentTable checkEnvTbl = tableMirror.getEnvironmentTable(Environment.RIGHT);

        if (glmMapping.isMapped()) {
            sbDir.append(targetNamespace);
            sbDir.append(glmMapping.getMappedDir());
        } else if (config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.ALIGNED) {
            if (isNull(checkEnvTbl) || checkEnvTbl.getDefinition().isEmpty()) {
                checkEnvTbl = tableMirror.getEnvironmentTable(Environment.LEFT);
            }
            if (TableUtils.isManaged(checkEnvTbl)) {
                if (tableMirror.getParent().getProperty(Environment.RIGHT, DB_MANAGED_LOCATION) != null) {
                    // Assumed that the location here has the appropriate namespace.
                    sbDir.append(tableMirror.getParent().getProperty(Environment.RIGHT, DB_MANAGED_LOCATION));
                } else {
                    sbDir.append(targetNamespace);
                    sbDir.append(warehouse.getManagedDirectory());
                    sbDir.append("/");
                    sbDir.append(targetDatabaseManagedDir);
                }
            } else if (TableUtils.isExternal(checkEnvTbl)) {
                if (tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION) != null) {
                    // Assumed that the location here has the appropriate namespace.
                    sbDir.append(tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION));
                } else {
                    sbDir.append(targetNamespace);
                    sbDir.append(warehouse.getExternalDirectory());
                    sbDir.append("/");
                    sbDir.append(targetDatabaseManagedDir);
                }
            } else {
                // TODO: Shouldn't happen.
            }
            sbDir.append("/");
            sbDir.append(tableName);
            if (partitionSpec != null)
                sbDir.append("/").append(partitionSpec);

        } else {
            switch (config.getDataStrategy()) {
                case EXPORT_IMPORT:
                case HYBRID:
                case SQL:
                case SCHEMA_ONLY:
                case DUMP:
                case STORAGE_MIGRATION:
                case CONVERT_LINKED:
                    sbDir.append(targetNamespace);
                    relativeDir = relativeDir.replace(originalDatabase, targetDatabase);
                    sbDir.append(relativeDir);
                    break;
                case LINKED:
                case COMMON:
                    // TODO: Work to do here!!!
                    newLocation = originalLocation;
                    break;
            }
        }
        newLocation = sbDir.toString();
        if (glmMapping.isMapped()) {
            tableMirror.addIssue(Environment.RIGHT, "GLM applied.  Original Location: " +
                    glmMapping.getOriginalDir() + " Mapped Location: " + glmMapping.getMappedDir());
        }
        // Check and warn against table location (for external tables) not aligning with the
        //   set db location.
        String testRelativeDir = NamespaceUtils.stripNamespace(newLocation);
        String checkType = nonNull(partitionSpec) ? "partition" : "table";
        if (TableUtils.isExternal(checkEnvTbl)) {
            String dbExtDir = tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
            if (!isBlank(dbExtDir)) {
                dbExtDir = NamespaceUtils.stripNamespace(dbExtDir);
                if (!testRelativeDir.startsWith(dbExtDir)) {
                    // Set warning that even though you've specified to warehouse directories, the current configuration
                    // will NOT place it in that directory.
                    String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), checkType,
                            testRelativeDir, dbExtDir);
                    tableMirror.addIssue(Environment.RIGHT, msg);
                }
            }
        } else {
            String lclLoc = tableMirror.getParent().getProperty(Environment.RIGHT, DB_MANAGED_LOCATION);
            if (!isBlank(lclLoc) && !newLocation.startsWith(lclLoc)) {
                // Set warning that even though you've specified to warehouse directories, the current configuration
                // will NOT place it in that directory.
                String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), checkType,
                        lclLoc, newLocation);
                tableMirror.addIssue(Environment.RIGHT, msg);
            }
        }

        log.debug("Translate Location: {}: {}", originalLocation, newLocation);
        // Add Location Map for table to a list.
        // TODO: Need to handle RIGHT locations.

        boolean consolidateSourceTables = config.getTransfer().getStorageMigration().isConsolidateTablesForDistcp();
        if (config.getTransfer().getStorageMigration().isDistcp()
                && config.getDataStrategy() != DataStrategyEnum.SQL) {
            if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
                config.getTranslator().addTranslation(originalDatabase, Environment.LEFT, originalLocation, newLocation, level, consolidateSourceTables);
            } else if (config.getTransfer().getStorageMigration().getDataFlow() == DistcpFlowEnum.PULL && !config.isFlip()) {
                config.getTranslator().addTranslation(originalDatabase, Environment.RIGHT, originalLocation, newLocation, level, consolidateSourceTables);
            } else {
                config.getTranslator().addTranslation(originalDatabase, Environment.LEFT, originalLocation, newLocation, level, consolidateSourceTables);
            }
        }

        return newLocation;
    }

    public void addGlobalLocationMap(TableType type, String source, String target) throws SessionException {
        // Don't reload if running.
        executeSessionService.closeSession();

        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        hmsMirrorConfig.getTranslator().addUserGlobalLocationMap(type, source, target);
    }

    public void removeGlobalLocationMap(String source, TableType type) throws SessionException {
        // Don't reload if running.
        executeSessionService.closeSession();

        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        hmsMirrorConfig.getTranslator().removeUserGlobalLocationMap(source, type);
    }

    public Map<String, Map<TableType, String>> getGlobalLocationMap() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        return hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap();
    }

    /*
    This has to run after the Database details have been collected so we can look at the database location details and
    construct the proper locations for the databases.
     */
    public Map<String, Map<TableType, String>> buildGlobalLocationMapFromWarehousePlansAndSources(boolean dryrun, int consolidationLevel) throws MismatchException, SessionException {

        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        Conversion conversion = session.getConversion();

        // We need to know if we are dealing with potential conversions (IE: Legacy Hive Managed to External)
        // If we are, we need to ensure that there are GLM's built for Managed Tables into External Locations.
        // This is because the location will be different and we need to ensure that the location is translated correctly.
        boolean conversions = HmsMirrorConfigUtil.possibleConversions(config);

        Translator translator = config.getTranslator();
        Map<String, Map<TableType, String>> lclGlobalLocationMap = new TreeMap<>(new StringLengthComparator());

        WarehouseMapBuilder warehouseMapBuilder = translator.getWarehouseMapBuilder();
        Map<String, Warehouse> warehousePlans = warehouseMapBuilder.getWarehousePlans();

        // Checks to see if we should move forward.
        if (isNull(warehousePlans) || warehousePlans.isEmpty()) {
            log.info("No warehouse plans available to build glm's from sources");
            return lclGlobalLocationMap;
        }
        Map<String, SourceLocationMap> sources = translator.getWarehouseMapBuilder().getSources();
        if (isNull(sources) || sources.isEmpty()) {
            log.info("No sources available to build glm's from warehouse plans");
            return lclGlobalLocationMap;
        }

        /*
         What do we have:
         1. Warehouse Plans (database, <external, managed>)
         2. Sources (database, <table_type, <location, tables>>)

        Build these by the database. So use the 'warehousePlans' as the base. For each warehouse plan
        database, get the external and managed locations.

        Now get the sources for the database.  For each *table type* go through the locations (reduce the location by the consolidation level).
            - The current level in the location is the database location, most likely. So by default, we should
                reduce the location by 1.
            - If the location is the equal to or starts with the warehouse location, then we don't need a glm for this.
            - If the location is different and they have specified `reset-to-default-location` then we need to add a glm
                for this that is the location (reduced by consolidation level) and the warehouse location.


        */

        for (Map.Entry<String, Warehouse> warehouseEntry : warehousePlans.entrySet()) {
            String database = HmsMirrorConfigUtil.getResolvedDB(warehouseEntry.getKey(), config);
            Warehouse warehouse = warehouseEntry.getValue();
            String externalBaseLocation = warehouse.getExternalDirectory();
            String managedBaseLocation = warehouse.getManagedDirectory();
            SourceLocationMap sourceLocationMap = sources.get(database);
            DBMirror dbMirror = conversion.getDatabase(warehouseEntry.getKey());
            if (sourceLocationMap != null && dbMirror != null) {
                for (Map.Entry<TableType, Map<String, Set<String>>> sourceLocationEntry : sourceLocationMap.getLocations().entrySet()) {
                    String typeTargetLocation = null;
                    String extTargetLocation = new String(externalBaseLocation + "/" + dbMirror.getLocationDirectory());
                    String mngdTargetLocation = new String(managedBaseLocation + "/" + dbMirror.getManagedLocationDirectory());

                    // Locations and the tables that are in that location.
                    for (Map.Entry<String, Set<String>> sourceLocationSet : sourceLocationEntry.getValue().entrySet()) {
                        String sourceLocation = new String(sourceLocationSet.getKey());
                        // Strip the namespace from the location.
                        sourceLocation = NamespaceUtils.stripNamespace(sourceLocation); //.replace(hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace(), "");

                        // NOTE: The locations were already reduced by '1' when the Sources were built.
                        //       This removed the 'table' directory from the location and allows for them to
                        //       by normalized to the database directory.
                        String reducedLocation = UrlUtils.reduceUrlBy(sourceLocation, 0);

                        if (sourceLocationEntry.getKey() == TableType.EXTERNAL_TABLE) {
                            if (!sourceLocation.startsWith(extTargetLocation)) {
                                // Get current entry
                                Map<TableType, String> currentEntry = lclGlobalLocationMap.get(reducedLocation);
                                if (isNull(currentEntry)) {
                                    currentEntry = new TreeMap<>();
                                    lclGlobalLocationMap.put(reducedLocation, currentEntry);
                                }
                                currentEntry.put(sourceLocationEntry.getKey(), extTargetLocation);
                            }
                        }
                        if (sourceLocationEntry.getKey() == TableType.MANAGED_TABLE) {
                            if (!sourceLocation.startsWith(mngdTargetLocation)) {
                                // Get current entry
                                Map<TableType, String> currentEntry = lclGlobalLocationMap.get(reducedLocation);
                                if (isNull(currentEntry)) {
                                    currentEntry = new TreeMap<>();
                                    lclGlobalLocationMap.put(reducedLocation, currentEntry);
                                }
                                currentEntry.put(sourceLocationEntry.getKey(), mngdTargetLocation);
                                // When we have conversions, we need to ensure that the managed location is also added.
                                if (conversions) {
                                    currentEntry.put(TableType.EXTERNAL_TABLE, extTargetLocation);
                                }
                            }

                        }
                    }
                }
            }
        }
        if (!dryrun) {
            translator.setAutoGlobalLocationMap(lclGlobalLocationMap);
            translator.rebuildOrderedGlobalLocationMap();
        }
        return lclGlobalLocationMap;
    }
}
