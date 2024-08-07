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

import com.cloudera.utils.hms.mirror.EnvironmentMap;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
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
//    @Getter
//    private ConfigService configService = null;

    //    private ConfigService configService;
    private DatabaseService databaseService;

//    @Autowired
//    public void setConfigService(ConfigService configService) {
//        this.configService = configService;
//    }

    /**
     * @param consolidationLevel how far up the directory hierarchy to go to build the distcp list based on the sources
     *                           provided.
     * @return A map of databases.  Each database will have a map that has 1 or more 'targets' and 'x' sources for each
     * target.
     */
    public synchronized Map<String, Map<String, Set<String>>> buildDistcpList(String database, Environment environment, int consolidationLevel) {
        Map<String, Map<String, Set<String>>> rtn = new TreeMap<>();

        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        // get the map for a db.
        Set<String> databases = hmsMirrorConfig.getTranslator().getTranslationMap().keySet();

        // get the map.entry
        Map<String, Set<String>> reverseMap = new TreeMap<>();
        // Get a static view of set to avoid concurrent modification.
        Set<EnvironmentMap.TranslationLevel> dbTranslationLevel =
                new HashSet<>(hmsMirrorConfig.getTranslator().getTranslationMap(database, environment));

        Map<String, String> dbLocationMap = new TreeMap<>();

        for (EnvironmentMap.TranslationLevel translationLevel : dbTranslationLevel) {
            if (translationLevel.getOriginal() != null &&
                    translationLevel.getTarget() != null) {
                dbLocationMap.put(translationLevel.getAdjustedOriginal(), translationLevel.getAdjustedTarget());
            }
        }

        for (Map.Entry<String, String> entry : dbLocationMap.entrySet()) {
            // reduce folder level by 'consolidationLevel' for key and value.
            // Source
            String reducedSource = UrlUtils.reduceUrlBy(entry.getKey(), consolidationLevel);
            // Target
            String reducedTarget = UrlUtils.reduceUrlBy(entry.getValue(), consolidationLevel);

            if (reverseMap.get(reducedTarget) != null) {
                reverseMap.get(reducedTarget).add(entry.getKey());
            } else {
                Set<String> sourceSet = new TreeSet<String>();
                sourceSet.add(entry.getKey());
                reverseMap.put(reducedTarget, sourceSet);
            }

        }
        if (!reverseMap.isEmpty()) {
            rtn.put(database, reverseMap);
        }
        return rtn;
    }

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

    /*
    TODO: Need to ensure that an "EXTERNAL" location is set in EVERY entry in-order for this to work.
     */
    public String processGlobalLocationMap(String originalLocation, Boolean externalTable) {
        // Set to original, so we capture the original location if we don't find a match.
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
                    } else {
                        rLoc = rLocMap.get(TableType.MANAGED_TABLE);
                        if (nonNull(rLoc)) {
                            newLocation = rLoc + originalLocation.replace(key, "");
                        }
                    }
                    log.info("Location Map Found. {}:{} New Location: {}", key, rLoc, newLocation);
                    break;
                }
            }
        }
        return newLocation;
    }

    @Autowired
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

//    public Warehouse getDatabaseWarehouse(String database) throws MissingDataPointException {
//        Warehouse dbWarehouse = null;
//        ExecuteSession session = executeSessionService.getSession();
//        HmsMirrorConfig config = session.getConfig();
//        dbWarehouse = config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().get(database);
//        if (isNull(dbWarehouse)) {
//            if (config.getTransfer().getWarehouse().getManagedDirectory() != null &&
//                    config.getTransfer().getWarehouse().getExternalDirectory() != null) {
//                dbWarehouse = new Warehouse(config.getTransfer().getWarehouse().getExternalDirectory(),
//                        config.getTransfer().getWarehouse().getManagedDirectory());
//            }
//        }
//        if (isNull(dbWarehouse)) {
//            // Look for Location in the right DB Definition for Migration Strategies.
//            switch (config.getDataStrategy()) {
//                case SCHEMA_ONLY:
//                case EXPORT_IMPORT:
//                case HYBRID:
//                case SQL:
//                case COMMON:
//                case LINKED:
//                    if (nonNull(config.getCluster(Environment.RIGHT).getEnvVars())) {
//                        String extDir = config.getCluster(Environment.RIGHT).getEnvVars().get(EXT_DB_LOCATION_PROP);
//                        String manDir = config.getCluster(Environment.RIGHT).getEnvVars().get(MNGD_DB_LOCATION_PROP);
//                        if (extDir != null && manDir != null) {
//                            dbWarehouse = new Warehouse(extDir, manDir);
//                            session.addWarning(WAREHOUSE_DIRECTORIES_RETRIEVED_FROM_HIVE_ENV);
//                        } else {
//                            session.addError(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
//                            throw new MissingDataPointException("Couldn't find a Warehouse Plan for database: " + database +
//                                    ". The global warehouse locations aren't defined either.  Please define a warehouse plan or " +
//                                    "set the global warehouse locations.");
//                        }
//                    } else {
//                        session.addError(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
//                        throw new MissingDataPointException("Couldn't find a Warehouse Plan for database: " + database +
//                                ". The global warehouse locations aren't defined either.  Please define a warehouse plan or " +
//                                "set the global warehouse locations.");
//                    }
//                    break;
//                default: // STORAGE_MIGRATION should set these manually.
//                    session.addError(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
//                    throw new MissingDataPointException("Couldn't find a Warehouse Plan for database: " + database +
//                            ". The global warehouse locations aren't defined either.  Please define a warehouse plan or " +
//                            "set the global warehouse locations.");
//            }
//        }
//        return dbWarehouse;
//    }

    public Boolean translatePartitionLocations(TableMirror tblMirror) throws RequiredConfigurationException, MissingDataPointException, MismatchException {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

//        Warehouse warehouse = databaseService.getWarehousePlan(tblMirror.getParent().getName());

//        Map<String, String> dbRef = tblMirror.getParent().getDBDefinition(Environment.RIGHT);
//        Boolean chkLocation = config.getTransfer().getWarehouse().getManagedDirectory() != null && config.getTransfer().getWarehouse().getExternalDirectory() != null;
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
                    int level = StringUtils.countMatches(partSpec, "/") + 1;
                    // Increase level to the table, since we're not filter any tables.  It's assumed that
                    //   we're pulling the whole DB.
                    if (!config.getFilter().isTableFiltering()) {
                        level++;
                    }
                    if (isBlank(partitionLocation) || partitionLocation.isEmpty() ||
                            partitionLocation.equals(NOT_SET)) {
                        rtn = Boolean.FALSE;
                        continue;
                    }

//                    // Get the relative dir.
//                    String relativeDir = NamespaceUtils.stripNamespace(partitionLocation);//.replace(config.getCluster(Environment.LEFT).getHcfsNamespace(), "");
//                    // Check the Global Location Map for a match.
//                    String mappedDir = processGlobalLocationMap(relativeDir, isExternal);
//                    if (relativeDir.equals(mappedDir)) {
//                        String errMsg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition", entry.getKey(), entry.getValue());
//                        tblMirror.addIssue(Environment.RIGHT, errMsg);
//                    }
//                    // Check for 'common storage'
//                    mappedDir = mappedDir.replace(originalDatabase, targetDatabase);
//
//                    String newPartitionLocation = config.getTargetNamespace() + mappedDir;

                    String newPartitionLocation = translateLocation(tblMirror, partitionLocation, level, partSpec);

                    entry.setValue(newPartitionLocation);
                    // For distcp.
//                    config.getTranslator().addTranslation(originalDatabase, Environment.RIGHT, partitionLocation,
//                            newPartitionLocation, ++level);

                    // Check and warn against warehouse locations if specified.
//                    if (config.getTransfer().getWarehouse().getExternalDirectory() != null &&
//                            config.getTransfer().getWarehouse().getManagedDirectory() != null) {
//                    if (TableUtils.isExternal(tblMirror.getEnvironmentTable(Environment.LEFT))) {
//                        // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
//                        String lclLoc = tblMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
//                        if (!isBlank(lclLoc) && !newPartitionLocation.startsWith(lclLoc)) {
//                            // Set warning that even though you've specified to warehouse directories, the current configuration
//                            // will NOT place it in that directory.
//                            String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
//                                    lclLoc, newPartitionLocation);
//                            tblMirror.addIssue(Environment.RIGHT, msg);
//                        }
//                    } else {
//                        String lclLoc = tblMirror.getParent().getProperty(Environment.RIGHT, DB_MANAGED_LOCATION);
//                        if (!isBlank(lclLoc) && !newPartitionLocation.startsWith(lclLoc)) {
//                            // Set warning that even though you've specified to warehouse directories, the current configuration
//                            // will NOT place it in that directory.
//                            String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
//                                    lclLoc, newPartitionLocation);
//                            tblMirror.addIssue(Environment.RIGHT, msg);
//                        }
//                    }
//                    }

                }
            }
            // end partitions location conversion.
        }
        return rtn;
    }

    public String translateLocation(TableMirror tableMirror, String originalLocation,
                                    int level, String partitionSpec)
            throws MismatchException, MissingDataPointException, RequiredConfigurationException {
        String rtn = originalLocation;

        String tableName = tableMirror.getName();
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        String originalDatabase = tableMirror.getParent().getName();
        String targetDatabase = HmsMirrorConfigUtil.getResolvedDB(originalDatabase, config);
        String originalTableLocation = TableUtils.getLocation(tableName, tableMirror.getEnvironmentTable(Environment.LEFT).getDefinition());

        String targetNamespace = config.getTargetNamespace();

        String relativeDir = NamespaceUtils.stripNamespace(rtn);

        // Check the Global Location Map for a match.
        String mappedDir = processGlobalLocationMap(relativeDir, TableUtils.isExternal(ret));
        // If they don't match, it was reMapped!
        boolean reMapped = !relativeDir.equals(mappedDir);
        if (reMapped) {
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
//                tableMirror.addIssue(Environment.LEFT, "Location Mapping can't be determined.  No matching `glm` entry to make translation." +
//                        "Original Location: " + originalLocation);
                tableMirror.setPhaseState(PhaseState.ERROR);
                throw new MissingDataPointException("Location Mapping can't be determined.  No matching `glm` entry to make translation." +
                        "Original Location: " + originalLocation);
            }
        }
        // Feature Off.  Basic translation which includes any GlobalLocationMaps.
        String newLocation = null;
        StringBuilder sbDir = new StringBuilder();

        sbDir.append(targetNamespace);

        Warehouse warehouse = databaseService.getWarehousePlan(originalDatabase);
        EnvironmentTable checkEnvTbl = tableMirror.getEnvironmentTable(Environment.RIGHT);

        if (reMapped) {
            sbDir.append(mappedDir);
        } else if (config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.ALIGNED) {
            if (isNull(checkEnvTbl) || checkEnvTbl.getDefinition().isEmpty()) {
                checkEnvTbl = tableMirror.getEnvironmentTable(Environment.LEFT);
            }
            if (TableUtils.isManaged(checkEnvTbl)) {
                sbDir.append(warehouse.getManagedDirectory()).append("/");
                sbDir.append(targetDatabase).append(".db").append("/").append(tableName);
                if (partitionSpec != null)
                    sbDir.append("/").append(partitionSpec);
            } else if (TableUtils.isExternal(checkEnvTbl)) {
                sbDir.append(warehouse.getExternalDirectory()).append("/");
                sbDir.append(targetDatabase).append(".db").append("/").append(tableName);
                if (partitionSpec != null)
                    sbDir.append("/").append(partitionSpec);
            } else {
                // TODO: Shouldn't happen.
            }
        } else {
            switch (config.getDataStrategy()) {
                case EXPORT_IMPORT:
                case HYBRID:
                case SQL:
                case SCHEMA_ONLY:
                case DUMP:
                case STORAGE_MIGRATION:
                case CONVERT_LINKED:
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
        if (config.getTransfer().getStorageMigration().isDistcp()
                && config.getDataStrategy() != DataStrategyEnum.SQL) {
            if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
                config.getTranslator().addTranslation(originalDatabase, Environment.LEFT, originalLocation, newLocation, level);
            } else if (config.getTransfer().getStorageMigration().getDataFlow() == DistcpFlowEnum.PULL && !config.isFlip()) {
                config.getTranslator().addTranslation(originalDatabase, Environment.RIGHT, originalLocation, newLocation, level);
            } else {
                config.getTranslator().addTranslation(originalDatabase, Environment.LEFT, originalLocation, newLocation, level);
            }
        }

        return newLocation;
    }

    public void addGlobalLocationMap(TableType type, String source, String target) throws SessionException {
        // Don't reload if running.
        executeSessionService.clearActiveSession();

        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        hmsMirrorConfig.getTranslator().addUserGlobalLocationMap(type, source, target);
    }

    public void removeGlobalLocationMap(String source, TableType type) throws SessionException {
        // Don't reload if running.
        executeSessionService.clearActiveSession();

        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        hmsMirrorConfig.getTranslator().removeUserGlobalLocationMap(source, type);
    }

    public Map<String, Map<TableType, String>> getGlobalLocationMap() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        return hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap();
    }

    public Map<String, Map<TableType, String>> buildGlobalLocationMapFromWarehousePlansAndSources(boolean dryrun, int consolidationLevel) throws MismatchException, SessionException {
        // Don't reload if running.
//        executeSessionService.clearActiveSession();

        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

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
//            String database = warehouseEntry.getKey();
            String database = HmsMirrorConfigUtil.getResolvedDB(warehouseEntry.getKey(), config);
            Warehouse warehouse = warehouseEntry.getValue();
            String externalBaseLocation = warehouse.getExternalDirectory();
            String managedBaseLocation = warehouse.getManagedDirectory();
            SourceLocationMap sourceLocationMap = sources.get(database);
            if (sourceLocationMap != null) {
                for (Map.Entry<TableType, Map<String, Set<String>>> sourceLocationEntry : sourceLocationMap.getLocations().entrySet()) {
                    String typeTargetLocation = null;
                    String extTargetLocation = new String(externalBaseLocation + "/" + database + ".db");
                    String mngdTargetLocation = new String(managedBaseLocation + "/" + database + ".db");
                    // Set the location based on the table type.

//                    switch (sourceLocationEntry.getKey()) {
//                        case EXTERNAL_TABLE:
//                            typeTargetLocation = externalBaseLocation;
//                            break;
//                        case MANAGED_TABLE:
//                            typeTargetLocation = managedBaseLocation;
//                            break;
//                    }
//                    typeTargetLocation = typeTargetLocation + "/" + database + ".db";
                    // Locations and the tables that are in that location.
                    for (Map.Entry<String, Set<String>> sourceLocationSet : sourceLocationEntry.getValue().entrySet()) {
                        String sourceLocation = new String(sourceLocationSet.getKey());
                        // Strip the namespace from the location.
                        sourceLocation = NamespaceUtils.stripNamespace(sourceLocation); //.replace(hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace(), "");
                        // I don't think this is relevant anymore, since we switch the namespace stripping to not required the LEFT hcfsNamespace.
//                        if (config.getTransfer().getStorageMigration().isStrict() && (!sourceLocation.startsWith("/") || (sourceLocation.length() == sourceLocationSet.getKey().length()))) {
//                            // Issue with reducing the location.
//                            // This happens when the table(s) location doesn't match the source namespace.
//                            throw new MismatchException("Location doesn't start with the configured namespace.  This is a problem"
//                                    + " and doesn't allow for the location to be converted to the new namespace."
//                                    + " Location: " + sourceLocationSet.getKey() + " Database: " + database + " Type: " + sourceLocationEntry.getKey()
//                                    + " containing tables: " + String.join(",", sourceLocationSet.getValue())
//                                    + " HCFS Namespace: " + config.getCluster(Environment.LEFT).getHcfsNamespace());
//                        }

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
