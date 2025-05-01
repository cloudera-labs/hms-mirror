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
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.cloudera.utils.hms.mirror.MessageCode.LOCATION_NOT_MATCH_WAREHOUSE;
import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * The TranslatorService class is responsible for managing and translating location mappings
 * for table partitions and global locations within a database warehouse environment.
 * It provides methods to handle migrations, mappings, and validations associated with
 * database table locations and partitions.
 */
@Service
@Slf4j
public class TranslatorService {

    private final ExecuteSessionService executeSessionService;
    private final WarehouseService warehouseService;
    // keeping the log field is not needed if using @Slf4j from Lombok

    public TranslatorService(ExecuteSessionService executeSessionService,
                             WarehouseService warehouseService) {
        this.executeSessionService = executeSessionService;
        this.warehouseService = warehouseService;
    }

    /**
     * Builds a SQL statement for adding partitions to a table based on the given
     * environment table's partition specifications and locations.
     *
     * @param environmentTable the environment table containing partition details
     *                         and their respective locations
     * @return a string representing the partition addition SQL statement
     */
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

    /**
     * Processes the global location map to determine a new location or mapping for the given original location.
     * This method checks the original location against a global location map to apply transformations or replacements
     * specific to the table type (external or managed).
     *
     * @param originalLocation The original location path to be processed.
     * @param externalTable    Indicates whether the table is an external table (true) or a managed table (false).
     * @return A {@code GLMResult} object containing the original directory, mapped directory,
     * and whether the location mapping was successfully applied.
     */
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

    /**
     * Translates the partition locations for a given table mirror, adjusting the namespace in the target environment
     * to match the resolved configuration settings. Updates the partition locations in the target environment table
     * where applicable.
     *
     * @param tblMirror The table mirror object containing the source and target environment tables, as well as
     *                  necessary metadata for translation.
     * @return A Boolean indicating the success or failure of the partition location translation. Returns true
     * if all partition locations were translated successfully; false if one or more partitions could not
     * be translated.
     * @throws RequiredConfigurationException If required configuration settings are not properly set or are missing.
     * @throws MissingDataPointException      If required data points needed for location translation are absent.
     * @throws MismatchException              If there is any mismatch between expected and actual data during the translation process.
     */
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
                    String[] spec = partSpec.split("/");
                    int level = spec.length + 1;
                    // Increase level to the table, since we're not filter any tables.  It's assumed that
                    //   we're pulling the whole DB.
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

    /**
     * Translates the table location from the original environment to the target environment,
     * taking into account various configurations, mappings, and potential transformations
     * such as storage migrations and partition specifications.
     *
     * @param tableMirror      the {@link TableMirror} object representing the table whose location
     *                         is being translated, including metadata for both source and target environments.
     * @param originalLocation the original location of the table or partition in the source environment.
     * @param level            an integer indicating the level or depth of the operation, used for hierarchical processing
     *                         or recursive calls.
     * @param partitionSpec    an optional string specifying the partition details if the translation is for
     *                         a specific partition rather than the entire table; it may be null for table-wide translations.
     * @return the translated table or partition location as a string, following the rules and mappings
     * defined by the system configuration and migration logic.
     * @throws MismatchException              if a mismatch in expected configurations or attributes is detected during translation.
     * @throws MissingDataPointException      if required data points for determining the translation are missing or incomplete.
     * @throws RequiredConfigurationException if configurations required for translating the location are not provided or invalid.
     */
    public String translateTableLocation(TableMirror tableMirror, String originalLocation,
                                         int level, String partitionSpec)
            throws MismatchException, MissingDataPointException, RequiredConfigurationException {

        String tableName = tableMirror.getName();
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        EnvironmentTable targetEnvTable = tableMirror.getEnvironmentTable(Environment.RIGHT);
        String originalDatabase = tableMirror.getParent().getName();
        String targetDatabase = HmsMirrorConfigUtil.getResolvedDB(originalDatabase, config);
        String targetDatabaseDir = getOrDefault(tableMirror.getParent().getLocationDirectory(), targetDatabase + ".db");
        String targetDatabaseManagedDir = getOrDefault(tableMirror.getParent().getManagedLocationDirectory(), targetDatabase + ".db");
        String originalTableLocation = TableUtils.getLocation(tableName, tableMirror.getEnvironmentTable(Environment.LEFT).getDefinition());
        String targetNamespace = config.getTargetNamespace();
        String relativeDir = NamespaceUtils.stripNamespace(originalLocation);

        GLMResult glmMapping = processGlobalLocationMap(relativeDir, TableUtils.isExternal(targetEnvTable));

        if (glmMapping.isMapped()) {
            tableMirror.setReMapped(Boolean.TRUE);
        } else {
            handleNoGlmMapping(originalLocation, originalTableLocation, tableMirror, config);
            handleStorageMigrationWithoutGlm(originalLocation, config, tableMirror);
        }

        String newLocation = computeNewLocation(
                glmMapping, targetNamespace, partitionSpec, tableMirror, config,
                targetDatabaseManagedDir, tableName, originalDatabase, targetDatabase,
                relativeDir, originalLocation, targetEnvTable
        );

        maybeAddGlmIssue(tableMirror, glmMapping);

        String testRelativeDir = NamespaceUtils.stripNamespace(newLocation);
        String checkType = partitionSpec != null ? "partition" : "table";

        warnIfLocationMismatch(tableMirror, testRelativeDir, checkType, newLocation, partitionSpec, targetEnvTable);

        log.debug("Translate Location: {}: {}", originalLocation, newLocation);

        addTranslationIfRequired(config, originalDatabase, originalLocation,
                newLocation, level, config.getTransfer().getStorageMigration().isConsolidateTablesForDistcp(), tableMirror);

        return newLocation;
    }

    // --- Helper methods for improved modularity ---

    private String getOrDefault(String value, String defaultValue) {
        return value != null ? value : defaultValue;
    }

    /**
     * Handles scenarios where no GLM (Global Location Mapping) can be determined for the given locations.
     *
     * @param originalLocation      the original location of the table data
     * @param originalTableLocation the base location of the original table
     * @param tableMirror           the table mirror object containing information about the table's state and configuration
     * @param config                the HmsMirrorConfig object holding the configuration details, including transfer and storage migration options
     * @throws MismatchException if the original location does not start with the original table location
     *                           and cannot be aligned using the GLM mapping in a DISTCP configuration
     */
    private void handleNoGlmMapping(String originalLocation, String originalTableLocation,
                                    TableMirror tableMirror, HmsMirrorConfig config) throws MismatchException {

        if (!originalLocation.startsWith(originalTableLocation)) {
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                tableMirror.setPhaseState(PhaseState.ERROR);
                throw new MismatchException("Location Mapping can't be determined.  No matching `glm` entry to make translation." +
                        "Original Location: " + originalLocation + " which doesn't align with the original table location " +
                        originalTableLocation + " and ALIGNED with DISTCP can't be determined.");
            }
        }
    }

    /**
     * Handles a storage migration process when the Global Location Mapping (GLM) is not provided or applicable.
     * This method ensures the migration adheres to specific constraints and throws an exception if a proper
     * location mapping cannot be determined.
     *
     * @param originalLocation The original storage location of the data.
     * @param config           The configuration object containing settings and parameters for the migration process.
     * @param tableMirror      Object representing the mirrored table, where phase state can be updated in case of errors.
     * @throws MissingDataPointException If the location mapping cannot be determined due to a missing or invalid GLM entry.
     */
    private void handleStorageMigrationWithoutGlm(String originalLocation, HmsMirrorConfig config, TableMirror tableMirror)
            throws MissingDataPointException {

        String origNamespace = NamespaceUtils.getNamespace(originalLocation);
        if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                && origNamespace.equals(config.getTransfer().getTargetNamespace())) {
            tableMirror.setPhaseState(PhaseState.ERROR);
            throw new MissingDataPointException("Location Mapping can't be determined.  No matching `glm` entry to make translation." +
                    "Original Location: " + originalLocation);
        }
    }

    /**
     * Computes the new location for a table or a partition based on various configuration parameters,
     * table characteristics, and namespace mappings.
     *
     * @param glmMapping               The GLMResult object containing mapping details for namespaces and directories.
     * @param targetNamespace          The namespace of the target environment.
     * @param partitionSpec            The partition specification, if applicable, for the table.
     * @param tableMirror              The TableMirror object that encapsulates table information across environments.
     * @param config                   The HmsMirrorConfig object providing configuration options for migration and translation.
     * @param targetDatabaseManagedDir The managed directory of the target database environment.
     * @param tableName                The name of the table being migrated.
     * @param originalDatabase         The name of the original database.
     * @param targetDatabase           The name of the target database.
     * @param relativeDir              The relative directory path from the source environment.
     * @param originalLocation         The original location of the table within the source environment.
     * @param checkEnvTbl              The environment-specific table information used for validation.
     * @return The computed location as a String where the table or partition will reside in the target environment.
     * @throws MissingDataPointException If a required data point for computing the location is missing.
     */
    private String computeNewLocation(
            GLMResult glmMapping,
            String targetNamespace,
            String partitionSpec,
            TableMirror tableMirror,
            HmsMirrorConfig config,
            String targetDatabaseManagedDir,
            String tableName,
            String originalDatabase,
            String targetDatabase,
            String relativeDir,
            String originalLocation,
            EnvironmentTable checkEnvTbl
    ) throws MissingDataPointException {
        StringBuilder sbDir = new StringBuilder();
        Warehouse warehouse = warehouseService.getWarehousePlan(tableMirror.getParent().getName());
        if (glmMapping.isMapped()) {
            sbDir.append(targetNamespace).append(glmMapping.getMappedDir());
        } else if (config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.ALIGNED) {
            if (checkEnvTbl == null || checkEnvTbl.getDefinition().isEmpty()) {
                checkEnvTbl = tableMirror.getEnvironmentTable(Environment.LEFT);
            }
            if (TableUtils.isManaged(checkEnvTbl)) {
                String managedLoc = tableMirror.getParent().getProperty(Environment.RIGHT, DB_MANAGED_LOCATION);
                if (managedLoc != null) {
                    sbDir.append(managedLoc);
                } else {
                    sbDir.append(targetNamespace)
                            .append(warehouse.getManagedDirectory())
                            .append("/")
                            .append(targetDatabaseManagedDir);
                }
            } else if (TableUtils.isExternal(checkEnvTbl)) {
                String dbLoc = tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
                if (dbLoc != null) {
                    sbDir.append(dbLoc);
                } else {
                    sbDir.append(targetNamespace)
                            .append(warehouse.getExternalDirectory())
                            .append("/")
                            .append(targetDatabaseManagedDir);
                }
            }
            sbDir.append("/").append(tableName);
            if (partitionSpec != null) {
                sbDir.append("/").append(partitionSpec);
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
                    sbDir.append(targetNamespace);
                    String patchedDir = relativeDir.replace(originalDatabase, targetDatabase);
                    sbDir.append(patchedDir);
                    break;
                case LINKED:
                case COMMON:
                    return originalLocation;
            }
        }
        return sbDir.toString();
    }

    /**
     * Adds a GLM issue to the specified table mirror if the GLM result indicates a mapping exists.
     *
     * @param tableMirror the table mirror to which the issue may be added
     * @param glmMapping  the GLM result containing the mapping status and directory information
     */
    private void maybeAddGlmIssue(TableMirror tableMirror, GLMResult glmMapping) {
        if (glmMapping.isMapped()) {
            tableMirror.addIssue(Environment.RIGHT, "GLM applied. Original Location: " +
                    glmMapping.getOriginalDir() + " Mapped Location: " + glmMapping.getMappedDir());
        }
    }

    /**
     * Warns if there is a mismatch between the expected location and the actual location
     * of a table or partition based on its environment type (external or managed).
     *
     * @param tableMirror     the TableMirror instance containing metadata about the table and its parent environment
     * @param testRelativeDir the relative directory path being tested
     * @param checkType       the type of check being performed (e.g., table or partition)
     * @param newLocation     the new location of the table or partition to validate against
     * @param partitionSpec   the partition specification (if applicable) being validated
     * @param checkEnvTbl     the environment table metadata to determine environment type (external or managed)
     */
    private void warnIfLocationMismatch(TableMirror tableMirror, String testRelativeDir,
                                        String checkType, String newLocation, String partitionSpec, EnvironmentTable checkEnvTbl) {

        if (TableUtils.isExternal(checkEnvTbl)) {
            String dbExtDir = tableMirror.getParent().getProperty(Environment.RIGHT, DB_LOCATION);
            if (!isBlank(dbExtDir)) {
                dbExtDir = NamespaceUtils.stripNamespace(dbExtDir);
                if (!testRelativeDir.startsWith(dbExtDir)) {
                    String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), checkType,
                            testRelativeDir, dbExtDir);
                    tableMirror.addIssue(Environment.RIGHT, msg);
                }
            }
        } else {
            String managedLoc = tableMirror.getParent().getProperty(Environment.RIGHT, DB_MANAGED_LOCATION);
            if (!isBlank(managedLoc) && !newLocation.startsWith(managedLoc)) {
                String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), checkType,
                        managedLoc, newLocation);
                tableMirror.addIssue(Environment.RIGHT, msg);
            }
        }
    }

    /**
     * Adds a translation entry to the translator if storage migration with distcp is enabled
     * and the data strategy is not SQL. It determines the environment and translation details
     * based on the specified configuration and parameters.
     *
     * @param config                  the HmsMirrorConfig object containing the migration configurations
     * @param originalDatabase        the name of the original database
     * @param originalLocation        the location of the original data
     * @param newLocation             the new target location for the data
     * @param level                   the level of the migration process
     * @param consolidateSourceTables a flag indicating whether source tables should be consolidated
     * @param tableMirror             the TableMirror instance representing the current table migration process
     */
    private void addTranslationIfRequired(HmsMirrorConfig config, String originalDatabase,
                                          String originalLocation, String newLocation, int level,
                                          boolean consolidateSourceTables, TableMirror tableMirror) {

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
    }

    /**
     * Adds a global location map entry to the translator configuration within the HMS mirror session.
     * This method updates the user-defined global location mappings for the specified table type
     * by adding the source and target locations.
     *
     * @param type   the type of table for which the location map will be added
     * @param source the source location to be mapped
     * @param target the target location to map to
     * @throws SessionException if an error occurs while closing the session or accessing the session configuration
     */
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

    /**
     * Constructs a Global Location Map (GLM) from the provided warehouse plans and source mappings.
     * This method generates the location mappings for databases based on their current configurations,
     * considering table types (e.g., managed or external tables) and consolidation levels.
     * <p>
     * This has to run after the Database details have been collected so we can look at the database location details and
     * construct the proper locations for the databases.
     *
     * @param dryrun             If true, performs a dry run without applying changes.
     * @param consolidationLevel The level of consolidation to apply when reducing locations.
     * @return A map of database locations, where keys are normalized database locations,
     * and values are maps of table types to their corresponding target paths.
     * @throws MismatchException If there are mismatches in the warehouse plans or source mappings.
     * @throws SessionException  If there are issues with the execution session.
     */ /*

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
                    String extTargetLocation = externalBaseLocation + "/" + dbMirror.getLocationDirectory();
                    String mngdTargetLocation = managedBaseLocation + "/" + dbMirror.getManagedLocationDirectory();

                    // Locations and the tables that are in that location.
                    for (Map.Entry<String, Set<String>> sourceLocationSet : sourceLocationEntry.getValue().entrySet()) {
                        String sourceLocation = sourceLocationSet.getKey();
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
        translator.setAutoGlobalLocationMap(lclGlobalLocationMap);
        translator.rebuildOrderedGlobalLocationMap();

        return lclGlobalLocationMap;
    }
}