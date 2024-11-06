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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.util.DatabaseUtils;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
@Getter
public class DatabaseService {

    private ConnectionPoolService connectionPoolService;
    private ExecuteSessionService executeSessionService;
    private QueryDefinitionsService queryDefinitionsService;
//    private TranslatorService translatorService;
    private WarehouseService warehouseService;
    private ConfigService configService;

    private final List<String> skipList = Arrays.asList(DB_LOCATION, DB_MANAGED_LOCATION, COMMENT, DB_NAME, OWNER_NAME, OWNER_TYPE);

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
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

//    @Autowired
//    public void setTranslatorService(TranslatorService translatorService) {
//        this.translatorService = translatorService;
//    }

    @Autowired
    public void setWarehouseService(WarehouseService warehouseService) {
        this.warehouseService = warehouseService;
    }

    // Look at the Warehouse Plans and pull the database/table/partition locations the metastoreDirect.
    public void buildDatabaseSources(int consolidationLevelBase, boolean partitionLevelMismatch)
            throws RequiredConfigurationException, EncryptionException, SessionException {
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = config.getTranslator().getWarehouseMapBuilder();

        // Don't go through this process if you don't need to.
        if (!config.loadMetadataDetails())
            return;

        if (!warehouseMapBuilder.isInSync()) {
            if (!connectionPoolService.isConnected() && !config.isLoadingTestData()) {
                try {
                    connectionPoolService.init();
                } catch (SQLException e) {
                    log.error("SQL Exception", e);
                    throw new SessionException(e.getMessage());
                }
            }


            // Check to see if there are any warehouse plans defined.  If not, skip this process.
            if (warehouseMapBuilder.getWarehousePlans().isEmpty()) {
                log.warn("No Warehouse Plans defined.  Skipping building out the database sources.");
                return;
            }

//            // Need to have this set to ensure we're picking everything up.
//            if (!config.isEvaluatePartitionLocation()) {
//                throw new RequiredConfigurationException("The 'evaluatePartitionLocation' setting must be set to 'true' to build out the database sources.");
//            }

            for (String database : warehouseMapBuilder.getWarehousePlans().keySet()) {
                // Reset the database in the translation map.
                config.getTranslator().removeDatabaseFromTranslationMap(database);
                // Load the database locations.
                if (!config.isLoadingTestData()) {
                    loadDatabaseLocationMetadataDirect(database, Environment.LEFT, consolidationLevelBase, partitionLevelMismatch);
                } else {
                    // Parse test data for sources.
                    loadDatabaseLocationMetadataFromTestData(database, Environment.LEFT, consolidationLevelBase, partitionLevelMismatch);
                }
            }
            warehouseMapBuilder.setInSync(Boolean.TRUE);
        }
    }

    public Map<String, SourceLocationMap> getDatabaseSources() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        return warehouseMapBuilder.getSources();
    }

    // Load sources from the test data set.
    protected void loadDatabaseLocationMetadataFromTestData(String database, Environment environment,
                                                            int consolidationLevelBase,
                                                            boolean partitionLevelMismatch) {
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();

        Conversion conversion = session.getConversion();

        conversion.getDatabases().forEach((dbName, dbMirror) -> {
            dbMirror.getTableMirrors().forEach((tableName, tableMirror) -> {
                EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
                if (et != null) {
                    String tableType = TableUtils.isExternal(et) ? TableType.EXTERNAL_TABLE.toString() : TableType.MANAGED_TABLE.toString();
                    String tableLocation = TableUtils.getLocation(tableName, et.getDefinition());
//                    String partitionSpec = et.getPartitionSpec();
//                    String partitionLocation = et.getPartitionLocation(
                    // config.getTranslator().addPartitionSource(database, table, tableType, partitionSpec, tableLocation, partitionLocation, consolidationLevelBase, partitionLevelMismatch);
                    config.getTranslator().addPartitionSource(database, tableName, tableType, null,
                            tableLocation, null, consolidationLevelBase, partitionLevelMismatch);
                    tableMirror.getPartitionDefinition(environment).forEach((partSpec, partLoc) -> {
                        config.getTranslator().addPartitionSource(database, tableName, tableType, partSpec,
                                tableLocation, partLoc, consolidationLevelBase, partitionLevelMismatch);
                    });
                }
            });
        });
    }

    protected void loadDatabaseLocationMetadataDirect(String database, Environment environment,
                                                      int consolidationLevelBase,
                                                      boolean partitionLevelMismatch) {
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
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        RunStatus runStatus = session.getRunStatus();

        // TODO: Handle RIGHT Environment. At this point, we're only handling LEFT.
        if (!configService.isMetastoreDirectConfigured(session, environment)) {
            log.info("Metastore Direct Connection is not configured for {}.  Skipping.", environment);
            runStatus.addWarning(METASTORE_TABLE_LOCATIONS_NOT_FETCHED);
            return;
        }

        try {
            conn = getConnectionPoolService().getMetastoreDirectEnvironmentConnection(environment);
            log.info("Loading Partitions from Metastore Direct Connection {}:{}", environment, database);
            QueryDefinitions queryDefinitions = getQueryDefinitionsService().getQueryDefinitions(environment);
            if (nonNull(queryDefinitions)) {
                String dbTableLocationQuery = queryDefinitions.getQueryDefinition("database_table_locations").getStatement();
                pstmt = conn.prepareStatement(dbTableLocationQuery);
                pstmt.setString(1, database);
                resultSet = pstmt.executeQuery();
                while (resultSet.next()) {
                    String tableName = resultSet.getString(1);
                    String tableType = resultSet.getString(2);
                    String location = resultSet.getString(3);
                    // Filter out some table types. Don't transfer previously moved tables or
                    // interim tables created by hms-mirror.
                    if (tableName.startsWith(config.getTransfer().getTransferPrefix())
                            || tableName.endsWith("storage_migration")) {
                        log.warn("Database: {} Table: {} was NOT added to list.  The name matches the transfer prefix " +
                                "and is most likely a remnant of a previous event. If this is a mistake, change the " +
                                "'transferPrefix' to something more unique.", database, tableName);
                    } else {
                        if (isBlank(config.getFilter().getTblRegEx()) && isBlank(config.getFilter().getTblExcludeRegEx())) {
                            config.getTranslator().addTableSource(database, tableName, tableType, location, consolidationLevelBase,
                                    partitionLevelMismatch);
                        } else if (!isBlank(config.getFilter().getTblRegEx())) {
                            // Filter Tables
                            assert (config.getFilter().getTblFilterPattern() != null);
                            Matcher matcher = config.getFilter().getTblFilterPattern().matcher(tableName);
                            if (matcher.matches()) {
                                config.getTranslator().addTableSource(database, tableName, tableType, location, consolidationLevelBase,
                                        partitionLevelMismatch);
                            }
                        } else if (config.getFilter().getTblExcludeRegEx() != null) {
                            assert (config.getFilter().getTblExcludeFilterPattern() != null);
                            Matcher matcher = config.getFilter().getTblExcludeFilterPattern().matcher(tableName);
                            if (!matcher.matches()) { // ANTI-MATCH
                                config.getTranslator().addTableSource(database, tableName, tableType, location, consolidationLevelBase,
                                        partitionLevelMismatch);
                            }
                        }
                    }
                }
                resultSet.close();
                pstmt.close();
                // Get the Partition Locations
                String dbPartitionLocationQuery = queryDefinitions.getQueryDefinition("database_partition_locations").getStatement();
                pstmt = conn.prepareStatement(dbPartitionLocationQuery);
                pstmt.setString(1, database);
                resultSet = pstmt.executeQuery();
                while (resultSet.next()) {
                    String tableName = resultSet.getString(1);
                    String tableType = resultSet.getString(2);
                    String partitionSpec = resultSet.getString(3);
                    String tableLocation = resultSet.getString(4);
                    String partitionLocation = resultSet.getString(5);

                    if (isBlank(config.getFilter().getTblRegEx()) && isBlank(config.getFilter().getTblExcludeRegEx())) {
                        config.getTranslator().addPartitionSource(database, tableName, tableType, partitionSpec,
                                tableLocation, partitionLocation, consolidationLevelBase, partitionLevelMismatch);
                    } else if (!isBlank(config.getFilter().getTblRegEx())) {
                        // Filter Tables
                        assert (config.getFilter().getTblFilterPattern() != null);
                        Matcher matcher = config.getFilter().getTblFilterPattern().matcher(tableName);
                        if (matcher.matches()) {
                            config.getTranslator().addPartitionSource(database, tableName, tableType, partitionSpec,
                                    tableLocation, partitionLocation, consolidationLevelBase, partitionLevelMismatch);
                        }
                    } else if (config.getFilter().getTblExcludeRegEx() != null) {
                        assert (config.getFilter().getTblExcludeFilterPattern() != null);
                        Matcher matcher = config.getFilter().getTblExcludeFilterPattern().matcher(tableName);
                        if (!matcher.matches()) { // ANTI-MATCH
                            config.getTranslator().addPartitionSource(database, tableName, tableType, partitionSpec,
                                    tableLocation, partitionLocation, consolidationLevelBase, partitionLevelMismatch);
                        }
                    }
                }
            }


            log.info("Loaded Database Table/Partition Locations from Metastore Direct Connection {}:{}", environment, database);
        } catch (SQLException throwables) {
            log.error("Issue loading Table/Partition Locations from Metastore Direct Connection. {}:{}", environment, database);
            log.error(throwables.getMessage(), throwables);
//            throw new RuntimeException(throwables);
        } finally {
            try {
                if (nonNull(conn))
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    public boolean loadEnvironmentVars() {
        boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        List<Environment> environments = Arrays.asList(Environment.LEFT, Environment.RIGHT);
        for (Environment environment : environments) {
            if (hmsMirrorConfig.getCluster(environment) != null) {
                Connection conn = null;
                Statement stmt = null;
                // Clear current variables.
                hmsMirrorConfig.getCluster(environment).getEnvVars().clear();
                log.info("Loading {} Environment Variables", environment);
                try {
                    conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
                    //getConfig().getCluster(Environment.LEFT).getConnection();
                    if (conn != null) {
                        log.info("Retrieving {} Cluster Connection", environment);
                        stmt = conn.createStatement();
                        // Load Session Environment Variables.
                        ResultSet rs = stmt.executeQuery(MirrorConf.GET_ENV_VARS);
                        while (rs.next()) {
                            String envVarSet = rs.getString(1);
                            hmsMirrorConfig.getCluster(environment).addEnvVar(envVarSet);
                        }
                    }
                } catch (SQLException se) {
                    // Issue
                    rtn = Boolean.FALSE;
                    log.error("Issue getting database connections", se);
                    executeSessionService.getSession().addError(MISC_ERROR, environment + ":Issue getting database connections");
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            log.error("Issue closing LEFT database connections", e);
                            executeSessionService.getSession().addError(MISC_ERROR, environment + ":Issue closing database connections");
                        }
                    }
                }
            }
        }
        return rtn;
    }

    public List<String> listAvailableDatabases(Environment environment) {
        List<String> dbs = new ArrayList<>();
        Connection conn = null;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        try {
            conn = connectionPoolService.getHS2EnvironmentConnection(environment);
            if (conn != null) {
                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    resultSet = stmt.executeQuery(SHOW_DATABASES);
                    while (resultSet.next()) {
                        dbs.add(resultSet.getString(1));
                    }
                } catch (SQLException sql) {
                    log.error("Issue getting database list", sql);
//                    throw new RuntimeException(sql);
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
            log.error("Issue getting database connections", se);
//            throw new RuntimeException(se);
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
        return dbs;
    }

    /*
    Build DB Statements for the LEFT (DUMP) and RIGHT (OTHER MIGRATIONS) databases.

    Conditions to set the RIGHT DB Location(s):
    - If they are set on the LEFT, set them on the RIGHT.
        - Unless the translated location matches the ENV Warehouse Locations.
    - If there is a Warehouse Plan for the Database, set the locations to the Warehouse Plan locations.
    - Capture the COMMENTS and translate them to the RIGHT.
    - Capture all DB Properties and translate them to the RIGHT.


     */
    public boolean buildDBStatements(DBMirror dbMirror) {
//        Config config = Context.getInstance().getConfig();
        log.info("Building DB Statements for {}", dbMirror.getName());
        boolean rtn = Boolean.TRUE; // assume all good till we find otherwise.
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();

        boolean buildLeft = Boolean.FALSE;
        boolean buildRight = Boolean.TRUE;
        boolean createRight = Boolean.FALSE;
        boolean altLeftDB = Boolean.FALSE;
        boolean altRightDB = Boolean.FALSE;
        boolean skipLocation = Boolean.FALSE;
        boolean skipManagedLocation = Boolean.FALSE;
        boolean forceLocations = Boolean.FALSE;

        Map<String, String> dbDefLeft = dbMirror.getProperty(Environment.LEFT);
        Map<String, String> dbDefRight = dbMirror.getProperty(Environment.RIGHT);

        switch (config.getDataStrategy()) {
            case DUMP:
                altLeftDB = Boolean.FALSE;
                buildLeft = Boolean.TRUE;
                buildRight = Boolean.FALSE;
                break;
            case STORAGE_MIGRATION:
                altLeftDB = Boolean.TRUE;
                buildLeft = Boolean.FALSE;
                buildRight = Boolean.TRUE;
                forceLocations = Boolean.TRUE;
                // Clone Left to Right as a Holding Location for work with STORAGE_MIGRATION.
                if (isNull(config.getCluster(Environment.RIGHT))) {
                    Cluster cluster = config.getCluster(Environment.LEFT).clone();
                    config.getClusters().put(Environment.RIGHT, cluster);
                }
                // Build the Right Def as a Clone of the Left to Seed it.
                if (isNull(dbDefRight)) {
                    dbDefRight = new TreeMap<String, String>();
                    dbMirror.setProperty(Environment.RIGHT, dbDefRight);
                }

                break;
            case SQL:
                altLeftDB = Boolean.FALSE;
                buildLeft = Boolean.FALSE;
                // This is a single cluster operation.
                if (config.getMigrateACID().isDowngradeInPlace()) {
                    buildRight = Boolean.FALSE;
                }
                // Build the Right Def as a Clone of the Left to Seed it.
                if (isNull(dbDefRight)) {
                    // No Right DB Definition.  So we're going to create it.
                    createRight = Boolean.TRUE;
                    dbDefRight = new TreeMap<String, String>();
                    dbMirror.setProperty(Environment.RIGHT, dbDefRight);
                }
                // Force Locations to ensure DB and new tables are created in the right locations.
                forceLocations = Boolean.TRUE;
                break;
            case SCHEMA_ONLY:
            case HYBRID:
            case EXPORT_IMPORT:
            case COMMON:
            case INTERMEDIATE:
            case LINKED:
                altLeftDB = Boolean.FALSE;
                buildLeft = Boolean.FALSE;
                buildRight = Boolean.TRUE;
                // Build the Right Def as a Clone of the Left to Seed it.
                if (isNull(dbDefRight)) {
                    // No Right DB Definition.  So we're going to create it.
                    createRight = Boolean.TRUE;
                    dbDefRight = new TreeMap<String, String>(dbDefLeft);
                    dbMirror.setProperty(Environment.RIGHT, dbDefRight);
                }
                // Force Locations to ensure DB and new tables are created in the right locations.
                if (config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.ALIGNED) {
                    forceLocations = Boolean.TRUE;
                }
                break;
            case CONVERT_LINKED:
                altLeftDB = Boolean.TRUE;
                buildLeft = Boolean.FALSE;
                buildRight = Boolean.FALSE;
                break;
            default:
                break;
        }


        try {


            // Start with the LEFT definition.
//            Map<String, String> dbDefLeft = dbMirror.getDBDefinition(Environment.LEFT);
//            Map<String, String> dbDefRight = dbMirror.getDBDefinition(Environment.RIGHT);
            String originalDatabase = dbMirror.getName();
            String targetDatabase = HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config);
            log.info("Original Database: {} Target Database: {}", originalDatabase, targetDatabase);

            String originalLocation = null;
            String originalManagedLocation = null;

            // The LOCATION element in HDP3 (Early Hive 3) is actually the MANAGEDLOCATION.
            //   all other versions, LOCATION is for EXTERNAL tables and Legacy Managed Tables (Hive 1/2)
            if (config.getCluster(Environment.LEFT).isHdpHive3()) {
                log.info("HDP Hive 3 Detected.  Adjusting LOCATION to MANAGEDLOCATION.");
                originalManagedLocation = dbMirror.getProperty(Environment.LEFT, DB_LOCATION);
            } else {
                originalLocation = dbMirror.getProperty(Environment.LEFT, DB_LOCATION);
                originalManagedLocation = dbMirror.getProperty(Environment.LEFT, DB_MANAGED_LOCATION);
            }

            String targetNamespace = null;
            try {
                targetNamespace = config.getTargetNamespace();
            } catch (RequiredConfigurationException rte) {
                // TODO: We need to rework this to handle multiple namespaces.
                if (config.getDataStrategy() == DataStrategyEnum.DUMP) {
                    targetNamespace = config.getCluster(Environment.LEFT).getHcfsNamespace();
                } else {
                    throw rte;
                }
            }
            // One of three type of warehouses: Plan, Global, or Environment.
            Warehouse warehouse = warehouseService.getWarehousePlan(dbMirror.getName());
            log.debug("Warehouse Plan for {}: {}", dbMirror.getName(), warehouse);

            Warehouse envWarehouse = null;
            if (config.getDataStrategy() != DataStrategyEnum.DUMP) {
                envWarehouse = config.getCluster(Environment.RIGHT).getEnvironmentWarehouse();
            } else {
                envWarehouse = config.getCluster(Environment.LEFT).getEnvironmentWarehouse();
            }
            log.debug("Environment Warehouse for {}: {}", dbMirror.getName(), envWarehouse);

            String targetLocation = null;
            String targetManagedLocation = null;

            if (buildRight) {
                log.debug("Building RIGHT DB Definition for {}", dbMirror.getName());
                // Configure the DB 'LOCATION'
                if (!isBlank(originalLocation) || forceLocations) {
                    log.debug("Original Location: {}", originalLocation);
                    // Get the base location without the original namespace.
                    // TODO: Need to address NULL here!!!!
                    targetLocation = NamespaceUtils.stripNamespace(originalLocation);
                    log.debug("Target Location from Original Location: {}", targetLocation);
                    // Only set to warehouse location if the translation type is 'ALIGNED',
                    //   otherwise we want to keep the same relative location.
                    if (nonNull(warehouse) && config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.ALIGNED) {
                        log.debug("Aligned Translation Type.  Adjusting Target Location to include Warehouse Location.");
                        String dbDirectory = NamespaceUtils.getLastDirectory(targetLocation);
                        switch (warehouse.getSource()) {
                            case PLAN:
                            case GLOBAL:
//                                targetLocation = warehouse.getExternalDirectory() + "/" + targetDatabase + ".db";
                                targetLocation = warehouse.getExternalDirectory() + "/" + dbDirectory;
                                break;
                            default:
                                // Didn't find an explicit location. So we're going to leave it as 'relative'.
                                // This handles any DB rename process.
                                targetLocation = NamespaceUtils.getParentDirectory(targetLocation);
//                                targetLocation = targetLocation + "/" + targetDatabase + ".db";
                                targetLocation = targetLocation + "/" + dbDirectory;
                                break;
                        }
                        log.debug("Target Location after Warehouse Adjustment: {}", targetLocation);

                        // Check the location against the ENV warehouse location.  If they match, don't set the location.
                        if (nonNull(envWarehouse)) {
                            String reducedTargetLocation = NamespaceUtils.getParentDirectory(targetLocation);
                            if (reducedTargetLocation.equals(envWarehouse.getExternalDirectory())) {
                                log.debug("The new target location: {} is the same as the ENV warehouse location.  " +
                                        "So we're not going to set it and let it default to the ENV warehouse location: {}.", targetLocation, envWarehouse.getExternalDirectory());
                                // The new target location is the same as the ENV warehouse location.  So we're not
                                // going to set it and let it default to the ENV warehouse location.
                                dbDefRight.put(DB_LOCATION, targetNamespace + targetLocation);
                                dbMirror.addIssue(Environment.RIGHT, "The database location is the same as the ENV warehouse location.  The database location will NOT be set and will depend on the ENV warehouse location.");
                                targetLocation = null;
                            }
                        }
                    }
                    if (nonNull(targetLocation)) {
                        // Add the Namespace.
                        // Tweak the db location incase the database name is different. But only if the original followed
                        //  some standard naming convention in hive.
                        if (!targetLocation.contains(targetDatabase)) {
                            log.debug("Target Location doesn't contain the target database name.  Adjusting.");
                            targetLocation = targetLocation.replace(originalDatabase, targetDatabase);
                        }
                        targetLocation = targetNamespace + targetLocation;
                    }
                }
                // Configure the DB 'MANAGEDLOCATION'
                if (!isBlank(originalManagedLocation) || forceLocations) {
                    // Get the base location without the original namespace.
                    log.debug("Original Managed Location: {}", originalManagedLocation);
                    targetManagedLocation = NamespaceUtils.stripNamespace(originalManagedLocation);
                    log.debug("Target Managed Location from Original Managed Location: {}", targetManagedLocation);
                    String dbDirectory = nonNull(targetManagedLocation)?NamespaceUtils.getLastDirectory(targetManagedLocation):
                            nonNull(dbMirror.getLocationDirectory())?dbMirror.getLocationDirectory():targetDatabase + ".db";

                    // Only set to warehouse location if the translation type is 'ALIGNED',
                    //   otherwise we want to keep the same relative location.
                    if (nonNull(warehouse) && config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.ALIGNED) {
                        log.debug("Aligned Translation Type.  Adjusting Target Managed Location to include Warehouse Location.");
                        switch (warehouse.getSource()) {
                            case PLAN:
                            case GLOBAL:
//                                targetManagedLocation = warehouse.getManagedDirectory() + "/" + targetDatabase + ".db";
                                targetManagedLocation = warehouse.getManagedDirectory() + "/" + dbDirectory;
                                break;
                            default:
                                // Didn't find an explicit location. So we're going to leave it as 'relative'.
                                // This handles any DB rename process.
                                targetManagedLocation = NamespaceUtils.getParentDirectory(targetManagedLocation);
//                                targetManagedLocation = targetManagedLocation + "/" + targetDatabase + ".db";
                                targetManagedLocation = targetManagedLocation + "/" + dbDirectory;
                                break;
                        }
                        log.debug("Target Managed Location after Warehouse Adjustment: {}", targetManagedLocation);
                        // Check the location against the ENV warehouse location.  If they match, don't set the location.
                        if (nonNull(targetManagedLocation) && nonNull(envWarehouse)) {
                            String reducedTargetLocation = NamespaceUtils.getParentDirectory(targetManagedLocation);
                            if (reducedTargetLocation.equals(envWarehouse.getManagedDirectory())) {
                                // The new target location is the same as the ENV warehouse location.  So we're not
                                // going to set it and let it default to the ENV warehouse location.
                                log.debug("The new target managed location: {} is the same as the ENV warehouse managed location.  " +
                                        "So we're not going to set it and let it default to the ENV warehouse managed location: {}.",
                                        targetManagedLocation, envWarehouse.getManagedDirectory());
                                dbDefRight.put(DB_MANAGED_LOCATION, targetNamespace + targetManagedLocation);
                                dbMirror.addIssue(Environment.RIGHT, "The database 'Managed' location is the same as the ENV warehouse Managed location.  The database location will NOT be set and will depend on the ENV warehouse location.");
                                targetManagedLocation = null;
                            }
                        }
                    }
                    if (nonNull(targetManagedLocation)) {
                        // Add the Namespace.
                        if (!targetManagedLocation.contains(targetDatabase)) {
                            log.debug("Target Managed Location: {} doesn't contain the target database name: {}.  Adjusting.",
                                    targetManagedLocation, targetDatabase);
                            targetManagedLocation = targetManagedLocation.replace(originalDatabase, targetDatabase);
                        }
                        targetManagedLocation = targetNamespace + targetManagedLocation;
                        log.debug("Target Managed Location after Namespace Adjustment: {}", targetManagedLocation);
                    }
                }

                // Upsert Source DB Parameters to Target DB Parameters.
                DatabaseUtils.upsertParameters(dbDefLeft, dbDefRight, skipList);

                // Deal with ReadOnly.
                if (config.isReadOnly()) {
                    if (nonNull(targetLocation)) {
                        log.debug("Checking if the target location: {} exists on the RIGHT cluster.", targetLocation);
                        // TODO: This check doesn't happen if targetLocation is null, which means it's the default location.
                        //       Still need to check that.
                        try {
                            CliEnvironment cli = executeSessionService.getCliEnvironment();

                            CommandReturn testCr = cli.processInput("test -d " + targetLocation);
                            if (testCr.isError()) {
                                // Doesn't exist.  So we can't create the DB in a "read-only" mode.
                                runStatus.addError(RO_DB_DOESNT_EXIST, targetLocation,
                                        testCr.getError(), testCr.getCommand(), dbMirror.getName());
                                dbMirror.addIssue(Environment.RIGHT, runStatus.getErrorMessage(RO_DB_DOESNT_EXIST));
                                rtn = Boolean.FALSE;
                            }
                        } catch (DisabledException e) {
                            log.warn("Unable to test location {} because the CLI Interface is disabled.", targetLocation);
                            dbMirror.addIssue(Environment.RIGHT, "Unable to test location " + targetLocation + " because the CLI Interface is disabled. " +
                                    "This may lead to issues when creating the database in 'read-only' mode.  ");
                        }
                    } else {
                        // Can't determine location.
                        // TODO: What to do here.
                        log.error("{}: Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in 'read-only' mode without knowing where it should go and validating existance.", targetDatabase);
//                        throw new RuntimeException(targetDatabase + ": Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in " +
//                                "'read-only' mode without knowing where it should go and validating existence.");
                    }

                }
            }

            if (config.isResetRight() && buildRight) {
                // Add DROP db to the RIGHT sql.

            }

            switch (config.getDataStrategy()) {
                case CONVERT_LINKED:
                    // ALTER the 'existing' database to ensure locations are set to the RIGHT hcfsNamespace.
                    /*
                    database = HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config);
                    originalLocation = dbDefRight.get(DB_LOCATION);
                    originalManagedLocation = dbDefRight.get(DB_MANAGED_LOCATION);

                    if (originalLocation != null && !config.getCluster(Environment.RIGHT).isHdpHive3()) {
                        originalLocation = NamespaceUtils.replaceNamespace(originalLocation, targetNamespace);
                        String alterDB_location = MessageFormat.format(ALTER_DB_LOCATION, database, originalLocation);
                        dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDB_location));
                        dbDefRight.put(DB_LOCATION, originalLocation);
                    }
                    if (originalManagedLocation != null) {
                        originalManagedLocation = NamespaceUtils.replaceNamespace(originalManagedLocation, targetNamespace);
                        if (!config.getCluster(Environment.RIGHT).isHdpHive3()) {
                            String alterDBMngdLocationSql = MessageFormat.format(ALTER_DB_MNGD_LOCATION, database, originalManagedLocation);
                            dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_MNGD_LOCATION_DESC, alterDBMngdLocationSql));
                        } else {
                            String alterDBMngdLocationSql = MessageFormat.format(ALTER_DB_LOCATION, database, originalManagedLocation);
                            dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDBMngdLocationSql));
                            dbMirror.addIssue(Environment.RIGHT, HDPHIVE3_DB_LOCATION.getDesc());
                        }
                        dbDefRight.put(DB_MANAGED_LOCATION, originalManagedLocation);
                    }
                    */
                    break;
                case DUMP:
                    // Build LEFT DB SQL.
                    log.debug("Building LEFT DB SQL for {}", dbMirror.getName());
                    String createDb = MessageFormat.format(CREATE_DB, targetDatabase);
                    StringBuilder sb = new StringBuilder();
                    sb.append(createDb).append("\n");
                    if (dbDefLeft.get(COMMENT) != null && !dbDefLeft.get(COMMENT).trim().isEmpty()) {
                        sb.append(COMMENT).append(" \"").append(dbDefLeft.get(COMMENT)).append("\"\n");
                    }
                    if (nonNull(originalLocation)) {
                        sb.append(DB_LOCATION).append(" \"").append(originalLocation).append("\"\n");
                        dbDefLeft.put(DB_LOCATION, originalLocation);
                    }
                    if (nonNull(originalManagedLocation)) {
                        sb.append(DB_MANAGED_LOCATION).append(" \"").append(originalManagedLocation).append("\"\n");
                        dbDefLeft.put(DB_MANAGED_LOCATION, originalManagedLocation);
                    }
                    dbMirror.getSql(Environment.LEFT).add(new Pair(CREATE_DB_DESC, sb.toString()));


                    // TODO: DB Properties.

                    break;
                case STORAGE_MIGRATION:
                    // Handle a situation where the database name is/is not the same as the original.
                    log.debug("Building RIGHT DB SQL for {} -> STORAGE_MIGRATION", dbMirror.getName());
                    if (targetDatabase.equals(originalDatabase)) {
                        dbMirror.addIssue(Environment.LEFT, "Database Name is the same as the original.  No changes will be made to the database name.");
                    } else {
                        String createDb1 = MessageFormat.format(CREATE_DB, targetDatabase);
                        dbMirror.getSql(Environment.LEFT).add(new Pair(CREATE_DB_DESC, createDb1));
                    }
                    if (config.getTransfer().getStorageMigration().isSkipDatabaseLocationAdjustments()) {
                        dbMirror.addIssue(Environment.LEFT, "Database Location Adjustments are being skipped. " +
                                "Only tables locations will be adjusted.  New tables will continue to goto the original " +
                                "database locations.");
                    } else {
                        if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
                            if (!isBlank(targetLocation)) {
                                String alterDbLoc = MessageFormat.format(ALTER_DB_LOCATION, targetDatabase, targetLocation);
                                dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbLoc));
                                dbDefRight.put(DB_LOCATION, targetLocation);
                            }
                        }
                        if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
                            if (!isBlank(targetManagedLocation)) {
                                String alterDbMngdLoc = MessageFormat.format(ALTER_DB_MNGD_LOCATION, targetDatabase, targetManagedLocation);
                                dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
                                dbDefRight.put(DB_MANAGED_LOCATION, targetManagedLocation);
                            }
                        } else {
                            if (!isBlank(targetManagedLocation)) {
                                String alterDbMngdLoc = MessageFormat.format(ALTER_DB_LOCATION, targetDatabase, targetManagedLocation);
                                dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
                                dbMirror.addIssue(Environment.LEFT, HDPHIVE3_DB_LOCATION.getDesc());
                                dbDefRight.put(DB_LOCATION, targetManagedLocation);
                            }
                        }

                        dbMirror.addIssue(Environment.LEFT, "This process, when 'executed' will leave the original tables intact in their renamed " +
                                "version.  They are NOT automatically cleaned up.  Run the produced '" +
                                dbMirror.getName() + "_LEFT_CleanUp_execute.sql' " +
                                "file to permanently remove them.  Managed and External/Purge table data will be " +
                                "removed when dropping these tables.  External non-purge table data will remain in storage.");
                    }
                    break;
                default:
                    /*
                    Check to see if the database already exists on the RIGHT.  If it does, we need to check the [MANAGED]LOCATION values
                    to see if they match the calculated values.  If they do, we don't need to do anything.  If they don't, we need to
                    CREATE and ALTER the Database to set them correctly.

                    https://github.com/cloudera-labs/hms-mirror/issues/146

                     */
                    if (dbMirror.getProperty(Environment.RIGHT) != null) {
                        String rightLocation = dbMirror.getProperty(Environment.RIGHT, DB_LOCATION);
                        String rightManagedLocation = dbMirror.getProperty(Environment.RIGHT, DB_MANAGED_LOCATION);
                        if (nonNull(rightLocation) && !rightLocation.equals(targetLocation)) {
                            altRightDB = Boolean.TRUE;
                        }
                        if (nonNull(rightManagedLocation) && !rightManagedLocation.equals(targetManagedLocation)) {
                            altRightDB = Boolean.TRUE;
                        }
                    }

                    // Create the DB on the RIGHT is it doesn't exist.
                    if (createRight) {
                        log.debug("Building RIGHT DB SQL for {} -> CREATE", dbMirror.getName());
                        String createDbL = MessageFormat.format(CREATE_DB, targetDatabase);
                        StringBuilder sbL = new StringBuilder();
                        sbL.append(createDbL).append("\n");
                        if (dbDefLeft.get(COMMENT) != null && !dbDefLeft.get(COMMENT).trim().isEmpty()) {
                            sbL.append(COMMENT).append(" \"").append(dbDefLeft.get(COMMENT)).append("\"\n");
                        }
                        dbMirror.getSql(Environment.RIGHT).add(new Pair(CREATE_DB_DESC, sbL.toString()));
                        log.trace("RIGHT DB Create SQL: {}", sbL.toString());
                    }

                    if (nonNull(targetLocation) && !config.getCluster(Environment.RIGHT).isHdpHive3()) {
                        String origRightLocation = dbDefRight.get(DB_LOCATION);
                        // If the original location is null or doesn't equal the target location, set it.
                        if (isNull(origRightLocation) || !origRightLocation.equals(targetLocation)) {
                            String alterDbLoc = MessageFormat.format(ALTER_DB_LOCATION, targetDatabase, targetLocation);
                            dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbLoc));
                            dbDefRight.put(DB_LOCATION, targetLocation);
                            log.trace("RIGHT DB Location SQL: {}", alterDbLoc);
                        }
                    }
                    if (nonNull(targetManagedLocation)) {
                        String origRightManagedLocation = config.getCluster(Environment.RIGHT).isHdpHive3() ? dbDefRight.get(DB_LOCATION) : dbDefRight.get(DB_MANAGED_LOCATION);
                        if (isNull(origRightManagedLocation) || !origRightManagedLocation.equals(targetManagedLocation)) {
                            if (!config.getCluster(Environment.RIGHT).isHdpHive3()) {
                                String alterDbMngdLoc = MessageFormat.format(ALTER_DB_MNGD_LOCATION, targetDatabase, targetManagedLocation);
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
                                dbDefRight.put(DB_MANAGED_LOCATION, targetManagedLocation);
                                log.trace("RIGHT DB Managed Location SQL: {}", alterDbMngdLoc);
                            } else {
                                String alterDbMngdLoc = MessageFormat.format(ALTER_DB_LOCATION, targetDatabase, targetManagedLocation);
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
                                dbMirror.addIssue(Environment.RIGHT, HDPHIVE3_DB_LOCATION.getDesc());
                                dbDefRight.put(DB_LOCATION, targetManagedLocation);
                                log.trace("RIGHT DB Managed Location SQL: {}", alterDbMngdLoc);
                            }
                        }
                    }

                    // Build the DBPROPERITES
                    Map<String, String> dbProperties = DatabaseUtils.getParameters(dbDefRight, skipList);
                    if (!dbProperties.isEmpty()) {
                        for (Map.Entry<String, String> entry : dbProperties.entrySet()) {
                            String alterDbProps = MessageFormat.format(ALTER_DB_PROPERTIES, targetDatabase, entry.getKey(), entry.getValue());
                            dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_PROPERTIES_DESC, alterDbProps));
                            log.trace("RIGHT DB Properties SQL: {}", alterDbProps);
                        }
                    }

                    if (config.getOwnershipTransfer().isDatabase()) {
                        String ownerFromLeft = dbDefLeft.get(OWNER_NAME);
                        String ownerTypeFromLeft = dbDefLeft.get(OWNER_TYPE);
                        if (nonNull(ownerFromLeft) && nonNull(ownerTypeFromLeft)) {
                            log.info("Setting Owner: {} of type: {} on Database: {}", ownerFromLeft, ownerTypeFromLeft, targetDatabase);
                            if (ownerTypeFromLeft.equals("USER")) {
                                String alterOwner = null;
                                // Figure out which DDL syntax to use.
                                if (config.getCluster(Environment.RIGHT).getPlatformType().isDbOwnerType()) {
                                    alterOwner = MessageFormat.format(SET_DB_OWNER_W_USER_TYPE, targetDatabase, ownerFromLeft);
                                } else {
                                    alterOwner = MessageFormat.format(SET_DB_OWNER, targetDatabase, ownerFromLeft);
                                }
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(SET_DB_OWNER_DESC, alterOwner));
                                log.trace("RIGHT DB Owner SQL: {}", alterOwner);
                            }
                        }
                    }

                    break;

            }
        } catch (MissingDataPointException e) {
            ExecuteSession session = executeSessionService.getSession();
            rtn = Boolean.FALSE;
            session.getRunStatus().addError(MessageCode.ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS);
            log.error(MessageCode.ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS.getDesc(), e);
            // TODO: Do we need to use the LEFT here when it's STORAGE_MIGRATION?
            dbMirror.addIssue(Environment.RIGHT, MessageCode.ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS.getDesc());
        } catch (RequiredConfigurationException e) {
            rtn = Boolean.FALSE;
            log.error("Required Configuration", e);
            // TODO: Do we need to use the LEFT here when it's STORAGE_MIGRATION?
            dbMirror.addIssue(Environment.RIGHT, "Required Configuration: " + e.getMessage());
        }

        return rtn;
    }

    public boolean createDatabases() {
        boolean rtn = true;
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        log.info("Creating Databases");
        if (config.getMigrateACID().isDowngradeInPlace() && config.getDataStrategy() == DataStrategyEnum.SQL) {
            log.info("Downgrade in place.  Skipping database creation.");
            return true;
        }

        Conversion conversion = session.getConversion();
        RunStatus runStatus = session.getRunStatus();
        OperationStatistics stats = runStatus.getOperationStatistics();
        for (String database : config.getDatabases()) {
            log.info("Creating Database: {}", database);
            DBMirror dbMirror = conversion.getDatabase(database);
            try {
                rtn = buildDBStatements(dbMirror);
            } catch (RuntimeException rte) {
                log.error("Issue building DB Statements for {}", database, rte);
                runStatus.addError(MISC_ERROR, database + ":Issue building DB Statements");
                rtn = false;
            }

            if (rtn) {
                if (!runDatabaseSql(dbMirror, Environment.LEFT)) {
                    rtn = false;
//                    stats.getFailures().incrementDatabases();
                } else {
//                    stats.getSuccesses().incrementDatabases();
                }
                if (!runDatabaseSql(dbMirror, Environment.RIGHT)) {
                    rtn = false;
//                    stats.getFailures().incrementDatabases();
                } else {
                    // TODO: Will this double up the success counts?  I think it will..  Will Observed..
//                    stats.getSuccesses().incrementDatabases();
                }
            } else {
//                stats.getFailures().incrementDatabases();
            }
        }
        return rtn;
    }

    public Boolean getDatabase(DBMirror dbMirror, Environment environment) throws SQLException {
        Boolean rtn = Boolean.FALSE;
        Connection conn = null;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        try {
            conn = connectionPoolService.getHS2EnvironmentConnection(environment);//getConnection();
            if (conn != null) {

                String database = (environment == Environment.LEFT ? dbMirror.getName() : HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config));

                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    log.info("{}:{}: Loading Database Definition", environment, database);
                    resultSet = stmt.executeQuery(MessageFormat.format(DESCRIBE_DB, database));
                    //Retrieving the ResultSetMetaData object
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    //getting the column type
                    int column_count = rsmd.getColumnCount();
                    Map<String, String> dbProps = new TreeMap<>();
                    while (resultSet.next()) {
                        for (int i = 0; i < column_count; i++) {
                            String cName = rsmd.getColumnName(i + 1).toUpperCase(Locale.ROOT);
                            String cValue = resultSet.getString(i + 1);
                            // Don't add element if its empty.
                            if (cValue != null && !cValue.trim().isEmpty()) {
                                if (cName.equalsIgnoreCase("parameters")) {
                                    // Split the parameters into key/value pairs.
                                    Map<String, String> dbPropsMap = DatabaseUtils.parametersToMap(cValue);
                                    for (Map.Entry<String, String> entry : dbPropsMap.entrySet()) {
                                        dbProps.put(entry.getKey(), entry.getValue());
                                    }
                                } else {
                                    dbProps.put(cName, cValue);
                                }
                            }
                        }
                    }
                    dbMirror.setProperty(environment, dbProps);

                    /* With the extend describe above, we get the 'parameters' column with all the DBPROPERTIES.
                    // Run the SHOW CREATE DATABASE command to pull DBPROPERTIES.
                    resultSet = stmt.executeQuery(MessageFormat.format(SHOW_CREATE_DATABASE, database));

                    // SHOW CREATE DATABASE is NOT support in Legacy Hive.
                    if (!config.getCluster(environment).isLegacyHive()) {
                        List<String> dbDef = dbMirror.getDefinition();
                        ResultSetMetaData meta = resultSet.getMetaData();
                        if (meta.getColumnCount() >= 0) {
                            while (resultSet.next()) {
                                try {
                                    dbDef.add(resultSet.getString(1).trim());
                                } catch (NullPointerException npe) {
                                    // catch and continue.
                                    log.error("Loading Database Definition.  Issue with SHOW CREATE DATABASE resultset. " +
                                                    "ResultSet record(line) is null. Skipping. {}:{}",
                                            environment, database);
                                }
                            }
                        } else {
                            log.error("Loading Database Definition.  Issue with SHOW CREATE DATABASE resultset. No Metadata. {}:{}"
                                    , environment, database);
                        }
                    }
                    */

                    rtn = Boolean.TRUE;
                } catch (SQLException sql) {
                    // DB Doesn't Exists.
                    log.error("{}:{}: Failed to loading Database Definition", environment, database);
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
        return rtn;
    }

    public Boolean runDatabaseSql(DBMirror dbMirror, Environment environment) {
        List<Pair> dbPairs = dbMirror.getSql(environment);
        Boolean rtn = Boolean.TRUE;
//        for (Pair pair : dbPairs) {
//            String action = pair.getAction();
        if (!runDatabaseSql(dbMirror, dbPairs, environment)) {
            rtn = Boolean.FALSE;
            // don't continue
//                break;
        }
//        }
        return rtn;
    }

    public Boolean runDatabaseSql(DBMirror dbMirror, List<Pair> pairs, Environment environment) {
        // Open the connections and ensure we are running this on the "RIGHT" cluster.
        Connection conn = null;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        Boolean rtn = Boolean.TRUE;
        // Skip when running test data.
        if (!config.isLoadingTestData()) {
            try {
                conn = connectionPoolService.getHS2EnvironmentConnection(environment);

                if (isNull(conn) && config.isExecute()
                        && !config.getCluster(environment).getHiveServer2().isDisconnected()) {
                    // this is a problem.
                    rtn = Boolean.FALSE;
                    dbMirror.addIssue(environment, "Connection missing. This is a bug.");
                }

                if (isNull(conn) && config.getCluster(environment).getHiveServer2().isDisconnected()) {
                    dbMirror.addIssue(environment, "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                            "The scripts will need to be run 'manually'.");
                }

                if (!isNull(conn) && config.isExecute()) {
                    Statement stmt = null;
                    try {
                        try {
                            stmt = conn.createStatement();
                        } catch (SQLException throwables) {
                            log.error("Issue building statement", throwables);
                            rtn = Boolean.FALSE;
                        }

                        for (Pair dbSqlPair : pairs) {
                            try {
                                String action = dbSqlPair.getAction();
                                if (action.trim().isEmpty() || action.trim().startsWith("--")) {
                                    continue;
                                } else {
                                    log.info("{}:{}:{}", environment, dbSqlPair.getDescription(), dbSqlPair.getAction());
                                    stmt.execute(dbSqlPair.getAction());
                                }
                            } catch (SQLException throwables) {
                                log.error("{}:{}:", environment, dbSqlPair.getDescription(), throwables);
                                dbMirror.addIssue(environment, throwables.getMessage() + " " + dbSqlPair.getDescription() +
                                        " " + dbSqlPair.getAction());
                                rtn = Boolean.FALSE;
                            }
                        }

                    } finally {
                        if (stmt != null) {
                            try {
                                stmt.close();
                            } catch (SQLException sqlException) {
                                // ignore
                            }
                        }
                    }
                } else {
                    log.info("DRY-RUN: {} - {}", environment, dbMirror.getName());
                    for (Pair dbSqlPair : pairs) {
                        log.info("{}:{}:{}", environment, dbSqlPair.getDescription(), dbSqlPair.getAction());
                    }
                }
            } catch (SQLException throwables) {
                log.error(environment.toString(), throwables);
//                throw new RuntimeException(throwables);
            } finally {
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
            }
        } else {
            log.info("TEST DATA RUN: {} - {}", environment, dbMirror.getName());
            for (Pair dbSqlPair : pairs) {
                log.info("{}:{}:{}", environment, dbSqlPair.getDescription(), dbSqlPair.getAction());
            }
        }
        return rtn;
    }

    public Map<String, Number> getEnvironmentSummaryStatistics(DBMirror dbMirror, Environment environment) {
        Map<String, Number> stats = new TreeMap<>();

        for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
            EnvironmentTable et = tableMirror.getEnvironmentTable(environment);

            if (nonNull(et)) {
                Map<String, Number> tableStats = new TreeMap<>();
                // Go through the et statistics and for every numeric value, add it to the tableStats.
                et.getStatistics().forEach((k, v) -> {
                    if (v instanceof String) {
                        try {
                            tableStats.put(k, Double.parseDouble((String) v));
                        } catch (NumberFormatException nfe) {
                            log.error("Issue parsing Table Statistics for {}.{}", dbMirror.getName(), tableMirror.getName());
                            log.error(nfe.getMessage(), nfe);
                        }
                    } else if (v instanceof Number) {
                        tableStats.put(k, (Number) v);
                    }

                });
                // Add them to the overall stats.
                try {
                    tableStats.forEach((k, v) -> { //stats.merge(k, v, Double::sum));
                        /*
                        PARTITION_COUNT
                        AVG_FILE_SIZE (do this at the end)
                        DATA_SIZE
                        DIR_COUNT
                        FILE_COUNT
                         */
                        switch (k) {
                            case FILE_COUNT:
                                int fileCount = stats.get(FILE_COUNT) == null ? 0 : stats.get(FILE_COUNT).intValue();
                                fileCount += v.intValue();
                                stats.put(FILE_COUNT, fileCount);
                                break;
                            case DIR_COUNT:
                                int dirCount = stats.get(DIR_COUNT) == null ? 0 : stats.get(DIR_COUNT).intValue();
                                dirCount += v.intValue();
                                stats.put(DIR_COUNT, dirCount);
                                break;
                            case DATA_SIZE:
                                long dataSize = stats.get(DATA_SIZE) == null ? 0L : stats.get(DATA_SIZE).longValue();
                                dataSize += v.longValue();
                                stats.put(DATA_SIZE, dataSize);
                                break;
                            case PARTITION_COUNT:
                                int partitionCount = stats.get(PARTITION_COUNT) == null ? 0 : stats.get(PARTITION_COUNT).intValue();
                                partitionCount += v.intValue();
                                stats.put(PARTITION_COUNT, partitionCount);
                                break;
                        }
                    });
                } catch (RuntimeException e) {
                    log.error("Issue aggregating Table Statistics for {}.{}", dbMirror.getName(), tableMirror.getName());
                    log.error(e.getMessage(), e);
                }
            }
        }
        Long totalDataSize = stats.get(DATA_SIZE) == null ? 0L : stats.get(DATA_SIZE).longValue();
        Integer totalFileCount = stats.get(FILE_COUNT) == null ? 0 : stats.get(FILE_COUNT).intValue();
        try {
            if (totalFileCount > 0 && totalDataSize > 0) {
                stats.put(AVG_FILE_SIZE, totalDataSize / totalFileCount);
            }
        } catch (ArithmeticException ae) {
            log.error("Issue calculating AVG_FILE_SIZE for {}", dbMirror.getName());
            log.error(ae.getMessage(), ae);
        }

        return stats;
    }
}
