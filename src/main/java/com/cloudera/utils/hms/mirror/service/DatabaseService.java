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

    public Warehouse addWarehousePlan(String database, String external, String managed) throws RequiredConfigurationException {
        if (isBlank(external) || isBlank(managed)) {
            throw new RequiredConfigurationException("External and Managed Warehouse Locations must be defined.");
        }
        if (external.equals(managed)) {
            throw new RequiredConfigurationException("External and Managed Warehouse Locations must be different.");
        }
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        hmsMirrorConfig.getDatabases().add(database);
        return warehouseMapBuilder.addWarehousePlan(database, external, managed);
    }

    public Warehouse removeWarehousePlan(String database) {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        hmsMirrorConfig.getDatabases().remove(database);
        return warehouseMapBuilder.removeWarehousePlan(database);
    }

    /*
    Look at the Warehouse Plans for a matching Database and pull that.  If that doesn't exist, then
    pull the general warehouse locations if they are defined.  If those aren't, try and pull the locations
    from the Hive Environment Variables

    A null returns means a warehouse couldn't be determined and the db location settings should be skipped.
     */
    public Warehouse getWarehousePlan(String database) throws MissingDataPointException {
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = config.getTranslator().getWarehouseMapBuilder();
        // Find it by a Warehouse Plan
        Warehouse warehouse = warehouseMapBuilder.getWarehousePlans().get(database);
        if (isNull(warehouse)) {
            ExecuteSession session = executeSessionService.getSession();
            // Get the default Warehouse defined for the config.
            if (nonNull(session.getConfig().getTransfer().getWarehouse())) {
                warehouse = session.getConfig().getTransfer().getWarehouse();
            }

            if (nonNull(warehouse) && (isBlank(warehouse.getExternalDirectory()) || isBlank(warehouse.getManagedDirectory()))) {
                warehouse = null;
            }

            if (isNull(warehouse)) {
                // Look for Location in the right DB Definition for Migration Strategies.
                switch (config.getDataStrategy()) {
                    case SCHEMA_ONLY:
                    case EXPORT_IMPORT:
                    case HYBRID:
                    case SQL:
                    case COMMON:
                    case LINKED:
                        warehouse = config.getCluster(Environment.RIGHT).getEnvironmentWarehouse();
                        if (nonNull(warehouse)) {
                            session.addWarning(WAREHOUSE_DIRECTORIES_RETRIEVED_FROM_HIVE_ENV);
                        } else {
                            session.addWarning(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
                        }
                        break;
                    default: // STORAGE_MIGRATION should set these manually.
                        session.addWarning(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
                }
            }
        }

        if (isNull(warehouse)) {
            throw new MissingDataPointException("Warehouse Plan for Database: " + database + " not found and couldn't be built from (Warehouse Plans, General Warehouse Configs or Hive ENV.");
        }

        return warehouse;
    }

    public Map<String, Warehouse> getWarehousePlans() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        return warehouseMapBuilder.getWarehousePlans();
    }

    public void clearWarehousePlan() {
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = config.getTranslator().getWarehouseMapBuilder();
        warehouseMapBuilder.clearWarehousePlan();
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
                    throw new RuntimeException(e);
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
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
//        String database = tableMirror.getParent().getName();
//        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
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
                    String table = resultSet.getString(1);
                    String tableType = resultSet.getString(2);
                    String location = resultSet.getString(3);
                    // Filter out some table types. Don't transfer previously moved tables or
                    // interim tables created by hms-mirror.
                    if (table.startsWith(hmsMirrorConfig.getTransfer().getTransferPrefix())
                            || table.endsWith("storage_migration")) {
                        log.warn("Database: {} Table: {} was NOT added to list.  The name matches the transfer prefix " +
                                "and is most likely a remnant of a previous event. If this is a mistake, change the " +
                                "'transferPrefix' to something more unique.", database, table);
                    } else {
                        hmsMirrorConfig.getTranslator().addTableSource(database, table, tableType, location, consolidationLevelBase,
                                partitionLevelMismatch);
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
                    String table = resultSet.getString(1);
                    String tableType = resultSet.getString(2);
                    String partitionSpec = resultSet.getString(3);
                    String tableLocation = resultSet.getString(4);
                    String partitionLocation = resultSet.getString(5);
                    hmsMirrorConfig.getTranslator().addPartitionSource(database, table, tableType, partitionSpec,
                            tableLocation, partitionLocation, consolidationLevelBase, partitionLevelMismatch);
                }
            }


            log.info("Loaded Database Table/Partition Locations from Metastore Direct Connection {}:{}", environment, database);
        } catch (SQLException throwables) {
            log.error("Issue loading Table/Partition Locations from Metastore Direct Connection. {}:{}", environment, database);
            log.error(throwables.getMessage(), throwables);
            throw new RuntimeException(throwables);
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
                    throw new RuntimeException(sql);
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
            throw new RuntimeException(se);
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
        boolean rtn = Boolean.TRUE; // assume all good till we find otherwise.
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();

        boolean buildLeft = Boolean.FALSE;
        boolean buildRight = Boolean.TRUE;
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
                } else {
                    buildRight = Boolean.TRUE;
                }
                // Build the Right Def as a Clone of the Left to Seed it.
                if (isNull(dbDefRight)) {
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

            String originalLocation = dbMirror.getProperty(Environment.LEFT, DB_LOCATION);
            String originalManagedLocation = dbMirror.getProperty(Environment.LEFT, DB_MANAGED_LOCATION);

//            String leftNamespace = config.getCluster(Environment.LEFT).getHcfsNamespace();
//            String rightNamespace = config.getCluster(Environment.RIGHT).getHcfsNamespace();
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
            Warehouse warehouse = getWarehousePlan(dbMirror.getName());
            Warehouse envWarehouse = null;
            if (config.getDataStrategy() != DataStrategyEnum.DUMP) {
                envWarehouse = config.getCluster(Environment.RIGHT).getEnvironmentWarehouse();
            } else {
                envWarehouse = config.getCluster(Environment.LEFT).getEnvironmentWarehouse();
            }

            String targetLocation = null;
            String targetManagedLocation = null;

            if (buildRight) {
                // Configure the DB 'LOCATION'
                if (!isBlank(originalLocation) || forceLocations) {
                    // Get the base location without the original namespace.
                    targetLocation = NamespaceUtils.stripNamespace(originalLocation);
                    // Only set to warehouse location if the translation type is 'ALIGNED',
                    //   otherwise we want to keep the same relative location.
                    if (nonNull(warehouse) && config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.ALIGNED) {
                        switch (warehouse.getSource()) {
                            case PLAN:
                            case GLOBAL:
                                targetLocation = warehouse.getExternalDirectory() + "/" + targetDatabase + ".db";
                                break;
                            default:
                                // Didn't find an explicit location. So we're going to leave it as 'relative'.
                                // This handles any DB rename process.
                                targetLocation = NamespaceUtils.getParentDirectory(targetLocation);
                                targetLocation = targetLocation + "/" + targetDatabase + ".db";
                                break;
                        }
                        // Check the location against the ENV warehouse location.  If they match, don't set the location.
                        if (nonNull(envWarehouse)) {
                            String reducedTargetLocation = NamespaceUtils.getParentDirectory(targetLocation);
                            if (reducedTargetLocation.equals(envWarehouse.getExternalDirectory())) {
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
                        targetLocation = targetNamespace + targetLocation;
//                        dbMirror.getP
                    }
                }
                // Configure the DB 'MANAGEDLOCATION'
                if (!isBlank(originalManagedLocation) || forceLocations) {
                    // Get the base location without the original namespace.
                    targetManagedLocation = NamespaceUtils.stripNamespace(originalManagedLocation);
                    // Only set to warehouse location if the translation type is 'ALIGNED',
                    //   otherwise we want to keep the same relative location.
                    if (nonNull(warehouse) && config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.ALIGNED) {
                        switch (warehouse.getSource()) {
                            case PLAN:
                            case GLOBAL:
                                targetManagedLocation = warehouse.getManagedDirectory() + "/" + targetDatabase + ".db";
                                break;
                        }
                        // Check the location against the ENV warehouse location.  If they match, don't set the location.
                        if (nonNull(envWarehouse)) {
                            String reducedTargetLocation = NamespaceUtils.getParentDirectory(targetManagedLocation);
                            if (reducedTargetLocation.equals(envWarehouse.getManagedDirectory())) {
                                // The new target location is the same as the ENV warehouse location.  So we're not
                                // going to set it and let it default to the ENV warehouse location.
                                dbDefRight.put(DB_MANAGED_LOCATION, targetNamespace + targetManagedLocation);
                                dbMirror.addIssue(Environment.RIGHT, "The database 'Managed' location is the same as the ENV warehouse Managed location.  The database location will NOT be set and will depend on the ENV warehouse location.");
                                targetManagedLocation = null;
                            }
                        }
                    }
                    if (nonNull(targetManagedLocation)) {
                        // Add the Namespace.
                        targetManagedLocation = targetNamespace + targetManagedLocation;
                    }
                }

                // Upsert Source DB Parameters to Target DB Parameters.
                DatabaseUtils.upsertParameters(dbDefLeft, dbDefRight, skipList);

                // Deal with ReadOnly.
                if (config.isReadOnly()) {
                    if (nonNull(targetLocation)) {
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
                        throw new RuntimeException(targetDatabase + ": Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in " +
                                "'read-only' mode without knowing where it should go and validating existence.");
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
                    String createDb = MessageFormat.format(CREATE_DB, originalDatabase);
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
                    if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
                        String alterDbLoc = MessageFormat.format(ALTER_DB_LOCATION, originalDatabase, targetLocation);
                        dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbLoc));
                        dbDefRight.put(DB_LOCATION, targetLocation);
                    }
                    if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
                        String alterDbMngdLoc = MessageFormat.format(ALTER_DB_MNGD_LOCATION, originalDatabase, targetManagedLocation);
                        dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
                        dbDefRight.put(DB_MANAGED_LOCATION, targetManagedLocation);
                    } else {
                        String alterDbMngdLoc = MessageFormat.format(ALTER_DB_LOCATION, originalDatabase, targetManagedLocation);
                        dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
                        dbMirror.addIssue(Environment.LEFT, HDPHIVE3_DB_LOCATION.getDesc());
                        dbDefRight.put(DB_LOCATION, targetManagedLocation);
                    }

                    dbMirror.addIssue(Environment.LEFT, "This process, when 'executed' will leave the original tables intact in their renamed " +
                            "version.  They are NOT automatically cleaned up.  Run the produced '" +
                            dbMirror.getName() + "_LEFT_CleanUp_execute.sql' " +
                            "file to permanently remove them.  Managed and External/Purge table data will be " +
                            "removed when dropping these tables.  External non-purge table data will remain in storage.");

                    break;
                default:
                    String createDbL = MessageFormat.format(CREATE_DB, targetDatabase);
                    StringBuilder sbL = new StringBuilder();
                    sbL.append(createDbL).append("\n");
                    if (dbDefLeft.get(COMMENT) != null && !dbDefLeft.get(COMMENT).trim().isEmpty()) {
                        sbL.append(COMMENT).append(" \"").append(dbDefLeft.get(COMMENT)).append("\"\n");
                    }
                    dbMirror.getSql(Environment.RIGHT).add(new Pair(CREATE_DB_DESC, sbL.toString()));

                    if (nonNull(targetLocation) && !config.getCluster(Environment.RIGHT).isHdpHive3()) {
                        String alterDbLoc = MessageFormat.format(ALTER_DB_LOCATION, targetDatabase, targetLocation);
                        dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbLoc));
                        dbDefRight.put(DB_LOCATION, targetLocation);
                    }
                    if (nonNull(targetManagedLocation)) {
                        if (!config.getCluster(Environment.RIGHT).isHdpHive3()) {
                            String alterDbMngdLoc = MessageFormat.format(ALTER_DB_MNGD_LOCATION, targetDatabase, targetManagedLocation);
                            dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
                            dbDefRight.put(DB_MANAGED_LOCATION, targetManagedLocation);
                        } else {
                            String alterDbMngdLoc = MessageFormat.format(ALTER_DB_LOCATION, targetDatabase, targetManagedLocation);
                            dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
                            dbMirror.addIssue(Environment.RIGHT, HDPHIVE3_DB_LOCATION.getDesc());
                            dbDefRight.put(DB_LOCATION, targetManagedLocation);
                        }
                    }

                    // Built the DBPROPERITES
                    Map<String, String> dbProperties = DatabaseUtils.getParameters(dbDefRight, skipList);
                    if (!dbProperties.isEmpty()) {
                        for (Map.Entry<String, String> entry : dbProperties.entrySet()) {
                            String alterDbProps = MessageFormat.format(ALTER_DB_PROPERTIES, targetDatabase, entry.getKey(), entry.getValue());
                            dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_PROPERTIES_DESC, alterDbProps));
                        }
                    }

                    if (config.isTransferOwnership()) {
                        String ownerFromLeft = dbDefLeft.get(OWNER_NAME);
                        String ownerTypeFromLeft = dbDefLeft.get(OWNER_TYPE);
                        if (nonNull(ownerFromLeft) && nonNull(ownerTypeFromLeft)) {
                            log.info("Setting Owner: {} of type: {} on Database: {}", ownerFromLeft, ownerTypeFromLeft, targetDatabase);
                            if (ownerTypeFromLeft.equals("USER")) {
                                String alterOwner = MessageFormat.format(SET_OWNER, targetDatabase, ownerFromLeft);
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(SET_OWNER_DESC, alterOwner));
                            }
                        }
                    }

                    break;

            }
            // ================================================

/*
            // Reset Right mean drop and recreate the database.
            if (!config.isResetRight()) {
                // Don't buildout RIGHT side with inplace downgrade of ACID tables.
                if (!config.getMigrateACID().isDowngradeInPlace()) {
                    switch (config.getDataStrategy()) {
                        case CONVERT_LINKED:
                            // ALTER the 'existing' database to ensure locations are set to the RIGHT hcfsNamespace.
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

                            break;
                        case DUMP:
                            // Build LEFT DB SQL.
                            break;
                        default:
                            // Start with the LEFT definition.
                            database = HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config);
                            originalLocation = dbDefLeft.get(DB_LOCATION);

//                            If location is not found, don't set it on the other side unless there is a warehouse for the database defined.

                            if (!isBlank(originalLocation)) {
                                // We can attempt to set the RIGHT DB_LOCATION.
                                String rightLocation = NamespaceUtils.stripNamespace(originalLocation);
                                if (nonNull(warehouse)) {
                                    switch (warehouse.getSource()) {
                                        case PLAN:
                                        case GLOBAL:
                                            rightLocation = warehouse.getExternalDirectory() + "/" + HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config) + ".db";
                                            break;
                                    }
                                    Warehouse envWarehouse = config.getCluster(Environment.RIGHT).getEnvironmentWarehouse();
                                    if (nonNull(envWarehouse)) {
                                        // Compare the ENV warehouse to the DB warehouse.
                                        if (envWarehouse.getExternalDirectory().equals(warehouse.getExternalDirectory())) {
                                            // The ENV warehouse is the same as the DB warehouse.  We don't need to set the location.
                                            rightLocation = null;
                                        }
                                    }

//                                    }
                                    switch (warehouse.getSource()) {
                                        case PLAN:
                                        case GLOBAL:
                                            rightLocation = warehouse.getExternalDirectory() + "/" + HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config) + ".db";
                                            break;
                                    }

                                }
                            }
//                            Warehouse dbWarehouse = null;
//                        try {
//                            dbWarehouse = getWarehousePlan(dbMirror.getName());
                            // A null warehouse means we should use the relative directories.
//                            assert dbWarehouse != null;
//                        } catch (MissingDataPointException e) {
//                            dbMirror.addIssue(Environment.LEFT, "TODO: Missing Warehouse details...");
//                            return Boolean.FALSE;
//                        }
//                            if (isBlank(config.getTransfer().getTargetNamespace()) && nonNull(dbWarehouse)) {
                            originalLocation = config.getTargetNamespace()
                                    + warehouse.getExternalDirectory()
                                    + "/" + dbMirror.getName() + ".db";
//                            } else if (!isBlank(config.getTransfer().getTargetNamespace()) && nonNull(dbWarehouse)) {
//                                location = config.getTransfer().getTargetNamespace()
//                                        + dbWarehouse.getExternalDirectory()
//                                        + "/" + dbMirror.getName() + ".db";
//                            }

                            if (!config.getCluster(Environment.LEFT).isLegacyHive()) {
                                // Check for Managed Location.
                                originalManagedLocation = dbDefLeft.get(DB_MANAGED_LOCATION);
                            }

                            if (!config.getCluster(Environment.RIGHT).isLegacyHive()) {
//                                if (isBlank(config.getTransfer().getTargetNamespace()) && nonNull(dbWarehouse)) {
                                originalManagedLocation = config.getTargetNamespace() //getCluster(Environment.RIGHT).getHcfsNamespace()
                                        + warehouse.getManagedDirectory()
                                        + "/" + dbMirror.getName() + ".db";
//                                } else if (!isBlank(config.getTransfer().getTargetNamespace()) && nonNull(dbWarehouse)){
//                                    managedLocation = config.getTransfer().getTargetNamespace()
//                                            + dbWarehouse.getManagedDirectory()
//                                            + "/" + dbMirror.getName() + ".db";
//                                }
                                // If we've set the managedLocation, check to see if it's the same as the default
                                if (nonNull(originalManagedLocation)) {
                                    // Check is the Managed Location matches the system default.  If it does,
                                    //  then we don't need to set it.
                                    String envDefaultFS = config.getCluster(Environment.RIGHT).getEnvVars().get(DEFAULT_FS);
                                    String envWarehouseDir = config.getCluster(Environment.RIGHT).getEnvVars().get(MNGD_DB_LOCATION_PROP);
                                    String defaultManagedLocation = envDefaultFS + envWarehouseDir;
                                    log.info("Comparing Managed Location: {} to default: {}", originalManagedLocation, defaultManagedLocation);
                                    if (originalManagedLocation.startsWith(defaultManagedLocation)) {
                                        originalManagedLocation = null;
                                        log.info("The location for the DB '{}' is the same as the default FS + warehouse dir.  The database location will NOT be set and will depend on the system default.", database);
                                        dbMirror.addIssue(Environment.RIGHT, "The location for the DB '" + database
                                                + "' is the same as the default FS + warehouse dir. The database " +
                                                "location will NOT be set and will depend on the system default.");
                                    }
                                }
                            }

                            switch (config.getDataStrategy()) {
                                case HYBRID:
                                case EXPORT_IMPORT:
                                case SCHEMA_ONLY:
                                case SQL:
                                case LINKED:
                                    if (nonNull(originalLocation)) {

//                                        location = location.replace(leftNamespace, rightNamespace);
                                        // https://github.com/cloudera-labs/hms-mirror/issues/13
                                        // LOCATION to MANAGED LOCATION silent translation for HDP 3 migrations.
                                        if (!config.getCluster(Environment.LEFT).isLegacyHive()) {
                                            String locationMinusNS = originalLocation.substring(targetNamespace.length());
                                            if (locationMinusNS.startsWith(DEFAULT_MANAGED_BASE_DIR)) {
                                                // Translate to managed.
                                                originalManagedLocation = originalLocation;
                                                // Set to null to skip processing.
                                                originalLocation = null;
                                                dbMirror.addIssue(Environment.RIGHT, "The LEFT's DB 'LOCATION' element was defined " +
                                                        "as the default 'managed' location in later versions of Hive3.  " +
                                                        "We've adjusted the DB to set the MANAGEDLOCATION setting instead, " +
                                                        "to avoid future conflicts. If your target environment is HDP3, this setting " +
                                                        "will FAIL since the MANAGEDLOCATION property for a Database doesn't exist. " +
                                                        "Fix the source DB's location element to avoid this translation.");
                                            }
                                        }

                                    }
//                                    if (managedLocation != null) {
//                                        managedLocation = managedLocation.replace(leftNamespace, rightNamespace);
//                                    }
                                    if (config.getDbPrefix() != null || config.getDbRename() != null) {
                                        // adjust locations.
                                        if (originalLocation != null) {
                                            originalLocation = UrlUtils.removeLastDirFromUrl(originalLocation) + "/"
                                                    + HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config) + ".db";
                                        }
                                        if (originalManagedLocation != null) {
                                            originalManagedLocation = UrlUtils.removeLastDirFromUrl(originalManagedLocation) + "/"
                                                    + HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config) + ".db";
                                        }
                                    }
                                    if (config.isReadOnly() && !config.isLoadingTestData()) {
                                        log.debug("Config set to 'read-only'.  Validating FS before continuing");
                                        CliEnvironment cli = executeSessionService.getCliEnvironment();

                                        // Check that location exists.
                                        String dbLocation = null;
                                        if (nonNull(originalLocation)) {
                                            dbLocation = originalLocation;
                                        } else {
                                            // Get location for DB. If it's not there than:
                                            //     SQL query to get default from Hive.
                                            String defaultDBLocProp = null;
                                            if (config.getCluster(Environment.RIGHT).isLegacyHive()) {
                                                defaultDBLocProp = LEGACY_DB_LOCATION_PROP;
                                            } else {
                                                defaultDBLocProp = EXT_DB_LOCATION_PROP;
                                            }

                                            Connection conn = null;
                                            Statement stmt = null;
                                            ResultSet resultSet = null;
                                            try {
                                                conn = connectionPoolService.getHS2EnvironmentConnection(Environment.RIGHT);
                                                //hmsMirrorConfig.getCluster(Environment.RIGHT).getConnection();

                                                stmt = conn.createStatement();

                                                String dbLocSql = "SET " + defaultDBLocProp;
                                                resultSet = stmt.executeQuery(dbLocSql);
                                                if (resultSet.next()) {
                                                    String propset = resultSet.getString(1);
                                                    String dbLocationPrefix = propset.split("=")[1];
                                                    dbLocation = dbLocationPrefix + dbMirror.getName().toLowerCase(Locale.ROOT) + ".db";
                                                    log.debug("{} location is: {}", database, dbLocation);

                                                } else {
                                                    // Could get property.
                                                    throw new RuntimeException("Could not determine DB Location for: " + database);
                                                }
                                            } catch (SQLException throwables) {
                                                log.error("Issue", throwables);
                                                throw new RuntimeException(throwables);
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
                                                if (conn != null) {
                                                    try {
                                                        conn.close();
                                                    } catch (SQLException throwables) {
                                                        //
                                                    }
                                                }
                                            }
                                        }
                                        if (nonNull(dbLocation)) {
                                            try {
//                                            CommandReturn cr = cli.processInput("connect");
                                                CommandReturn testCr = cli.processInput("test -d " + dbLocation);
                                                if (testCr.isError()) {
                                                    // Doesn't exist.  So we can't create the DB in a "read-only" mode.
                                                    runStatus.addError(RO_DB_DOESNT_EXIST, dbLocation,
                                                            testCr.getError(), testCr.getCommand(), dbMirror.getName());
                                                    dbMirror.addIssue(Environment.RIGHT, runStatus.getErrorMessage(RO_DB_DOESNT_EXIST));
                                                    rtn = Boolean.FALSE;
//                                                throw new RuntimeException(hmsMirrorConfig.getProgression().getErrorMessage(RO_DB_DOESNT_EXIST));
                                                }
                                            } catch (DisabledException e) {
                                                log.warn("Unable to test location {} because the CLI Interface is disabled.", dbLocation);
                                                dbMirror.addIssue(Environment.RIGHT, "Unable to test location " + dbLocation + " because the CLI Interface is disabled. " +
                                                        "This may lead to issues when creating the database in 'read-only' mode.  ");
                                            }
                                        } else {
                                            // Can't determine location.
                                            // TODO: What to do here.
                                            log.error("{}: Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in 'read-only' mode without knowing where it should go and validating existance.", dbMirror.getName());
                                            throw new RuntimeException(dbMirror.getName() + ": Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in " +
                                                    "'read-only' mode without knowing where it should go and validating existance.");
                                        }
//                                    } finally {
//                                        config.getCliEnv().returnSession(main);
//                                    }
                                    }

                                    String createDbL = MessageFormat.format(CREATE_DB, database);
                                    StringBuilder sbL = new StringBuilder();
                                    sbL.append(createDbL).append("\n");
                                    if (dbDefLeft.get(COMMENT) != null && !dbDefLeft.get(COMMENT).trim().isEmpty()) {
                                        sbL.append(COMMENT).append(" \"").append(dbDefLeft.get(COMMENT)).append("\"\n");
                                    }
                                    // TODO: DB Properties.
                                    dbMirror.getSql(Environment.RIGHT).add(new Pair(CREATE_DB_DESC, sbL.toString()));

                                    if (nonNull(originalLocation) && !config.getCluster(Environment.RIGHT).isHdpHive3()) {
                                        String alterDbLoc = MessageFormat.format(ALTER_DB_LOCATION, database, originalLocation);
                                        dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbLoc));
                                        dbDefRight.put(DB_LOCATION, originalLocation);
                                    }
                                    if (nonNull(originalManagedLocation)) {
                                        if (!config.getCluster(Environment.RIGHT).isHdpHive3()) {
                                            String alterDbMngdLoc = MessageFormat.format(ALTER_DB_MNGD_LOCATION, database, originalManagedLocation);
                                            dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
                                            dbDefRight.put(DB_MANAGED_LOCATION, originalManagedLocation);
                                        } else {
                                            String alterDbMngdLoc = MessageFormat.format(ALTER_DB_LOCATION, database, originalManagedLocation);
                                            dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
                                            dbMirror.addIssue(Environment.RIGHT, HDPHIVE3_DB_LOCATION.getDesc());
                                            dbDefRight.put(DB_LOCATION, originalManagedLocation);
                                        }
                                    }

                                    break;
                                case DUMP:
                                    String createDb = MessageFormat.format(CREATE_DB, database);
                                    StringBuilder sb = new StringBuilder();
                                    sb.append(createDb).append("\n");
                                    if (dbDefLeft.get(COMMENT) != null && !dbDefLeft.get(COMMENT).trim().isEmpty()) {
                                        sb.append(COMMENT).append(" \"").append(dbDefLeft.get(COMMENT)).append("\"\n");
                                    }
                                    if (originalLocation != null) {
                                        sb.append(DB_LOCATION).append(" \"").append(originalLocation).append("\"\n");
                                    }
                                    if (originalManagedLocation != null) {
                                        sb.append(DB_MANAGED_LOCATION).append(" \"").append(originalManagedLocation).append("\"\n");
                                    }
                                    // TODO: DB Properties.
                                    dbMirror.getSql(Environment.LEFT).add(new Pair(CREATE_DB_DESC, sb.toString()));
                                    break;
                                case COMMON:
                                    String createDbCom = MessageFormat.format(CREATE_DB, database);
                                    StringBuilder sbCom = new StringBuilder();
                                    sbCom.append(createDbCom).append("\n");
                                    if (dbDefLeft.get(COMMENT) != null && !dbDefLeft.get(COMMENT).trim().isEmpty()) {
                                        sbCom.append(COMMENT).append(" \"").append(dbDefLeft.get(COMMENT)).append("\"\n");
                                    }
                                    if (originalLocation != null) {
                                        sbCom.append(DB_LOCATION).append(" \"").append(originalLocation).append("\"\n");
                                    }
                                    if (originalManagedLocation != null) {
                                        sbCom.append(DB_MANAGED_LOCATION).append(" \"").append(originalManagedLocation).append("\"\n");
                                    }
                                    // TODO: DB Properties.
                                    dbMirror.getSql(Environment.RIGHT).add(new Pair(CREATE_DB_DESC, sbCom.toString()));
                                    break;
                                case STORAGE_MIGRATION:
//                                StringBuilder sbLoc = new StringBuilder();
//                                sbLoc.append(hmsMirrorConfig.getTransfer().getTargetNamespace());
//                                sbLoc.append(hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory());
//                                sbLoc.append("/");
//                                sbLoc.append(database);
//                                sbLoc.append(".db");
                                    if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
                                        String alterDbLoc = MessageFormat.format(ALTER_DB_LOCATION, database, originalLocation);
                                        dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbLoc));
                                        dbDefRight.put(DB_LOCATION, originalLocation);
                                    }

//                                StringBuilder sbMngdLoc = new StringBuilder();
//                                sbMngdLoc.append(hmsMirrorConfig.getTransfer().getTargetNamespace());
//                                sbMngdLoc.append(hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory());
//                                sbMngdLoc.append("/");
//                                sbMngdLoc.append(database);
//                                sbMngdLoc.append(".db");
                                    if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
                                        String alterDbMngdLoc = MessageFormat.format(ALTER_DB_MNGD_LOCATION, database, originalManagedLocation);
                                        dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
                                        dbDefRight.put(DB_MANAGED_LOCATION, originalManagedLocation);
                                    } else {
                                        String alterDbMngdLoc = MessageFormat.format(ALTER_DB_LOCATION, database, originalManagedLocation);
                                        dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
                                        dbMirror.addIssue(Environment.LEFT, HDPHIVE3_DB_LOCATION.getDesc());
                                        dbDefRight.put(DB_LOCATION, originalManagedLocation);
                                    }

                                    dbMirror.addIssue(Environment.LEFT, "This process, when 'executed' will leave the original tables intact in their renamed " +
                                            "version.  They are NOT automatically cleaned up.  Run the produced '" +
                                            dbMirror.getName() + "_LEFT_CleanUp_execute.sql' " +
                                            "file to permanently remove them.  Managed and External/Purge table data will be " +
                                            "removed when dropping these tables.  External non-purge table data will remain in storage.");

                                    break;
                            }
                    }
                } else {
                    // Downgrade in place.
//                    if (config.getTransfer().getWarehouse().getExternalDirectory() != null) {
                    // Set the location to the external directory for the database.
                    database = HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config);
                    originalLocation = config.getCluster(Environment.LEFT).getHcfsNamespace() + warehouse.getExternalDirectory() +
                            "/" + database + ".db";
                    String alterDB_location = MessageFormat.format(ALTER_DB_LOCATION, database, originalLocation);
                    dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDB_location));
                    dbDefLeft.put(DB_LOCATION, originalLocation);
//                    }
                }
            } else {
                // Reset Right DB.
                database = HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config);
                String dropDb = MessageFormat.format(DROP_DB, database);
                dbMirror.getSql(Environment.RIGHT).add(new Pair(DROP_DB_DESC, dropDb));
            }
            */
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

        if (config.getMigrateACID().isDowngradeInPlace() && config.getDataStrategy() == DataStrategyEnum.SQL) {
            log.info("Downgrade in place.  Skipping database creation.");
            return true;
        }

        Conversion conversion = session.getConversion();
        RunStatus runStatus = session.getRunStatus();
        OperationStatistics stats = runStatus.getOperationStatistics();
        for (String database : config.getDatabases()) {
            DBMirror dbMirror = conversion.getDatabase(database);
            rtn = buildDBStatements(dbMirror);

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

                log.debug("{}:{}: Loading database definition.", environment, database);

                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    log.debug("{}:{}: Getting Database Definition", environment, database);
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
                    log.error("Issue with collecting DB Definition.", sql);
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
        for (Pair pair : dbPairs) {
            if (!runDatabaseSql(dbMirror, pair, environment)) {
                rtn = Boolean.FALSE;
                // don't continue
                break;
            }
        }
        return rtn;
    }

    public Boolean runDatabaseSql(DBMirror dbMirror, Pair dbSqlPair, Environment environment) {
        // Open the connections and ensure we are running this on the "RIGHT" cluster.
        Connection conn = null;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        Boolean rtn = Boolean.TRUE;
        // Skip when running test data.
        if (!hmsMirrorConfig.isLoadingTestData()) {
            try {
                conn = connectionPoolService.getHS2EnvironmentConnection(environment);

                if (isNull(conn) && hmsMirrorConfig.isExecute()
                        && !hmsMirrorConfig.getCluster(environment).getHiveServer2().isDisconnected()) {
                    // this is a problem.
                    rtn = Boolean.FALSE;
                    dbMirror.addIssue(environment, "Connection missing. This is a bug.");
                }

                if (isNull(conn) && hmsMirrorConfig.getCluster(environment).getHiveServer2().isDisconnected()) {
                    dbMirror.addIssue(environment, "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                            "The scripts will need to be run 'manually'.");
                }

                if (!isNull(conn)) {
                    if (dbMirror != null)
                        log.debug("{} - {}: {}", environment, dbSqlPair.getDescription(), dbMirror.getName());
                    else
                        log.debug("{} - {}:{}", environment, dbSqlPair.getDescription(), dbSqlPair.getAction());

                    Statement stmt = null;
                    try {
                        try {
                            stmt = conn.createStatement();
                        } catch (SQLException throwables) {
                            log.error("Issue building statement", throwables);
                            rtn = Boolean.FALSE;
                        }

                        try {
                            log.debug("{}:{}:{}", environment, dbSqlPair.getDescription(), dbSqlPair.getAction());
                            if (hmsMirrorConfig.isExecute()) // on dry-run, without db, hard to get through the rest of the steps.
                                stmt.execute(dbSqlPair.getAction());
                        } catch (SQLException throwables) {
                            log.error("{}:{}:", environment, dbSqlPair.getDescription(), throwables);
                            dbMirror.addIssue(environment, throwables.getMessage() + " " + dbSqlPair.getDescription() +
                                    " " + dbSqlPair.getAction());
                            rtn = Boolean.FALSE;
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
                }
            } catch (SQLException throwables) {
                log.error(environment.toString(), throwables);
                throw new RuntimeException(throwables);
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

}
