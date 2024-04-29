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
import com.cloudera.utils.hms.mirror.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static com.cloudera.utils.hms.mirror.MessageCode.HDPHIVE3_DB_LOCATION;
import static com.cloudera.utils.hms.mirror.MessageCode.RO_DB_DOESNT_EXIST;
import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hms.mirror.SessionVars.EXT_DB_LOCATION_PROP;
import static com.cloudera.utils.hms.mirror.SessionVars.LEGACY_DB_LOCATION_PROP;

@Service
@Slf4j
@Getter
public class DatabaseService {

    private ConnectionPoolService connectionPoolService;
    private HmsMirrorCfgService hmsMirrorCfgService;
    private Conversion conversion;

    public boolean buildDBStatements(DBMirror dbMirror) {
//        Config config = Context.getInstance().getConfig();
        boolean rtn = Boolean.TRUE; // assume all good till we find otherwise.
        HmsMirrorConfig hmsMirrorConfig = hmsMirrorCfgService.getHmsMirrorConfig();
        // Start with the LEFT definition.
        Map<String, String> dbDefLeft = dbMirror.getDBDefinition(Environment.LEFT);
        Map<String, String> dbDefRight = dbMirror.getDBDefinition(Environment.RIGHT);
        String database = null;
        String location = null;
        String managedLocation = null;
        String leftNamespace = hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace();
        String rightNamespace = hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace();

        if (!hmsMirrorConfig.isResetRight()) {
            // Don't buildout RIGHT side with inplace downgrade of ACID tables.
            if (!hmsMirrorConfig.getMigrateACID().isDowngradeInPlace()) {
                switch (hmsMirrorConfig.getDataStrategy()) {
                    case CONVERT_LINKED:
                        // ALTER the 'existing' database to ensure locations are set to the RIGHT hcfsNamespace.
                        database = getHmsMirrorCfgService().getResolvedDB(dbMirror.getName());
                        location = dbDefRight.get(DB_LOCATION);
                        managedLocation = dbDefRight.get(MirrorConf.DB_MANAGED_LOCATION);

                        if (location != null && !hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
                            location = location.replace(leftNamespace, rightNamespace);
                            String alterDB_location = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, location);
                            dbMirror.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDB_location));
                            dbDefRight.put(DB_LOCATION, location);
                        }
                        if (managedLocation != null) {
                            managedLocation = managedLocation.replace(leftNamespace, rightNamespace);
                            if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
                                String alterDBMngdLocationSql = MessageFormat.format(MirrorConf.ALTER_DB_MNGD_LOCATION, database, managedLocation);
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_MNGD_LOCATION_DESC, alterDBMngdLocationSql));
                            } else {
                                String alterDBMngdLocationSql = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, managedLocation);
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDBMngdLocationSql));
                                dbMirror.addIssue(Environment.RIGHT, HDPHIVE3_DB_LOCATION.getDesc());
                            }
                            dbDefRight.put(MirrorConf.DB_MANAGED_LOCATION, managedLocation);
                        }

                        break;
                    default:
                        // Start with the LEFT definition.
                        dbDefLeft = dbMirror.getDBDefinition(Environment.LEFT);
                        database = getHmsMirrorCfgService().getResolvedDB(dbMirror.getName());
                        location = dbDefLeft.get(DB_LOCATION);
                        if (hmsMirrorConfig.getTransfer().getWarehouse() != null
                                && hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() != null) {
                            if (hmsMirrorConfig.getTransfer().getCommonStorage() == null) {
                                location = hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace()
                                        + hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory()
                                        + "/" + dbMirror.getName() + ".db";
                            } else {
                                location = hmsMirrorConfig.getTransfer().getCommonStorage()
                                        + hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory()
                                        + "/" + dbMirror.getName() + ".db";
                            }
                        }

                        if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                            // Check for Managed Location.
                            managedLocation = dbDefLeft.get(MirrorConf.DB_MANAGED_LOCATION);
                        }
                        if (hmsMirrorConfig.getTransfer().getWarehouse() != null
                                && hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory() != null
                                && !hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                            if (hmsMirrorConfig.getTransfer().getCommonStorage() == null) {
                                managedLocation = hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace()
                                        + hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory()
                                        + "/" + dbMirror.getName() + ".db";
                            } else {
                                managedLocation = hmsMirrorConfig.getTransfer().getCommonStorage()
                                        + hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory()
                                        + "/" + dbMirror.getName() + ".db";
                            }
                            // Check is the Managed Location matches the system default.  If it does,
                            //  then we don't need to set it.
                            String envDefaultFS = hmsMirrorConfig.getCluster(Environment.RIGHT).getEnvVars().get(DEFAULT_FS);
                            String envWarehouseDir = hmsMirrorConfig.getCluster(Environment.RIGHT).getEnvVars().get(METASTOREWAREHOUSE);
                            String defaultManagedLocation = envDefaultFS + envWarehouseDir;
                            log.info("Comparing Managed Location: {} to default: {}", managedLocation, defaultManagedLocation);
                            if (managedLocation.startsWith(defaultManagedLocation)) {
                                managedLocation = null;
                                log.info("The location for the DB '{}' is the same as the default FS + warehouse dir.  The database location will NOT be set and will depend on the system default.", database);
                                dbMirror.addIssue(Environment.RIGHT, "The location for the DB '" + database
                                        + "' is the same as the default FS + warehouse dir. The database " +
                                        "location will NOT be set and will depend on the system default.");
                            }
                        }

                        switch (hmsMirrorConfig.getDataStrategy()) {
                            case HYBRID:
                            case EXPORT_IMPORT:
                            case SCHEMA_ONLY:
                            case SQL:
                            case LINKED:
                                if (location != null) {
                                    location = location.replace(leftNamespace, rightNamespace);
                                    // https://github.com/cloudera-labs/hms-mirror/issues/13
                                    // LOCATION to MANAGED LOCATION silent translation for HDP 3 migrations.
                                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                                        String locationMinusNS = location.substring(rightNamespace.length());
                                        if (locationMinusNS.startsWith(MirrorConf.DEFAULT_MANAGED_BASE_DIR)) {
                                            // Translate to managed.
                                            managedLocation = location;
                                            // Set to null to skip processing.
                                            location = null;
                                            dbMirror.addIssue(Environment.RIGHT, "The LEFT's DB 'LOCATION' element was defined " +
                                                    "as the default 'managed' location in later versions of Hive3.  " +
                                                    "We've adjusted the DB to set the MANAGEDLOCATION setting instead, " +
                                                    "to avoid future conflicts. If your target environment is HDP3, this setting " +
                                                    "will FAIL since the MANAGEDLOCATION property for a Database doesn't exist. " +
                                                    "Fix the source DB's location element to avoid this translation.");
                                        }
                                    }

                                }
                                if (managedLocation != null) {
                                    managedLocation = managedLocation.replace(leftNamespace, rightNamespace);
                                }
                                if (hmsMirrorConfig.getDbPrefix() != null || hmsMirrorConfig.getDbRename() != null) {
                                    // adjust locations.
                                    if (location != null) {
                                        location = Translator.removeLastDirFromUrl(location) + "/" + getHmsMirrorCfgService().getResolvedDB(dbMirror.getName()) + ".db";
                                    }
                                    if (managedLocation != null) {
                                        managedLocation = Translator.removeLastDirFromUrl(managedLocation) + "/" + getHmsMirrorCfgService().getResolvedDB(dbMirror.getName()) + ".db";
                                    }
                                }
                                if (hmsMirrorConfig.isReadOnly() && !hmsMirrorConfig.isLoadingTestData()) {
                                    log.debug("Config set to 'read-only'.  Validating FS before continuing");
                                    CliEnvironment cli = getHmsMirrorCfgService().getHmsMirrorConfig().getCliEnvironment();

                                    // Check that location exists.
                                    String dbLocation = null;
                                    if (location != null) {
                                        dbLocation = location;
                                    } else {
                                        // Get location for DB. If it's not there than:
                                        //     SQL query to get default from Hive.
                                        String defaultDBLocProp = null;
                                        if (hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                                            defaultDBLocProp = LEGACY_DB_LOCATION_PROP;
                                        } else {
                                            defaultDBLocProp = EXT_DB_LOCATION_PROP;
                                        }

                                        Connection conn = null;
                                        Statement stmt = null;
                                        ResultSet resultSet = null;
                                        try {
                                            conn = hmsMirrorConfig.getCluster(Environment.RIGHT).getConnection();

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
                                    if (dbLocation != null) {
                                        try {
//                                            CommandReturn cr = cli.processInput("connect");
                                            CommandReturn testCr = cli.processInput("test -d " + dbLocation);
                                            if (testCr.isError()) {
                                                // Doesn't exist.  So we can't create the DB in a "read-only" mode.
                                                hmsMirrorConfig.addError(RO_DB_DOESNT_EXIST, dbLocation,
                                                        testCr.getError(), testCr.getCommand(), dbMirror.getName());
                                                dbMirror.addIssue(Environment.RIGHT, hmsMirrorConfig.getProgression().getErrorMessage(RO_DB_DOESNT_EXIST));
                                                rtn = Boolean.FALSE;
//                                                throw new RuntimeException(hmsMirrorConfig.getProgression().getErrorMessage(RO_DB_DOESNT_EXIST));
                                            }
                                        } catch (DisabledException e) {
                                            log.warn("Unable to test location {} because the CLI Interface is disabled.", dbLocation);
                                            dbMirror.addIssue(Environment.RIGHT, "Unable to test location " + dbLocation + " because the CLI Interface is disabled. " +
                                                            "This may lead to issues when creating the database in 'read-only' mode.  " );
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

                                String createDbL = MessageFormat.format(MirrorConf.CREATE_DB, database);
                                StringBuilder sbL = new StringBuilder();
                                sbL.append(createDbL).append("\n");
                                if (dbDefLeft.get(MirrorConf.COMMENT) != null && !dbDefLeft.get(MirrorConf.COMMENT).trim().isEmpty()) {
                                    sbL.append(MirrorConf.COMMENT).append(" \"").append(dbDefLeft.get(MirrorConf.COMMENT)).append("\"\n");
                                }
                                // TODO: DB Properties.
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(MirrorConf.CREATE_DB_DESC, sbL.toString()));

                                if (location != null && !hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
                                    String alterDbLoc = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, location);
                                    dbMirror.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDbLoc));
                                    dbDefRight.put(DB_LOCATION, location);
                                }
                                if (managedLocation != null) {
                                    if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
                                        String alterDbMngdLoc = MessageFormat.format(MirrorConf.ALTER_DB_MNGD_LOCATION, database, managedLocation);
                                        dbMirror.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
                                        dbDefRight.put(DB_MANAGED_LOCATION, managedLocation);
                                    } else {
                                        String alterDbMngdLoc = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, managedLocation);
                                        dbMirror.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
                                        dbMirror.addIssue(Environment.RIGHT, HDPHIVE3_DB_LOCATION.getDesc());
                                        dbDefRight.put(DB_LOCATION, managedLocation);
                                    }
                                }

                                break;
                            case DUMP:
                                String createDb = MessageFormat.format(MirrorConf.CREATE_DB, database);
                                StringBuilder sb = new StringBuilder();
                                sb.append(createDb).append("\n");
                                if (dbDefLeft.get(MirrorConf.COMMENT) != null && !dbDefLeft.get(MirrorConf.COMMENT).trim().isEmpty()) {
                                    sb.append(MirrorConf.COMMENT).append(" \"").append(dbDefLeft.get(MirrorConf.COMMENT)).append("\"\n");
                                }
                                if (location != null) {
                                    sb.append(DB_LOCATION).append(" \"").append(location).append("\"\n");
                                }
                                if (managedLocation != null) {
                                    sb.append(MirrorConf.DB_MANAGED_LOCATION).append(" \"").append(managedLocation).append("\"\n");
                                }
                                // TODO: DB Properties.
                                dbMirror.getSql(Environment.LEFT).add(new Pair(MirrorConf.CREATE_DB_DESC, sb.toString()));
                                break;
                            case COMMON:
                                String createDbCom = MessageFormat.format(MirrorConf.CREATE_DB, database);
                                StringBuilder sbCom = new StringBuilder();
                                sbCom.append(createDbCom).append("\n");
                                if (dbDefLeft.get(MirrorConf.COMMENT) != null && !dbDefLeft.get(MirrorConf.COMMENT).trim().isEmpty()) {
                                    sbCom.append(MirrorConf.COMMENT).append(" \"").append(dbDefLeft.get(MirrorConf.COMMENT)).append("\"\n");
                                }
                                if (location != null) {
                                    sbCom.append(DB_LOCATION).append(" \"").append(location).append("\"\n");
                                }
                                if (managedLocation != null) {
                                    sbCom.append(MirrorConf.DB_MANAGED_LOCATION).append(" \"").append(managedLocation).append("\"\n");
                                }
                                // TODO: DB Properties.
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(MirrorConf.CREATE_DB_DESC, sbCom.toString()));
                                break;
                            case STORAGE_MIGRATION:
                                StringBuilder sbLoc = new StringBuilder();
                                sbLoc.append(hmsMirrorConfig.getTransfer().getCommonStorage());
                                sbLoc.append(hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory());
                                sbLoc.append("/");
                                sbLoc.append(database);
                                sbLoc.append(".db");
                                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                                    String alterDbLoc = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, sbLoc.toString());
                                    dbMirror.getSql(Environment.LEFT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDbLoc));
                                    dbDefRight.put(DB_LOCATION, sbLoc.toString());
                                }

                                StringBuilder sbMngdLoc = new StringBuilder();
                                sbMngdLoc.append(hmsMirrorConfig.getTransfer().getCommonStorage());
                                sbMngdLoc.append(hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory());
                                sbMngdLoc.append("/");
                                sbMngdLoc.append(database);
                                sbMngdLoc.append(".db");
                                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                                    String alterDbMngdLoc = MessageFormat.format(MirrorConf.ALTER_DB_MNGD_LOCATION, database, sbMngdLoc.toString());
                                    dbMirror.getSql(Environment.LEFT).add(new Pair(MirrorConf.ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
                                    dbDefRight.put(DB_MANAGED_LOCATION, sbMngdLoc.toString());
                                } else {
                                    String alterDbMngdLoc = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, sbMngdLoc.toString());
                                    dbMirror.getSql(Environment.LEFT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
                                    dbMirror.addIssue(Environment.LEFT, HDPHIVE3_DB_LOCATION.getDesc());
                                    dbDefRight.put(DB_LOCATION, sbMngdLoc.toString());
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
                if (hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() != null) {
                    // Set the location to the external directory for the database.
                    database = getHmsMirrorCfgService().getResolvedDB(dbMirror.getName());
                    location = hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace() + hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() +
                            "/" + database + ".db";
                    String alterDB_location = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, location);
                    dbMirror.getSql(Environment.LEFT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDB_location));
                    dbDefLeft.put(DB_LOCATION, location);
                }
            }
        } else {
            // Reset Right DB.
            database = getHmsMirrorCfgService().getResolvedDB(dbMirror.getName());
            String dropDb = MessageFormat.format(MirrorConf.DROP_DB, database);
            dbMirror.getSql(Environment.RIGHT).add(new Pair(MirrorConf.DROP_DB_DESC, dropDb));
        }
        return rtn;
    }

    public boolean createDatabases() {
        boolean rtn = true;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();
        for (String database : hmsMirrorConfig.getDatabases()) {
            DBMirror dbMirror = getConversion().getDatabase(database);
            rtn = buildDBStatements(dbMirror);

            if (rtn) {
                if (!runDatabaseSql(dbMirror, Environment.LEFT)) {
                    rtn = false;
                }
                if (!runDatabaseSql(dbMirror, Environment.RIGHT)) {
                    rtn = false;
                }
            }
        }
        return rtn;
    }

    public Boolean getDatabase(DBMirror dbMirror, Environment environment) throws SQLException {
        Boolean rtn = Boolean.FALSE;
        Connection conn = null;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        try {
            conn = connectionPoolService.getHS2EnvironmentConnection(environment);//getConnection();
            if (conn != null) {

                String database = (environment == Environment.LEFT ? dbMirror.getName() : getHmsMirrorCfgService().getResolvedDB(dbMirror.getName()));

                log.debug("{}:{}: Loading database definition.", environment, database);

                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    log.debug("{}:{}: Getting Database Definition", environment, database);
                    resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.DESCRIBE_DB, database));
                    //Retrieving the ResultSetMetaData object
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    //getting the column type
                    int column_count = rsmd.getColumnCount();
                    Map<String, String> dbDef = new TreeMap<>();
                    while (resultSet.next()) {
                        for (int i = 0; i < column_count; i++) {
                            String cName = rsmd.getColumnName(i + 1).toUpperCase(Locale.ROOT);
                            String cValue = resultSet.getString(i + 1);
                            // Don't add element if its empty.
                            if (cValue != null && !cValue.trim().isEmpty()) {
                                dbDef.put(cName, cValue);
                            }
                        }
                    }
                    dbMirror.setDBDefinition(environment, dbDef);
                    rtn = Boolean.TRUE;
                } catch (SQLException sql) {
                    // DB Doesn't Exists.
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
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Connection conn = null;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        Boolean rtn = Boolean.TRUE;
        // Skip when running test data.
        if (!hmsMirrorConfig.isLoadingTestData()) {
            try {
                conn = connectionPoolService.getHS2EnvironmentConnection(environment);

                if (conn == null && hmsMirrorConfig.isExecute() && !hmsMirrorConfig.getCluster(environment).getHiveServer2().isDisconnected()) {
                    // this is a problem.
                    rtn = Boolean.FALSE;
                    dbMirror.addIssue(environment, "Connection missing. This is a bug.");
                }

                if (conn == null && hmsMirrorConfig.getCluster(environment).getHiveServer2().isDisconnected()) {
                    dbMirror.addIssue(environment, "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                            "The scripts will need to be run 'manually'.");
                }

                if (conn != null) {
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

    @Autowired
    public void setHmsMirrorCfgService(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    @Autowired
    public void setConversion(Conversion conversion) {
        this.conversion = conversion;
    }

}
