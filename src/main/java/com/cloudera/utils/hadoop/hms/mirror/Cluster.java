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

import com.cloudera.utils.hadoop.HadoopSession;
import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.*;
import java.text.*;
import java.util.*;
import java.util.regex.Matcher;

import static com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum.DUMP;
import static com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum.STORAGE_MIGRATION;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hadoop.hms.mirror.TablePropertyVars.HMS_STORAGE_MIGRATION_FLAG;

public class Cluster implements Comparable<Cluster> {
    private static final Logger LOG = LogManager.getLogger(Cluster.class);
    private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @JsonIgnore
    private ConnectionPools pools = null;

    @JsonIgnore
    private Boolean initialized = Boolean.FALSE;
    @JsonIgnore
    private Config config = null;

    private Environment environment = null;
    private Boolean legacyHive = Boolean.TRUE;
    private Boolean createIfNotExists = Boolean.FALSE;

    /*
    HDP Hive 3 and Manage table creation and location methods weren't mature and had a lot of
    bugs/incomplete features.

    Hive 3 Databases have an MANAGEDLOCATION attribute used to override the 'warehouse' location
    specified in the hive metastore as the basis for the root directory of ACID tables in Hive.
    Unfortunately, this setting isn't available in HDP Hive 3.  It was added later in CDP Hive 3.

    In lew of this, when this flag is set to true (default is false), we will NOT strip the location
    element from the tables CREATE for ACID tables.  This is the only method we have to control the
    tables location.  THe DATABASE doesn't yet support the MANAGEDLOCATION attribute, so we will not
    build/run the ALTER DATABASE ...  SET MANAGEDLOCATION ... for this configuration.

    We will add a DB properties that can be used later on to set the DATABASE's MANAGEDLOCATION property
    after you've upgraded to CDP Hive 3.
     */
    private Boolean hdpHive3 = Boolean.FALSE;
    private String hcfsNamespace = null;
    private HiveServer2Config hiveServer2 = null;
    @JsonProperty(value = "metastore_direct")
    private DBStore metastoreDirect = null;
    private PartitionDiscovery partitionDiscovery = new PartitionDiscovery();
    private Boolean enableAutoTableStats = Boolean.FALSE;
    private Boolean enableAutoColumnStats = Boolean.FALSE;

    public Cluster() {
    }

    public Environment getEnvironment() {
        return environment;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public Boolean isInitialized() {
        return initialized;
    }

    public void setInitialized(Boolean initialized) {
        this.initialized = initialized;
    }

    public Config getConfig() {
        return config;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public Boolean getLegacyHive() {
        return legacyHive;
    }

    public void setLegacyHive(Boolean legacyHive) {
        this.legacyHive = legacyHive;
    }

    public Boolean isHdpHive3() {
        return hdpHive3;
    }

    public Boolean getHdpHive3() {
        return hdpHive3;
    }

    public void setHdpHive3(Boolean hdpHive3) {
        this.hdpHive3 = hdpHive3;
    }

    public Boolean getCreateIfNotExists() {
        return createIfNotExists;
    }

    public void setCreateIfNotExists(Boolean createIfNotExists) {
        this.createIfNotExists = createIfNotExists;
    }

    public Boolean getEnableAutoTableStats() {
        return enableAutoTableStats;
    }

    public void setEnableAutoTableStats(Boolean enableAutoTableStats) {
        this.enableAutoTableStats = enableAutoTableStats;
    }

    public Boolean getEnableAutoColumnStats() {
        return enableAutoColumnStats;
    }

    public void setEnableAutoColumnStats(Boolean enableAutoColumnStats) {
        this.enableAutoColumnStats = enableAutoColumnStats;
    }

    public String getHcfsNamespace() {
        return hcfsNamespace;
    }

    public void setHcfsNamespace(String hcfsNamespace) {
        this.hcfsNamespace = hcfsNamespace;
    }

    public HiveServer2Config getHiveServer2() {
        // Only create when using test data.  Otherwise, leave to expose config issue.
        if (hiveServer2 == null && Context.getInstance().getConfig().isLoadingTestData())
            hiveServer2 = new HiveServer2Config();
        return hiveServer2;
    }

    public void setHiveServer2(HiveServer2Config hiveServer2) {
        this.hiveServer2 = hiveServer2;
        this.initialized = Boolean.TRUE;
    }

    public DBStore getMetastoreDirect() {
        return metastoreDirect;
    }

    public void setMetastoreDirect(DBStore metastoreDirect) {
        this.metastoreDirect = metastoreDirect;
    }

    public PartitionDiscovery getPartitionDiscovery() {
        return partitionDiscovery;
    }

    public void setPartitionDiscovery(PartitionDiscovery partitionDiscovery) {
        this.partitionDiscovery = partitionDiscovery;
    }

    public ConnectionPools getPools() {
        return pools;
    }

    public void setPools(ConnectionPools pools) {
        this.pools = pools;
    }

    @JsonIgnore
    public Connection getConnection() throws SQLException {
        Connection conn = null;
        if (pools != null) {
            try {
                conn = pools.getHS2EnvironmentConnection(getEnvironment());
            } catch (RuntimeException rte) {
                config.getErrors().set(MessageCode.CONNECTION_ISSUE.getCode());
                throw rte;
            }
        }
        return conn;
    }

    @JsonIgnore
    public Connection getMetastoreDirectConnection() throws SQLException {
        Connection conn = null;
        if (pools != null) {
            try {
                conn = pools.getMetastoreDirectEnvironmentConnection(getEnvironment());
            } catch (RuntimeException rte) {
                config.getErrors().set(MessageCode.CONNECTION_ISSUE.getCode());
                throw rte;
            }
        }
        return conn;
    }

    public Boolean getDatabase(Config config, DBMirror dbMirror) throws SQLException {
        Boolean rtn = Boolean.FALSE;
        Connection conn = null;
        try {
            conn = getConnection();
            if (conn != null) {

                String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

                LOG.debug(getEnvironment() + ":" + database + ": Loading database definition.");

                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    LOG.debug(getEnvironment() + ":" + database + ": Getting Database Definition");
                    resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.DESCRIBE_DB, database));
                    //Retrieving the ResultSetMetaData object
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    //getting the column type
                    int column_count = rsmd.getColumnCount();
                    Map<String, String> dbDef = new TreeMap<String, String>();
                    while (resultSet.next()) {
                        for (int i = 0; i < column_count; i++) {
                            String cName = rsmd.getColumnName(i + 1).toUpperCase(Locale.ROOT);
                            String cValue = resultSet.getString(i + 1);
                            // Don't add element if its empty.
                            if (cValue != null && cValue.trim().length() > 0) {
                                dbDef.put(cName, cValue);
                            }
                        }
                    }
                    dbMirror.setDBDefinition(getEnvironment(), dbDef);
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
            se.printStackTrace();
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

    public void getTables(Config config, DBMirror dbMirror) throws SQLException {
        Connection conn = null;
        try {
            conn = getConnection();
            if (conn != null) {
                String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

                LOG.info(getEnvironment() + ":" + database + ": Loading tables for database");

                Statement stmt = null;
                ResultSet resultSet = null;
                // Stub out the tables
                try {
                    stmt = conn.createStatement();
                    LOG.debug(getEnvironment() + ":" + database + ": Setting Hive DB Session Context");
                    stmt.execute(MessageFormat.format(MirrorConf.USE, database));
                    LOG.info(getEnvironment() + ":" + database + ": Getting Table List");
                    List<String> shows = new ArrayList<String>();
                    if (!this.getLegacyHive()) {
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
                                LOG.info("Database: " + database + " Table: " + tableName + " was NOT added to list.  " +
                                        "The name matches the transfer prefix and is most likely a remnant of a previous " +
                                        "event. If this is a mistake, change the 'transferPrefix' to something more unique.");
                            } else if (tableName.endsWith("storage_migration")) {
                                LOG.info("Database: " + database + " Table: " + tableName + " was NOT added to list.  " +
                                        "The name is the result of a previous STORAGE_MIGRATION attempt that has not been " +
                                        "cleaned up.");
                            } else {
                                if (config.getFilter().getTblRegEx() == null && config.getFilter().getTblExcludeRegEx() == null) {
                                    TableMirror tableMirror = dbMirror.addTable(tableName);
                                    tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                } else if (config.getFilter().getTblRegEx() != null) {
                                    // Filter Tables
                                    assert (config.getFilter().getTblFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblFilterPattern().matcher(tableName);
                                    if (matcher.matches()) {
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    } else {
                                        LOG.info(database + ":" + tableName + " didn't match table regex filter and " +
                                                "will NOT be added to processing list.");
                                    }
                                } else if (config.getFilter().getTblExcludeRegEx() != null) {
                                    assert (config.getFilter().getTblExcludeFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblExcludeFilterPattern().matcher(tableName);
                                    if (!matcher.matches()) { // ANTI-MATCH
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    } else {
                                        LOG.info(database + ":" + tableName + " matched exclude table regex filter and " +
                                                "will NOT be added to processing list.");
                                    }
                                }
                            }
                        }

                    }
                } catch (SQLException se) {
                    LOG.error(getEnvironment() + ":" + database + " ", se);
                    // This is helpful if the user running the process doesn't have permissions.
                    dbMirror.addIssue(getEnvironment(), database + " " + se.getMessage());
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


    public void getTableDefinition(String database, TableMirror tableMirror) throws SQLException {
        // The connection should already be in the database;
        Config config = Context.getInstance().getConfig();
        Connection conn = null;
        try {
            conn = getConnection();
            EnvironmentTable et = tableMirror.getEnvironmentTable(getEnvironment());
            if (conn != null) {
                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    LOG.info(getEnvironment() + ":" + database + "." + tableMirror.getName() +
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
                                LOG.error(getEnvironment() + ":" + database + "." + tableMirror.getName() +
                                        ": Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. " +
                                        "ResultSet record(line) is null. Skipping.");
                            }
                        }
                    } else {
                        LOG.error(getEnvironment() + ":" + database + "." + tableMirror.getName() +
                                ": Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. No Metadata.");
                    }
                    et.setDefinition(tblDef);
                    et.setName(tableMirror.getName());
                    // Identify that the table existed in the Database before other activity.
                    et.setExists(Boolean.TRUE);
                    tableMirror.addStep(getEnvironment().toString(), "Fetched Schema");

                    if (this.environment == Environment.LEFT) {
                        if (config.getMigrateVIEW().isOn() && config.getDataStrategy() != DUMP) {
                            if (!TableUtils.isView(et)) {
                                tableMirror.setRemove(Boolean.TRUE);
                                tableMirror.setRemoveReason("VIEW's only processing selected.");
                            }
                        } else {
                            // Check if ACID for only the LEFT cluster.  If it's the RIGHT cluster, other steps will deal with
                            // the conflict.  IE: Rename or exists already.
                            if (TableUtils.isACID(et) && this.getEnvironment() == Environment.LEFT) {
                                // For ACID tables, check that Migrate is ON.
                                if (config.getMigrateACID().isOn()) {
                                    tableMirror.addStep("TRANSACTIONAL", Boolean.TRUE);
                                } else {
                                    tableMirror.setRemove(Boolean.TRUE);
                                    tableMirror.setRemoveReason("ACID table and ACID processing not selected (-ma|-mao).");
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
                                if (!config.getMigratedNonNative()) {
                                    tableMirror.setRemove(Boolean.TRUE);
                                    tableMirror.setRemoveReason("This is a Non-Native hive table and non-native process wasn't " +
                                            "selected.");
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
                        if (!tableMirror.isRemove()) {
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
                                        loadTableStats(et);
                                    }
                                    break;
                            }
                        }
                    }
                    // Check for table size filter
                    if (config.getFilter().getTblSizeLimit() != null) {
                        Long dataSize = (Long) et.getStatistics().get(DATA_SIZE);
                        if (dataSize != null) {
                            if (config.getFilter().getTblSizeLimit() * (1024 * 1024) < dataSize) {
                                tableMirror.setRemove(Boolean.TRUE);
                                tableMirror.setRemoveReason("The table dataset size exceeds the specified table filter size limit: " +
                                        config.getFilter().getTblSizeLimit() + "Mb < " + dataSize);
                            }
                        }
                    }

                    Boolean partitioned = TableUtils.isPartitioned(et);
                    if (partitioned && !tableMirror.isRemove()) {
                        /*
                        If we are -epl, we need to load the partition metadata for the table. And we need to use the
                        metastore_direct connection to do so. Trying to load this through the standard Hive SQL process
                        is 'extremely' slow.
                         */
                        if (config.getEvaluatePartitionLocation() ||
                                (config.getDataStrategy() == STORAGE_MIGRATION && config.getTransfer().getStorageMigration().isDistcp())) {
                            loadTablePartitionMetadataDirect(database, et);
                        } else {
                            loadTablePartitionMetadata(conn, database, et);
                        }
                        // Check for table partition count filter
                        if (config.getFilter().getTblPartitionLimit() != null) {
                            Integer partLimit = config.getFilter().getTblPartitionLimit();
                            if (et.getPartitions().size() > partLimit) {
                                tableMirror.setRemove(Boolean.TRUE);
                                tableMirror.setRemoveReason("The table partition count exceeds the specified table filter partition limit: " +
                                        config.getFilter().getTblPartitionLimit() + " < " + et.getPartitions().size());

                            }
                        }
                    }

                    if (config.getTransferOwnership()) {
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
                                        LOG.error("Couldn't parse 'owner' value from: " + resultSet.getString(1) +
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

                    LOG.debug(getEnvironment() + ":" + database + "." + et.getName() +
                            ": Loaded Table Definition");
                } catch (SQLException throwables) {
                    if (throwables.getMessage().contains("Table not found") || throwables.getMessage().contains("Database does not exist")) {
                        // This is ok in the upper cluster where we don't expect the table to exist if the process hadn't run before.
                        tableMirror.addStep(this.getEnvironment().toString(), "No Schema");
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
                }
            }
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    public Boolean runTableSql(TableMirror tblMirror) {
        return runTableSql(tblMirror, getEnvironment());
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
        // Skip this if using test data.
        if (!config.isLoadingTestData()) {

            try {
                // conn will be null if config.execute != true.
                conn = getConnection();

                if (conn == null && config.isExecute() && !this.getHiveServer2().isDisconnected()) {
                    // this is a problem.
                    rtn = Boolean.FALSE;
                    tblMirror.addIssue(getEnvironment(), "Connection missing. This is a bug.");
                }

                if (conn == null && this.getHiveServer2().isDisconnected()) {
                    tblMirror.addIssue(getEnvironment(), "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                            "The scripts will need to be run 'manually'.");
                }

                if (conn != null) {
                    Statement stmt = null;
                    try {
                        stmt = conn.createStatement();
                        for (Pair pair : sqlList) {
                            LOG.debug(getEnvironment() + ":SQL:" + pair.getDescription() + ":" + pair.getAction());
                            tblMirror.setMigrationStageMessage("Executing SQL: " + pair.getDescription());
                            if (config.isExecute())
                                stmt.execute(pair.getAction());
                            tblMirror.addStep(getEnvironment().toString(), "Sql Run Complete for: " + pair.getDescription());
                        }
                    } catch (SQLException throwables) {
                        LOG.error(throwables);
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
                LOG.error(throwables);
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

    public Boolean runClusterSql(List<Pair> clusterSql) {
        Boolean rtn = Boolean.TRUE;
        for (Pair pair : clusterSql) {
            if (!runDatabaseSql(null, pair)) {
                rtn = Boolean.FALSE;
                // don't continue
                break;
            }
        }
        return rtn;
    }


    public Boolean runDatabaseSql(DBMirror dbMirror) {
        List<Pair> dbPairs = dbMirror.getSql(environment);
        Boolean rtn = Boolean.TRUE;
        for (Pair pair : dbPairs) {
            if (!runDatabaseSql(dbMirror, pair)) {
                rtn = Boolean.FALSE;
                // don't continue
                break;
            }
        }
        return rtn;
    }

    public Boolean runDatabaseSql(DBMirror dbMirror, Pair dbSqlPair) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Connection conn = null;
        Boolean rtn = Boolean.TRUE;
        // Skip when running test data.
        if (!config.isLoadingTestData()) {
            try {
                conn = getConnection();

                if (conn == null && config.isExecute() && !this.getHiveServer2().isDisconnected()) {
                    // this is a problem.
                    rtn = Boolean.FALSE;
                    dbMirror.addIssue(getEnvironment(), "Connection missing. This is a bug.");
                }

                if (conn == null && this.getHiveServer2().isDisconnected()) {
                    dbMirror.addIssue(getEnvironment(), "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                            "The scripts will need to be run 'manually'.");
                }

                if (conn != null) {
                    if (dbMirror != null)
                        LOG.debug(getEnvironment() + " - " + dbSqlPair.getDescription() + ": " + dbMirror.getName());
                    else
                        LOG.debug(getEnvironment() + " - " + dbSqlPair.getDescription() + ":" + dbSqlPair.getAction());

                    Statement stmt = null;
                    try {
                        try {
                            stmt = conn.createStatement();
                        } catch (SQLException throwables) {
                            LOG.error("Issue building statement", throwables);
                            rtn = Boolean.FALSE;
                        }

                        try {
                            LOG.debug(getEnvironment() + ":" + dbSqlPair.getDescription() + ":" + dbSqlPair.getAction());
                            if (config.isExecute()) // on dry-run, without db, hard to get through the rest of the steps.
                                stmt.execute(dbSqlPair.getAction());
                        } catch (SQLException throwables) {
                            LOG.error(getEnvironment() + ":" + dbSqlPair.getDescription() + ":", throwables);
                            dbMirror.addIssue(getEnvironment(), throwables.getMessage() + " " + dbSqlPair.getDescription() +
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
                LOG.error("Issue", throwables);
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

    protected void loadTablePartitionMetadataDirect(String database, EnvironmentTable envTable) {
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
        try {
            conn = getMetastoreDirectConnection();
            LOG.debug(getEnvironment() + ":" + database + "." + envTable.getName() +
                    ": Loading Partitions from Metastore Direct Connection.");
            QueryDefinitions queryDefinitions = Context.getInstance().getQueryDefinitions(this.environment);
            if (queryDefinitions != null) {
                String partLocationQuery = queryDefinitions.getQueryDefinition("part_locations").getStatement();
                pstmt = conn.prepareStatement(partLocationQuery);
                pstmt.setString(1, database);
                pstmt.setString(2, envTable.getName());
                resultSet = pstmt.executeQuery();
                Map<String, String> partDef = new HashMap<String, String>();
                while (resultSet.next()) {
                    partDef.put(resultSet.getString(1), resultSet.getString(2));
                }
                envTable.setPartitions(partDef);
            }
            LOG.debug(getEnvironment() + ":" + database + "." + envTable.getName() +
                    ": Loaded Partitions from Metastore Direct Connection.");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    protected void loadTablePartitionMetadata(Connection conn, String database, EnvironmentTable envTable) throws SQLException {
        Statement stmt = null;
        ResultSet resultSet = null;
        Config config = Context.getInstance().getConfig();
        try {
            stmt = conn.createStatement();
            LOG.debug(getEnvironment() + ":" + database + "." + envTable.getName() +
                    ": Loading Partitions");

            resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_PARTITIONS, database, envTable.getName()));
            Map<String, String> partDef = new HashMap<String, String>();
            while (resultSet.next()) {
                partDef.put(resultSet.getString(1), NOT_SET);
            }
            envTable.setPartitions(partDef);

            // If they need to gather partition details, do it here.
            // This method is TOO SLOW.  Need to go directly the metastore DB.
//            if (config.getEvaluatePartitionLocation()) {
//                switch (config.getDataStrategy()) {
//                    case SCHEMA_ONLY:
//                    case DUMP:
//                        // Collect the partition details.
//                        // Loop through the partitions and get the details.
//                        Map<String, String> partDefWithLocation = new HashMap<String, String>();
//                        for (String partSpec : partDef.keySet()) {
//                            LOG.debug(getEnvironment() + ":" + database + "." + envTable.getName() +
//                                    ": Loading Partition Details: " + partSpec);
//                            resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_TABLE_EXTENDED_WITH_PARTITION, envTable.getName(), partSpec));
//                            String location = null;
//                            while (resultSet.next()) {
//                                String strDetail = resultSet.getString(1);
//                                if (strDetail.startsWith("location")) {
//                                    location = strDetail.substring("location:".length());
//                                    break;
//                                }
//                            }
//                            if (resultSet != null) {
//                                try {
//                                    resultSet.close();
//                                } catch (SQLException sqlException) {
//                                    // ignore
//                                }
//                            }
//                            if (location != null)
//                                partDefWithLocation.put(partSpec, location);
//                            else
//                                partDefWithLocation.put(partSpec, null);
//                        }
//
//                        LOG.debug(getEnvironment() + ":" + database + "." + envTable.getName() +
//                                ": Loading Partition Details: Complete(" + partDefWithLocation.size() + ")");
//
//                        // Replace the partition details with the location.
//                        envTable.setPartitions(partDefWithLocation);
//                        break;
//                    default:
//                        break;
//                }
//            }

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

    protected void loadTableStats(EnvironmentTable envTable) throws SQLException {
        // Considered only gathering stats for partitioned tables, but decided to gather for all tables to support
        //  smallfiles across the board.
        if (Context.getInstance().getConfig().getOptimization().getSkipStatsCollection()) {
            LOG.debug(getEnvironment() + ":" + envTable.getName() + ": Skipping Stats Collection.");
            return;
        }
        switch (Context.getInstance().getConfig().getDataStrategy()) {
            case DUMP:
            case SCHEMA_ONLY:
                // We don't need stats for these.
                return;
            case STORAGE_MIGRATION:
                if (Context.getInstance().getConfig().getTransfer().getStorageMigration().isDistcp()) {
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
        String location = TableUtils.getLocation(envTable.getName(), envTable.getDefinition());
        // Only run checks against hdfs and ozone namespaces.
        String[] locationParts = location.split(":");
        String protocol = locationParts[0];
        if (Context.getInstance().getSupportFileSystems().contains(protocol)) {
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
                        envTable.getStatistics().put(DIR_COUNT, Integer.valueOf(countRecord.get(0).toString()));
                        envTable.getStatistics().put(FILE_COUNT, Integer.valueOf(countRecord.get(1).toString()));
                        envTable.getStatistics().put(DATA_SIZE, Long.valueOf(countRecord.get(2).toString()));
                        envTable.getStatistics().put(AVG_FILE_SIZE, avgFileSize);
                        envTable.getStatistics().put(TABLE_EMPTY, Boolean.FALSE);
                    } catch (ArithmeticException ae) {
                        // Directory is probably empty.
                        envTable.getStatistics().put(TABLE_EMPTY, Boolean.TRUE);
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
        TableUtils.getSerdeType(envTable);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Cluster cluster = (Cluster) o;

        if (!legacyHive.equals(cluster.legacyHive)) return false;
        if (!hcfsNamespace.equals(cluster.hcfsNamespace)) return false;
        return hiveServer2.equals(cluster.hiveServer2);
    }

    @Override
    public int hashCode() {
        int result = legacyHive.hashCode();
        result = 31 * result + hcfsNamespace.hashCode();
        result = 31 * result + hiveServer2.hashCode();
        return result;
    }

    @Override
    public int compareTo(Cluster o) {
        return 0;
    }
}
