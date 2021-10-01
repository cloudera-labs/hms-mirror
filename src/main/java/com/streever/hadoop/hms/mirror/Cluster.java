package com.streever.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.*;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;

public class Cluster implements Comparable<Cluster> {
    private static Logger LOG = LogManager.getLogger(Cluster.class);
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @JsonIgnore
    private ConnectionPools pools = null;

    private Environment environment = null;
    private Boolean legacyHive = Boolean.TRUE;
    private String hcfsNamespace = null;
    private HiveServer2Config hiveServer2 = null;
    private PartitionDiscovery partitionDiscovery = new PartitionDiscovery();

    public Cluster() {
    }

    public Environment getEnvironment() {
        return environment;
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

    public String getHcfsNamespace() {
        return hcfsNamespace;
    }

    public void setHcfsNamespace(String hcfsNamespace) {
        this.hcfsNamespace = hcfsNamespace;
    }

    public HiveServer2Config getHiveServer2() {
        if (hiveServer2 == null)
            hiveServer2 = new HiveServer2Config();
        return hiveServer2;
    }

    public void setHiveServer2(HiveServer2Config hiveServer2) {
        this.hiveServer2 = hiveServer2;
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
        if (pools != null)
            conn = pools.getEnvironmentConnection(getEnvironment());
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

                LOG.debug(getEnvironment() + ":" + database + ": Loading tables for database");

                Statement stmt = null;
                ResultSet resultSet = null;
                // Stub out the tables
                try {
                    stmt = conn.createStatement();
                    LOG.debug(getEnvironment() + ":" + database + ": Setting Hive DB Session Context");
                    stmt.execute(MessageFormat.format(MirrorConf.USE, database));
                    LOG.debug(getEnvironment() + ":" + database + ": Getting Table List");
                    List<String> shows = new ArrayList<String>();
                    if (!this.getLegacyHive()) {
                        if (config.getMigrateVIEW().isOn()) {
                            shows.add(MirrorConf.SHOW_VIEWS);
                            if (config.getDataStrategy() == DataStrategy.DUMP) {
                                shows.add(MirrorConf.SHOW_TABLES);
                            }
                        }
                    } else {
                        shows.add(MirrorConf.SHOW_TABLES);
                    }
                    for (String show: shows) {
                        resultSet = stmt.executeQuery(show);
                        while (resultSet.next()) {
                            String tableName = resultSet.getString(1);
                            if (tableName.startsWith(config.getTransfer().getTransferPrefix())) {
                                LOG.info("Database: " + database + " Table: " + tableName + " was NOT added to list.  " +
                                        "The name matches the transfer prefix and is most likely a remnant of a previous " +
                                        "event. If this is a mistake, change the 'transferPrefix' to something more unique.");
                            } else {
                                if (config.getTblRegEx() == null) {
                                    TableMirror tableMirror = dbMirror.addTable(tableName);
                                    tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                } else {
                                    // Filter Tables
                                    assert (config.getTblFilterPattern() != null);
                                    Matcher matcher = config.getTblFilterPattern().matcher(tableName);
                                    if (matcher.matches()) {
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    }
                                }
                            }
                        }

                    }
                } catch (SQLException se) {
                    LOG.error(getEnvironment() + ":" + database + " ", se);
                    // This is helpful if the user running the process doesn't have permissions.
                    dbMirror.addIssue(getEnvironment() + ":" + database + " " + se.getMessage());
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

    public void getTableDefinition(Config config, String database, TableMirror tableMirror) throws SQLException {
        // The connection should already be in the database;
        Connection conn = null;
        try {
            conn = getConnection();
            EnvironmentTable et = tableMirror.getEnvironmentTable(getEnvironment());
            if (conn != null) {
                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    LOG.debug(getEnvironment() + ":" + database + "." + tableMirror.getName() +
                            ": Loading Table Definition");
                    String useStatement = MessageFormat.format(MirrorConf.USE, database);
                    stmt.execute(useStatement);
                    String showStatement = MessageFormat.format(MirrorConf.SHOW_CREATE_TABLE, tableMirror.getName());
                    resultSet = stmt.executeQuery(showStatement);
                    List<String> tblDef = new ArrayList<String>();
                    while (resultSet.next()) {
                        tblDef.add(resultSet.getString(1));
                    }

                    et.setDefinition(tblDef);
                    et.setName(tableMirror.getName());
                    // Identify that the table existed in the Database before other activity.
                    et.setExists(Boolean.TRUE);
                    tableMirror.addStep(getEnvironment().toString(), "Fetched Schema");

                    if (config.getMigrateVIEW().isOn() && config.getDataStrategy() != DataStrategy.DUMP) {
                        if (!TableUtils.isView(et)) {
                            tableMirror.setRemove(Boolean.TRUE);
                            tableMirror.setRemoveReason("VIEW's only processing selected.");
                        }
                    } else {
                        if (TableUtils.isACID(et)) {
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
                            // When processing Tables, remove views.
                            if (TableUtils.isView(et) && config.getDataStrategy() != DataStrategy.DUMP) {
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


                    Boolean partitioned = TableUtils.isPartitioned(et.getName(), et.getDefinition());
                    if (partitioned) {
                        loadTablePartitionMetadata(database, et);
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

    public Boolean runSql(TableMirror tblMirror) {
        return runSql(tblMirror, getEnvironment());
    }

    /**
     * From this cluster, run the SQL built up in the tblMirror(environment)
     *
     * @param tblMirror
     * @param environment Allows to override cluster environment
     * @return
     */
    public Boolean runSql(TableMirror tblMirror, Environment environment) {
        Connection conn = null;
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable et = tblMirror.getEnvironmentTable(environment);

        rtn = runSql(et.getSql(), tblMirror, environment);

        return rtn;
    }

    public Boolean runSql(List<Pair> sqlList, TableMirror tblMirror, Environment environment) {
        Connection conn = null;
        Boolean rtn = Boolean.FALSE;

        try {
            conn = getConnection();

            if (conn != null) {
                Statement stmt = null;
                try {
                    stmt = conn.createStatement();
                    for (Pair pair : sqlList) {
                        LOG.info(getEnvironment() + ":SQL:" + pair.getDescription() + ":" + pair.getAction());
                        tblMirror.setMigrationStageMessage("Executing SQL: " + pair.getDescription());
                        stmt.execute(pair.getAction());
                        tblMirror.addStep(getEnvironment().toString(), "Sql Run Complete for: " + pair.getDescription());
                    }
                    rtn = Boolean.TRUE;
                } catch (SQLException throwables) {
                    LOG.error(throwables);
                    String message = throwables.getMessage();
                    if (throwables.getMessage().contains("HiveAccessControlException Permission denied")) {
                        message = message + " See [Hive SQL Exception / HDFS Permissions Issues](https://github.com/dstreev/hms-mirror#hive-sql-exception--hdfs-permissions-issues)";
                    }
                    if (throwables.getMessage().contains("AvroSerdeException")) {
                        message = message + ". It's possible the `avro.schema.url` referenced file doesn't exist at the target. " +
                                "Use the `-asm` option and hms-mirror will attempt to copy it to the new cluster.";
                    }
                    tblMirror.getEnvironmentTable(environment).addIssue(message);
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

    public void createDatabase(Config config, String database, String createDBSql) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Connection conn = null;
        try {
            conn = getConnection();
            if (conn != null) {

                LOG.debug(getEnvironment() + " - Create Database: " + database);

                Statement stmt = null;
                try {
                    try {
                        stmt = conn.createStatement();
                    } catch (SQLException throwables) {
                        LOG.error("Issue building statement", throwables);
                    }

                    try {
                        LOG.debug(getEnvironment() + ":CREATE_DB:" + createDBSql);
                        if (config.isExecute()) // on dry-run, without db, hard to get through the rest of the steps.
                            stmt.execute(createDBSql);
                    } catch (SQLException throwables) {
                        LOG.error(getEnvironment() + ":Creating DB", throwables);
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

    public void createDatabase(Config config, String database) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        String createDb = MessageFormat.format(MirrorConf.CREATE_DB, database);
        createDatabase(config, database, createDb);
    }

//    protected Boolean checkAndDoOverwrite(Statement stmt, Config config, DBMirror dbMirror, EnvironmentTable source, EnTableMirror tblMirror) {
//        return checkAndDoOverwrite(stmt, config, dbMirror, tblMirror, null);
//    }

    //    protected Boolean checkAndDoOverwrite(Statement stmt, Config config, DBMirror dbMirror, TableMirror tblMirror, String tblPrefix) {
//    protected Boolean checkAndDoOverwrite(Statement stmt, Config config, DBMirror dbMirror, EnvironmentTable source, EnvironmentTable target) {
//        Boolean rtn = Boolean.TRUE;
//
//        String database = dbMirror.getName();
//        tblMirror.setMigrationStageMessage("Checking whether the overwrite");
//        String tableName = tblPrefix == null ? tblMirror.getName() : tblPrefix + tblMirror.getName();
//        try {
//            // Run check for non-prefix (transfer) tables.
//            if (tblPrefix == null) {
//                // Check if we should overwrite existing table.
//                if (tblMirror.getTableDefinition(Environment.RIGHT) != null) {
//                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Exists in RIGHT cluster. ");
//                    tblMirror.addIssue("Schema exists already");
////                    tblMirror.addIssue("No Schema action performed");
//                    // Compare Schemas
//                    if (config.isSync()) {
//                        // ? Check for ACID table ?
//                        tblMirror.addIssue("Schema has changed and 'sync' is set. Will drop table so it can be recreated.");
//                        if (config.isReadOnly()) {
//                            // Alter target table and unset external.table.purge property.  May fail if not set, which is ok.
//                            String removePurgePropSql = MessageFormat.format(MirrorConf.REMOVE_TBL_PROP, config.getResolvedDB(database), tableName, MirrorConf.EXTERNAL_TABLE_PURGE);
//                            LOG.debug(getEnvironment() + ":" + removePurgePropSql);
//                            try {
//                                if (stmt != null && config.isExecute()) {
//                                    stmt.execute(removePurgePropSql);
//                                }
//                                tblMirror.addSql(removePurgePropSql);
//                                tblMirror.addIssue(getEnvironment() + ":Removed " + MirrorConf.EXTERNAL_TABLE_PURGE + " property from " + config.getResolvedDB(database) + "." + tableName);
//                            } catch (SQLException sqle) {
//                                LOG.debug(getEnvironment() + ":" + removePurgePropSql + " failed, reason: " + sqle.getMessage());
//                                tblMirror.addIssue(getEnvironment() + ":Failed to Removed " + MirrorConf.EXTERNAL_TABLE_PURGE + " property from " +
//                                        config.getResolvedDB(database) + "." + tableName + ". It probably wasn't set.");
//                            }
//                        }
//                        // Drop table.
//                        String dropTableSql = MessageFormat.format(MirrorConf.DROP_TABLE, config.getResolvedDB(database), tableName);
//                        LOG.debug(getEnvironment() + ":" + dropTableSql);
//                        try {
//                            if (stmt != null && config.isExecute()) {
//                                stmt.execute(dropTableSql);
//                            }
//                            tblMirror.addSql(dropTableSql);
//                            tblMirror.addIssue(getEnvironment() + ": Dropped table " + config.getResolvedDB(database) + "." + tableName);
//                        } catch (SQLException sqle) {
//                            LOG.debug(getEnvironment() + ":" + dropTableSql + " failed, reason: " + sqle.getMessage());
//                        }
//                    } else {
//                        tblMirror.addIssue("SCHEMA HAS CHANGED");
//                        tblMirror.addIssue("DROP before running again to MIGRATE FRESH SCHEMA.");
//                        tblMirror.addIssue("If you are dropping table, understand the impact on the underlying data (is it attached).");
//                        tblMirror.addIssue("No Schema action performed");
//                        rtn = Boolean.FALSE;
//                    }
//                }
//            } else {
//                // Always drop the transfer table.
//                if (rtn) {
//                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Dropping table");
//                    // It was created by HMS Mirror, safe to drop.
//                    String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, database, tableName);
//                    LOG.debug(getEnvironment() + ":(SQL)" + dropTable);
//                    tblMirror.setMigrationStageMessage("Dropping table (overwrite)");
//
//                    tblMirror.addSql(dropTable);
//                    if (stmt != null && config.isExecute()) {
//                        stmt.execute(dropTable);
//                    } else {
//                        tblMirror.addIssue("DRY-RUN: Table NOT dropped");
//                    }
//                    // TODO: change current to address tablename so we pickup transfer.
//                    tblMirror.setMigrationStageMessage("Table dropped");
//                    tblMirror.addAction(getEnvironment().toString(), "DROPPED Transfer (if existed)");
//                }
//            }
//        } catch (SQLException throwables) {
//            tblMirror.addIssue(this.getEnvironment() + ":" + tableName + "->" + throwables.getMessage());
//        }
//        return rtn;
//    }

    protected void loadTablePartitionMetadata(String database, EnvironmentTable envTable) throws SQLException {
        Connection conn = null;
//        EnvironmentTable et = tableMirror.getEnvironmentTable(getEnvironment());

        try {
            conn = getConnection();
            if (conn != null) {

                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    LOG.debug(getEnvironment() + ":" + database + "." + envTable.getName() +
                            ": Loading Partitions");

                    resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_PARTITIONS, database, envTable.getName()));
                    List<String> partDef = new ArrayList<String>();
                    while (resultSet.next()) {
                        partDef.add(resultSet.getString(1));
                    }
                    envTable.setPartitions(partDef);
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

//    public Boolean exportSchema(Config config, String database, DBMirror dbMirror, TableMirror tblMirror, String
//            exportBaseDirPrefix) {
//        Connection conn = null;
//        Statement stmt = null;
//        Boolean rtn = Boolean.FALSE;
//        LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
//        try {
//            conn = getConnection();
//            if (conn != null) {
//
//                String tableName = tblMirror.getName();
//
//                stmt = conn.createStatement();
//
//                // Get the definition for this environment.
//                List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
//                LOG.debug(getEnvironment() + ":" + database + "." +
//                        tableName + ": Exporting Table to " + config.getCluster(Environment.LEFT).hcfsNamespace + exportBaseDirPrefix + database + "/" + tableName);
//                String exportTransferSchema = MessageFormat.format(MirrorConf.EXPORT_TABLE,
//                        database, tableName,
//                        config.getCluster(Environment.LEFT).hcfsNamespace + exportBaseDirPrefix + database + "/" + tableName);
//                LOG.debug(getEnvironment() + ":(SQL)" + exportTransferSchema);
//                tblMirror.setMigrationStageMessage("Export Schema");
//
//                tblMirror.addSql(exportTransferSchema);
//                if (config.isExecute()) {
//                    stmt.execute(exportTransferSchema);
//                } else {
//                    tblMirror.addIssue("DRY-RUN: EXPORT table not run");
//                }
//                tblMirror.setMigrationStageMessage("Schema export complete");
//                tblMirror.addAction("EXPORT", "CREATED");
//
//                rtn = Boolean.TRUE;
//            }
//        } catch (SQLException throwables) {
//            tblMirror.addIssue(throwables.getMessage());
//        } finally {
//            try {
//                if (stmt != null)
//                    stmt.close();
//            } catch (SQLException sqlException) {
//                // ignore
//            }
//            try {
//                if (conn != null)
//                    conn.close();
//            } catch (SQLException throwables) {
//                //
//            }
//            LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
//        }
//        return rtn;
//    }

    /*
    In the same target db, build a transfer table from the lower cluster.  This table will be the table
    where we push all data to in the SQL method.  The data will be source by the RIGHT clusters shadow
    table (with original name) in the RIGHT cluster, looking at the data in the lower cluster.
    This transfer table should use the RIGHT cluster for storage and the location is the same (relative) as
    where the data was in the LEFT cluster.
    Once the transfer is complete, the two tables can be swapped in a move that is quick.
    */
//    public Boolean createUpperTransferTable(Config config, DBMirror dbMirror, TableMirror tblMirror, String
//            transferPrefix) {
//        // Open the connection and ensure we are running this on the "RIGHT" cluster.
//        Boolean rtn = Boolean.FALSE;
//        if (this.getEnvironment() == Environment.RIGHT) {
//
//            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));
//
//            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
//            Connection conn = null;
//            Statement stmt = null;
//            ResultSet resultSet = null;
//            try {
//                conn = getConnection();
//                if (conn != null) {
//
//                    String tableName = tblMirror.getName();
//
//                    stmt = conn.createStatement();
//
//                    // Set db context;
//                    String useDb = MessageFormat.format(MirrorConf.USE, database);
//
//                    LOG.debug(useDb);
//
//                    tblMirror.addSql(useDb);
//                    if (config.isExecute())
//                        stmt.execute(useDb);
//
//                    if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror, transferPrefix)) {
//                        Boolean takeOwnership = Boolean.TRUE;
//                        // NOT taking ownership for a converted table isn't supported with the SQL
//                        // DataStrategy.  So this isn't needed.
//                        //                    if (config.isReadOnly()) {
//                        //                        takeOwnership = Boolean.FALSE;
//                        //                    }
//                        Boolean buildUpper = tblMirror.buildUpperSchema(config, takeOwnership);
//
//                        // Don't change namespace when using Shared Storage.
//                        String leftLocation = TableUtils.getLocation(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT));
//                        String rightLocation = config.getTranslator().translateTableLocation(dbMirror.getName(), tblMirror.getName(), leftLocation, config);
//                        if (!TableUtils.updateTableLocation(tblMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT), rightLocation)) {
//                            // throw error!!
//                        }
//
////                    if (config.getDataStrategy() != DataStrategy.COMMON) {
////                        TableUtils.changeLocationNamespace(tblMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT),
////                                config.getCluster(Environment.LEFT).getHcfsNamespace(),
////                                config.getCluster(Environment.RIGHT).getHcfsNamespace());
////                    }
//
//                        if (TableUtils.isHMSLegacyManaged(tableName, tblMirror.getTableDefinition(Environment.RIGHT))) {
//                            tblMirror.setMigrationStageMessage("Adding EXTERNAL purge to table properties");
//                            TableUtils.upsertTblProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true", tblMirror.getTableDefinition(Environment.RIGHT));
//                        }
//
//                        // Get the Create State for this environment.
//                        // This create is the transfer table
//                        String createTable = tblMirror.getCreateStatement(this.getEnvironment(), transferPrefix);
//                        LOG.debug(getEnvironment() + ":(SQL)" + createTable);
//
//                        // The location should NOT have data in it. If it does, this could cause an issue.
//                        // TODO: validate target data location.
//                        // The transfer table should be an empty shell and the data directory, which points to the permanent
//                        //  location for the target table, should be empty.  If NOT, what do we do?
//                        tblMirror.setMigrationStageMessage("Creating table");
//                        tblMirror.addSql(createTable);
//                        if (config.isExecute()) {
//                            stmt.execute(createTable);
//                        } else {
//                            tblMirror.addIssue("DRY-RUN: Table NOT created");
//                        }
//                        tblMirror.setMigrationStageMessage("Table created");
//
//                        tblMirror.addAction("Transfer Schema Created", Boolean.TRUE);
//
//                        rtn = Boolean.TRUE;
//                    }
//                }
//            } catch (SQLException throwables) {
//                LOG.error("Issue", throwables);
//                tblMirror.addIssue(throwables.getMessage());
//            } catch (RuntimeException throwables) {
//                LOG.error("Issue", throwables);
//                tblMirror.addIssue(throwables.getMessage());
//            } finally {
//                try {
//                    if (resultSet != null)
//                        resultSet.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (stmt != null)
//                        stmt.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (conn != null)
//                        conn.close();
//                } catch (SQLException throwables) {
//                    //
//                }
//                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
//            }
//        }
//        return rtn;
//    }

//    public Boolean populateTransferTable(Config config, DBMirror dbMirror, TableMirror tblMirror) {
//        Boolean rtn = Boolean.FALSE;
//
//        // Create Schema in LEFT Cluster.
//        if (this.getEnvironment() == Environment.LEFT) {
//            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));
//
//            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
//            Connection conn = null;
//            Statement stmt = null;
//            ResultSet resultSet = null;
//            try {
//                conn = getConnection();
//
//                String tableName = tblMirror.getName();
//
//                LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Populating Transfer table");
//
//                // Set db context;
//                String useDb = MessageFormat.format(MirrorConf.USE, database);
//
//                LOG.debug(useDb);
//                tblMirror.addSql(useDb);
//
//                if (conn != null)
//                    stmt = conn.createStatement();
//
//                if (conn != null && config.isExecute())
//                    stmt.execute(useDb);
//
//                String transferSql = tblMirror.getCreateStatement(Environment.TRANSFER);
//
//                LOG.debug(getEnvironment() + ":(SQL)" + transferTable);
//                tblMirror.setMigrationStageMessage("Creating Transfer Table in LEFT cluster");
//                tblMirror.addSql(transferTable);
//                if (stmt != null && config.isExecute()) {
//                    stmt.execute(transferTable);
//                }
//                tblMirror.addAction("Transfer Schema Created", Boolean.TRUE);
//                tblMirror.setMigrationStageMessage("Transfer Table created in LEFT cluster");
//                rtn = Boolean.TRUE;
//
//            } catch (SQLException throwables) {
//                LOG.error("Issue", throwables);
//                tblMirror.addIssue(throwables.getMessage());
//            } catch (RuntimeException throwables) {
//                LOG.error("Issue", throwables);
//                tblMirror.addIssue(throwables.getMessage());
//            } finally {
//                try {
//                    if (resultSet != null)
//                        resultSet.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (stmt != null)
//                        stmt.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (conn != null)
//                        conn.close();
//                } catch (SQLException throwables) {
//                    //
//                }
//                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
//
//            }
//        } else {
//            // BAD
//            rtn = Boolean.FALSE;
//        }
//
//        return rtn;
//    }

//    private Boolean runCreateTable(Config config, Environment environment, TableMirror tblMirror, String database) {
//        Boolean rtn = Boolean.FALSE;
//
//        LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
//        Connection conn = null;
//        Statement stmt = null;
//        ResultSet resultSet = null;
//        try {
//            conn = getConnection();
//
//            String tableName = tblMirror.getName();
//
//            LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Build Transfer Schema");
//
//            // Set db context;
//            String useDb = MessageFormat.format(MirrorConf.USE, database);
//
//            LOG.debug(useDb);
//            tblMirror.addSql(useDb);
//
//            if (conn != null)
//                stmt = conn.createStatement();
//
//            if (conn != null && config.isExecute())
//                stmt.execute(useDb);
//
//            String createTable = tblMirror.getCreateStatement(environment);
//
//            LOG.debug(getEnvironment() + ":(SQL)" + createTable);
//            tblMirror.setMigrationStageMessage("Creating Table in " + getEnvironment() + " cluster");
//            tblMirror.addSql(createTable);
//
//            if (stmt != null && config.isExecute()) {
//                stmt.execute(createTable);
//            }
//            tblMirror.addAction("Schema Created", Boolean.TRUE);
//            tblMirror.setMigrationStageMessage("Table created in " + getEnvironment() + " cluster");
//            rtn = Boolean.TRUE;
//
//        } catch (SQLException throwables) {
//            LOG.error("Issue", throwables);
//            tblMirror.addIssue(throwables.getMessage());
//        } catch (RuntimeException throwables) {
//            LOG.error("Issue", throwables);
//            tblMirror.addIssue(throwables.getMessage());
//        } finally {
//            try {
//                if (resultSet != null)
//                    resultSet.close();
//            } catch (SQLException sqlException) {
//                // ignore
//            }
//            try {
//                if (stmt != null)
//                    stmt.close();
//            } catch (SQLException sqlException) {
//                // ignore
//            }
//            try {
//                if (conn != null)
//                    conn.close();
//            } catch (SQLException throwables) {
//                //
//            }
//            // TODO: The tblMirror.getName() isn't accurate for transfer and shadow tables.
//            LOG.debug(getEnvironment().toString() + ":END:" + database + "." + tblMirror.getName());
//        }
//        return rtn;
//    }

    /*
    Build a parallel table.  This table is NON-ACID and matches the structure of the original.

     */
//    public Boolean createTransferSchema(Config config, DBMirror dbMirror, TableMirror tblMirror) {
//        Boolean rtn = Boolean.FALSE;
//
//        // Build the schema def for the transfer
//        CopySpec transferSpec = CopySpec.transferSpec(config);
//
////        tblMirror.buildTableSchema(transferSpec);
//
//        // Create Schema in LEFT Cluster.
//        if (this.getEnvironment() == Environment.LEFT) {
//            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));
//
//            rtn = runCreateTable(config, transferSpec.getTarget(), tblMirror, database);
//
//        } else {
//            // BAD
//            rtn = Boolean.FALSE;
//        }
//        return rtn;
//    }

    /*
    Create the shadow schema in the RIGHT cluster that uses the 'transfer' schema's data location on the LEFT cluster.

    This will be the source of data being moved into the final table on the RIGHT cluster.

     */
//    public Boolean createShadowSchema(Config config, DBMirror dbMirror, TableMirror tblMirror) {
//        Boolean rtn = Boolean.FALSE;
//
//        // Build the schema def for the transfer
//        CopySpec shadowSpec = CopySpec.shadowSpec(config, Environment.TRANSFER, Environment.RIGHT);
//
////        tblMirror.buildTableSchema(shadowSpec);
//
//        rtn = runCreateTable(config, shadowSpec.getTarget(), tblMirror, dbMirror.getName());
//
//        return rtn;
//    }

//    public Boolean createRightSchema(Config config, DBMirror dbMirror, TableMirror tblMirror) {
//        // Open the connection and ensure we are running this on the "RIGHT" cluster.
//        Boolean rtn = Boolean.FALSE;
//        if (this.getEnvironment() == Environment.RIGHT) {
//            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));
//
//            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
//            Connection conn = null;
//            Statement stmt = null;
//            ResultSet resultSet = null;
//            try {
//                conn = getConnection();
//
//                String tableName = tblMirror.getName();
//
//                LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Build Upper Schema");
//
//                // Set db context;
//                String useDb = MessageFormat.format(MirrorConf.USE, database);
//
//                LOG.debug(useDb);
//                tblMirror.addSql(useDb);
//
//                if (conn != null)
//                    stmt = conn.createStatement();
//
//                if (conn != null && config.isExecute())
//                    stmt.execute(useDb);
//
//                // TODO: here. need to figure out what comparisons are needed (if any)
//                //       for non schema-only transfers.
//
//                if (!tblMirror.schemasEqual(Environment.LEFT, Environment.RIGHT) &&
//                        checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {
//
//                    boolean readonly = (config.isReadOnly() && config.getDataStrategy() == DataStrategy.SCHEMA_ONLY) |
//                            config.getDataStrategy() == DataStrategy.LINKED |
//                            config.getDataStrategy() == DataStrategy.COMMON |
//                            config.getDataStrategy() == DataStrategy.SQL |
//                            config.getDataStrategy() == DataStrategy.HYBRID; // The SQL initial schema is a transition table and should be read-only.
//                    Boolean buildUpper = tblMirror.buildUpperSchema(config, !readonly);
//
//                    String leftLocation = TableUtils.getLocation(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT));
//                    String rightLocation = config.getTranslator().translateTableLocation(dbMirror.getName(), tblMirror.getName(), leftLocation, config);
//
//                    // Adjust the Location to be Relative to the RIGHT cluster.
//                    // as long as we're not using shared storage or linked
//                    if (config.getDataStrategy() != DataStrategy.SQL &&
//                            config.getDataStrategy() != DataStrategy.HYBRID) {
////                        String leftLocation = TableUtils.getLocation(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT));
////                        String rightLocation = config.getTranslator().translateTableLocation(dbMirror.getName(), tblMirror.getName(), leftLocation, config);
//
//                        if (!TableUtils.updateTableLocation(tblMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT), rightLocation)) {
//                            // throw error!!
//                        }
//                    }
////                    if (config.getDataStrategy() != DataStrategy.LINKED &&
////                            config.getDataStrategy() != DataStrategy.COMMON &&
////                            config.getDataStrategy() != DataStrategy.SQL &&
////                            config.getDataStrategy() != DataStrategy.HYBRID) {
////                        TableUtils.changeLocationNamespace(dbMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT),
////                                config.getCluster(Environment.LEFT).getHcfsNamespace(),
////                                config.getCluster(Environment.RIGHT).getHcfsNamespace());
////                    }
//
//                    // Get the Create State for this environment.
//                    // For READ-ONLY
//                    boolean ok2go = Boolean.TRUE;
//                    // Check for existing directories to ensure we aren't corrupting the FS for READ-ONLY.
//                    // Don't bother check FS for LINKED and COMMON
//                    if (config.isReadOnly() &&
//                            !(config.getDataStrategy() == DataStrategy.LINKED || config.getDataStrategy() == DataStrategy.COMMON)) {
//                        String tableLocation = TableUtils.getLocation(tblMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT));
//                        LOG.debug("Config set to 'read-only'.  Validating FS before continuing for table: " + tblMirror.getName() +
//                                " at location " + tableLocation);
//                        HadoopSession main = null;
//                        if (conn != null) {
//                            try {
//                                main = config.getCliPool().borrow();
//                                String[] api = {"-api"};
//                                try {
//                                    main.start(api);
//                                } catch (Exception e) {
//                                    e.printStackTrace();
//                                }
//
//                                CommandReturn cr = main.processInput("connect");
//                                cr = main.processInput("test -d " + tableLocation);
//                                if (cr.isError()) {
//                                    // Dir doesn't exist.  Do NOT proceed.
//                                    String message = "FS location **'" + tableLocation + "'** doesn't exist.  With 'read-only' directive " +
//                                            "we can't create table **'" + tblMirror.getName() + "'** without altering FS";
//                                    tblMirror.addIssue(Environment.RIGHT, message);
//                                    dbMirror.addIssue(message);
//                                    rtn = Boolean.FALSE;
//                                    ok2go = Boolean.FALSE;
//                                }
//                            } finally {
//                                config.getCliPool().returnSession(main);
//                            }
//                        }
//                    }
//                    if (ok2go) {
//                        //
//                        String createTable = tblMirror.getCreateStatement(this.getEnvironment());
//                        LOG.debug(getEnvironment() + ":(SQL)" + createTable);
//                        tblMirror.setMigrationStageMessage("Creating Table in RIGHT cluster");
//                        tblMirror.addSql(createTable);
//                        if (stmt != null && config.isExecute()) {
//                            stmt.execute(createTable);
//                        }
//                        tblMirror.addAction("RIGHT Schema Create", Boolean.TRUE);
//                        tblMirror.setMigrationStageMessage("Table created in RIGHT cluster");
//                        rtn = Boolean.TRUE;
//                    }
//                } else if (tblMirror.schemasEqual(Environment.LEFT, Environment.RIGHT)) {
//                    tblMirror.addIssue(Environment.RIGHT, "Schema Fields(name, type, and order), Row Format, and Table Format are consistent between clusters.");
//                    tblMirror.addIssue(Environment.RIGHT, "TBLProperties were NOT considered while comparing schemas");
//                    tblMirror.addIssue(Environment.RIGHT, "No Schema action performed");
//                    rtn = Boolean.TRUE;
//                } else {
//                    tblMirror.addIssue(Environment.RIGHT, "Schema Fields(name, type, and order), Row Format, and Table Format are NOT consistent between clusters.");
//                    tblMirror.addIssue(Environment.RIGHT, "Use --sync option to overwrite table schema with changes");
//                    tblMirror.addIssue(Environment.RIGHT, "No Schema action performed");
//                    rtn = Boolean.TRUE;
//                }
//                if (!config.isExecute()) {
//                    tblMirror.addIssue(Environment.RIGHT, "DRY-RUN mode");
//                }
//
//            } catch (SQLException throwables) {
//                LOG.error("Issue", throwables);
//                tblMirror.addIssue(Environment.RIGHT, throwables.getMessage());
//            } catch (RuntimeException throwables) {
//                LOG.error("Issue", throwables);
//                tblMirror.addIssue(Environment.RIGHT, throwables.getMessage());
//            } finally {
//                try {
//                    if (resultSet != null)
//                        resultSet.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (stmt != null)
//                        stmt.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (conn != null)
//                        conn.close();
//                } catch (SQLException throwables) {
//                    //
//                }
//                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
//            }
//        }
//        return rtn;
//    }

    /*
    Import the table schema from a 'shell' export (transition schema) on the lower cluster
    then redirect to using data in the lower cluster
    */
//    public Boolean importTransferSchemaUsingLowerData(Config config, DBMirror dbMirror, TableMirror tblMirror,
//                                                      String exportBaseDirPrefix) {
//        // Open the connection and ensure we are running this on the "RIGHT" cluster.
//        Boolean rtn = Boolean.FALSE;
//        if (this.getEnvironment() == Environment.RIGHT) {
//            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));
//
//            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
//            Connection conn = null;
//            Statement stmt = null;
//            ResultSet resultSet = null;
//            try {
//                conn = getConnection();
//
//                String tableName = tblMirror.getName();
//
//                stmt = conn.createStatement();
//
//                // Set db context;
//                String useDb = MessageFormat.format(MirrorConf.USE, database);
//
//                LOG.debug(useDb);
//                tblMirror.addSql(useDb);
//                if (config.isExecute())
//                    stmt.execute(useDb);
//
//                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {
//                    // Get the definition for this environment.
//                    List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
//                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Importing Table from " +
//                            config.getCluster(Environment.LEFT).getHcfsNamespace() + exportBaseDirPrefix + dbMirror.getName() + "/" + tableName);
//
//                    String importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE,
//                            database, tableName,
//                            config.getCluster(Environment.LEFT).getHcfsNamespace() + exportBaseDirPrefix + dbMirror.getName() + "/" + tableName);
//                    LOG.debug(getEnvironment() + ":(SQL)" + importTransferSchema);
//
//                    tblMirror.addSql(importTransferSchema);
//                    if (config.isExecute()) {
//                        stmt.execute(importTransferSchema);
//                    } else {
//                        tblMirror.addIssue("DRY-RUN: IMPORT table NOT run");
//                    }
//                    tblMirror.addAction("IMPORTED", Boolean.TRUE);
//
//                    // Alter Table Data Location back to the original table.
//                    // TODO: I think this triggers an msck....  need to verify
//                    String originalLocation = TableUtils.getLocation(tableName, tblMirror.getTableDefinition(Environment.LEFT));
//                    String alterTableLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, database, tableName, originalLocation);
//                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Altering table Location to original table in lower cluster");
//                    LOG.debug(getEnvironment() + ":(SQL)" + alterTableLocSql);
//
//                    tblMirror.addSql(alterTableLocSql);
//                    if (config.isExecute()) {
//                        stmt.execute(alterTableLocSql);
//                    } else {
//                        tblMirror.addIssue("DRY-RUN: ALTER Table location NOT run");
//                    }
//                    tblMirror.addAction("LOCATION Adjusted(to Original LEFT)", Boolean.TRUE);
//
//                    String flagDate = df.format(new Date());
//                    String hmsMirrorFlagStage1 = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
//                            MirrorConf.HMS_MIRROR_METADATA_FLAG, flagDate);
//                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
//                            ": Setting table property to identify as 'Stage-1' conversion.");
//                    LOG.debug(getEnvironment() + "(SQL)" + hmsMirrorFlagStage1);
//
//                    tblMirror.addSql(hmsMirrorFlagStage1);
//                    if (config.isExecute()) {
//                        stmt.execute(hmsMirrorFlagStage1);
//                    } else {
//                        tblMirror.addIssue("DRY-RUN: RIGHT table property not set: " + MirrorConf.HMS_MIRROR_METADATA_FLAG.toString());
//                    }
//                    tblMirror.addProp(MirrorConf.HMS_MIRROR_METADATA_FLAG, flagDate);
//
//                    String hmsMirrorConverted = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
//                            MirrorConf.HMS_MIRROR_CONVERTED_FLAG, "true");
//                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
//                            ": Setting table property to identify as 'hms-mirror' converted");
//                    LOG.debug(getEnvironment() + ":(SQL)" + hmsMirrorConverted);
//
//                    tblMirror.addSql(hmsMirrorConverted);
//                    if (config.isExecute()) {
//                        stmt.execute(hmsMirrorConverted);
//                    } else {
//                        tblMirror.addIssue("DRY-RUN: RIGHT table property not set: " + MirrorConf.HMS_MIRROR_CONVERTED_FLAG.toString());
//                    }
//                    tblMirror.addProp(MirrorConf.HMS_MIRROR_CONVERTED_FLAG,"true");
//
//                    // Ensure the partition discovery matches our config directive
//                    String dpProp = null;
//                    if (config.getCluster(Environment.RIGHT).getPartitionDiscovery().getAuto()) {
//                        dpProp = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(), MirrorConf.DISCOVER_PARTITIONS, "true");
//                        tblMirror.addProp(MirrorConf.DISCOVER_PARTITIONS, "true");
//
//                    } else {
//                        dpProp = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(), MirrorConf.DISCOVER_PARTITIONS, "false");
//                        tblMirror.addProp(MirrorConf.DISCOVER_PARTITIONS, "false");
//                    }
//
//                    tblMirror.addSql(dpProp);
//                    if (config.isExecute()) {
//                        stmt.execute(dpProp);
//                    } else {
//                        tblMirror.addIssue("DRY-RUN: RIGHT table properties not set: " + MirrorConf.DISCOVER_PARTITIONS.toString());
//                    }
//
//
//                    rtn = Boolean.TRUE;
//                } else {
//                    tblMirror.addIssue("Table exists before transfer and 'OVERWRITE' was not ON");
//                }
//            } catch (SQLException throwables) {
//                LOG.error("Issue", throwables);
//            } finally {
//                try {
//                    if (resultSet != null)
//                        resultSet.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (stmt != null)
//                        stmt.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (conn != null)
//                        conn.close();
//                } catch (SQLException throwables) {
//                    //
//                }
//                LOG.debug(getEnvironment().toString() + ":END:" + database + "." + tblMirror.getName());
//            }
//        }
//        return rtn;
//    }

    /*
    Import the table schema AND data from the LEFT cluster.  This is a migration of the schema
    and data from the lower cluster.
    */
//    public Boolean importSchemaWithData(Config config, DBMirror dbMirror, TableMirror tblMirror,
//                                        String exportBaseDirPrefix) {
//        // Open the connection and ensure we are running this on the "RIGHT" cluster.
//        Boolean rtn = Boolean.FALSE;
//        if (this.getEnvironment() == Environment.RIGHT) {
//            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));
//
//            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
//            Connection conn = null;
//            Statement stmt = null;
//            ResultSet resultSet = null;
//            try {
//                conn = getConnection();
//                if (conn != null) {
//
//                    String tableName = tblMirror.getName();
//
//                    stmt = conn.createStatement();
//
//                    // Set db context;
//                    String useDb = MessageFormat.format(MirrorConf.USE, database);
//
//                    LOG.debug(useDb);
//                    tblMirror.addSql(getEnvironment(), useDb);
//                    if (config.isExecute())
//                        stmt.execute(useDb);
//
//                    if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {
//
//                        // Get the definition for this environment.
//                        List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
//                        LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Importing Table from " +
//                                config.getCluster(Environment.LEFT).getHcfsNamespace() + exportBaseDirPrefix + dbMirror.getName() + "/" + tableName);
//
//                        // Import for the location of the export on the lower cluster, to the same relative location
//                        // on the upper cluster.
//                        String importTransferSchema = null;
//                        if (TableUtils.isACID(tableName, tblMirror.getTableDefinition(Environment.LEFT))) {
//                            // Support ACID table migrations. Location here isn't relevant and will
//                            // go to the default managed location.
//                            importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_TABLE,
//                                    database, tableName,
//                                    config.getCluster(Environment.LEFT).getHcfsNamespace() + exportBaseDirPrefix +
//                                            dbMirror.getName() + "/" + tableName);
//                            tblMirror.addAction("ACID Table", Boolean.TRUE);
//                        } else {
//                            // Get Original Location
//                            String originalLowerLocation = TableUtils.getLocation(tableName, tblMirror.getTableDefinition(Environment.LEFT));
//                            String upperLocation = originalLowerLocation.replace(config.getCluster(Environment.LEFT).getHcfsNamespace(),
//                                    config.getCluster(Environment.RIGHT).getHcfsNamespace());
//                            importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION,
//                                    database, tableName,
//                                    config.getCluster(Environment.LEFT).getHcfsNamespace() + exportBaseDirPrefix +
//                                            dbMirror.getName() + "/" + tableName, upperLocation);
//                        }
//                        LOG.debug(getEnvironment() + ":(SQL)" + importTransferSchema);
//                        tblMirror.setMigrationStageMessage("Import Schema w/Data.  This could take a while, be patient. Partition Count and Data Volume have an impact");
//
//                        tblMirror.addSql(getEnvironment(), importTransferSchema);
//                        if (config.isExecute()) {
//                            stmt.execute(importTransferSchema);
//                        } else {
//                            tblMirror.addIssue(getEnvironment(), "DRY-RUN: IMPORT transfer schema NOT run");
//                        }
//                        tblMirror.setMigrationStageMessage("Import Complete");
//                        tblMirror.addAction("IMPORTED (w/data)", Boolean.TRUE);
//
//                        if (TableUtils.isLegacyManaged(config.getCluster(Environment.LEFT), tblMirror.getName(),
//                                tblMirror.getTableDefinition(Environment.LEFT))) {
//                            String externalPurge = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
//                                    MirrorConf.EXTERNAL_TABLE_PURGE, "true");
//                            LOG.debug(getEnvironment() + ":" + database + "." + tableName +
//                                    ": Setting table property " + MirrorConf.EXTERNAL_TABLE_PURGE +
//                                    " because this table was a legacy Managed Table.");
//                            LOG.debug(getEnvironment() + ":(SQL) " + externalPurge);
//
//                            tblMirror.addSql(getEnvironment(), externalPurge);
//                            if (config.isExecute()) {
//                                stmt.execute(externalPurge);
//                            } else {
//                                tblMirror.addIssue(getEnvironment(), "DRY-RUN: RIGHT table properties not set: " + MirrorConf.EXTERNAL_TABLE_PURGE);
//                            }
//                            tblMirror.addProp(Environment.RIGHT, MirrorConf.EXTERNAL_TABLE_PURGE, "true");
//
//                        } else {
//                            // TODO: RERUNNING THE PROCESS WILL BREAK BECAUSE THE TABLE DATA WON'T BE DROP WHEN THE TABLE
//                            //         IS DROPPED, WHICH PREVENTS THE IMPORT FROM SUCCEEDING.
//                        }
//
//                        // Trigger Discover Partitions.
//                        // for legacy managed tables
//                        // if auto discovery flag set.
//                        if (TableUtils.isLegacyManaged(this, tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) ||
//                                config.getCluster(Environment.RIGHT).getPartitionDiscovery().getAuto()) {
//
//                            String discoverPartitions = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
//                                    MirrorConf.DISCOVER_PARTITIONS, "true");
//                            LOG.debug(getEnvironment() + ":" + database + "." + tableName +
//                                    ": Setting table property " + MirrorConf.DISCOVER_PARTITIONS +
//                                    " because this table was a legacy Managed Table or 'auto' discovery set in migration config.");
//                            LOG.debug(getEnvironment() + ":(SQL) " + discoverPartitions);
//                            tblMirror.setMigrationStageMessage("Adding 'discover.partitions' to table properties");
//
//                            tblMirror.addSql(getEnvironment(), discoverPartitions);
//                            if (config.isExecute()) {
//                                stmt.execute(discoverPartitions);
//                            } else {
//                                tblMirror.addIssue(getEnvironment(), "DRY-RUN: RIGHT table properties not set: " + MirrorConf.DISCOVER_PARTITIONS);
//                            }
//                            tblMirror.setMigrationStageMessage("'discover.partitions' table property added");
//                            tblMirror.addProp(Environment.RIGHT, MirrorConf.DISCOVER_PARTITIONS, "true");
//                        }
//
//                        // Set IMPORT Flag Table Property
//                        String flagDate = df.format(new Date());
//                        String hmsMirrorImportFlag = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
//                                MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG, flagDate);
//                        LOG.debug(getEnvironment() + ":" + database + "." + tableName +
//                                ": Setting table property to identify as 'IMPORT' conversion.");
//                        LOG.debug(getEnvironment() + ":(SQL)" + hmsMirrorImportFlag);
//                        tblMirror.setMigrationStageMessage("Adding ' " + MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG + "' to table properties");
//
//                        tblMirror.addSql(getEnvironment(), hmsMirrorImportFlag);
//                        if (config.isExecute()) {
//                            stmt.execute(hmsMirrorImportFlag);
//                        } else {
//                            tblMirror.addIssue(getEnvironment(), "DRY-RUN: RIGHT table properties not set: " + MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG);
//                        }
//                        tblMirror.setMigrationStageMessage("'" + MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG + "' table property added");
//
//                        tblMirror.addProp(Environment.RIGHT, MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG, flagDate);
//
//                        // Set Converted Table Property
//                        String hmsMirrorConverted = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
//                                MirrorConf.HMS_MIRROR_CONVERTED_FLAG, "true");
//                        LOG.debug(getEnvironment() + ":" + database + "." + tableName +
//                                ": Setting table property to identify as 'hms-mirror' converted");
//                        LOG.debug(getEnvironment() + ":(SQL)" + hmsMirrorConverted);
//                        tblMirror.setMigrationStageMessage("Adding ' " + MirrorConf.HMS_MIRROR_CONVERTED_FLAG + "' to table properties");
//
//                        tblMirror.addSql(getEnvironment(), hmsMirrorConverted);
//                        if (config.isExecute()) {
//                            stmt.execute(hmsMirrorConverted);
//                        } else {
//                            tblMirror.addIssue(getEnvironment(), "DRY-RUN: RIGHT table properties not set: " + MirrorConf.HMS_MIRROR_CONVERTED_FLAG);
//                        }
//                        tblMirror.setMigrationStageMessage("'" + MirrorConf.HMS_MIRROR_CONVERTED_FLAG + "' table property added");
//
//                        tblMirror.addProp(Environment.RIGHT, MirrorConf.HMS_MIRROR_CONVERTED_FLAG, "true");
//
//                        rtn = Boolean.TRUE;
//                    }
//                }
//            } catch (SQLException throwables) {
//                LOG.error("Issue", throwables);
//                tblMirror.addIssue(getEnvironment(), throwables.getMessage());
//            } finally {
//                try {
//                    if (resultSet != null)
//                        resultSet.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (stmt != null)
//                        stmt.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (conn != null)
//                        conn.close();
//                } catch (SQLException throwables) {
//                    //
//                }
//                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
//            }
//        }
//        return rtn;
//    }

    //////////////////////////
    //   DATA MOVEMENT
    /////////////////////////

    /*

     */
//    public Boolean doSqlDataTransfer(Config config, DBMirror dbMirror, TableMirror tblMirror, Environment source, Environment target) {
//        // Open the connection and ensure we are running this on the "RIGHT" cluster.
//        Boolean rtn = Boolean.FALSE;
////        if (this.getEnvironment() == Environment.RIGHT) {
//        String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));
//
//        LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
//        Connection conn = null;
//        Statement stmt = null;
//        ResultSet resultSet = null;
//        try {
//            conn = getConnection();
//            if (conn != null) {
//
////                String database = dbMirror.getName();
//                String tableName = tblMirror.getName();
//
//                stmt = conn.createStatement();
//
//                // Set db context;
//                String useDb = MessageFormat.format(MirrorConf.USE, database);
//                LOG.debug(useDb);
//                tblMirror.addSql(useDb);
//                if (config.isExecute())
//                    stmt.execute(useDb);
//
//                String sqlDataTransfer = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER, tableName, transferPrefix + tableName);
//
//                LOG.debug(getEnvironment() + ":(SQL)" + sqlDataTransfer);
//                tblMirror.setMigrationStageMessage("Migrating data using Hive SQL.  Please wait. Check Hive Service for Job Details");
//
//                tblMirror.addSql(sqlDataTransfer);
//                if (config.isExecute()) {
//                    stmt.execute(sqlDataTransfer);
//                } else {
//                    tblMirror.addIssue("DRY-RUN: SQL Transfer NOT run");
//                }
//                tblMirror.setMigrationStageMessage("Hive SQL Migration Complete");
//                tblMirror.addAction("IMPORTED (via SQL)", Boolean.TRUE);
//                rtn = Boolean.TRUE;
//            }
//        } catch (SQLException throwables) {
//            LOG.error("Issue", throwables);
//            tblMirror.addIssue(throwables.getMessage());
//        } catch (RuntimeException throwables) {
//            LOG.error("Issue", throwables);
//            tblMirror.addIssue(throwables.getMessage());
//        } finally {
//            try {
//                if (resultSet != null)
//                    resultSet.close();
//            } catch (SQLException sqlException) {
//                // ignore
//            }
//            try {
//                if (stmt != null)
//                    stmt.close();
//            } catch (SQLException sqlException) {
//                // ignore
//            }
//            try {
//                if (conn != null)
//                    conn.close();
//            } catch (SQLException throwables) {
//                //
//            }
//            LOG.debug(getEnvironment().toString() + ":END:" + database + "." + tblMirror.getName());
//        }
////        }
//        return rtn;
//    }

    /*
    Prior to this process the following should be true:
     - A transition table has been create and points to the same 'relative' location on the NEW cluster as the LEFT cluster
     - An original table has been create on the RIGHT cluster and points to the LEFT dataset.  This table is 'external' and 'non' purgeable.
     - A SQL data transfer has happened to move the data into the 'transfer' table.
     - The transfer table has been set to match the legacy managed behavior ('external.table.purge')

     In this process, we will:
      - "DROP" the 'original' table that points to the LEFT cluster
      - RENAME the transfer table to the 'original' table name.
     */
//    public Boolean completeSqlTransfer(Config config, DBMirror dbMirror, TableMirror tblMirror) {
//        // Open the connection and ensure we are running this on the "RIGHT" cluster.
//        Boolean rtn = Boolean.FALSE;
//        if (this.getEnvironment() == Environment.RIGHT) {
//
//            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));
//
//            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
//            Connection conn = null;
//            Statement stmt = null;
//            ResultSet resultSet = null;
//            try {
//                conn = getConnection();
//                if (conn != null) {
//
//                    String tableName = tblMirror.getName();
//
//                    stmt = conn.createStatement();
//
//                    // Set db context;
//                    String useDb = MessageFormat.format(MirrorConf.USE, database);
//                    LOG.debug(useDb);
//                    tblMirror.addSql(useDb);
//                    if (config.isExecute())
//                        stmt.execute(useDb);
//
//                    String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, database, tableName);
//
//                    LOG.debug(getEnvironment() + ":(SQL) " + dropTable);
//                    tblMirror.setMigrationStageMessage("Dropping original table (start of swap process)");
//
//                    tblMirror.addSql(dropTable);
//                    if (config.isExecute()) {
//                        stmt.execute(dropTable);
//                    } else {
//                        tblMirror.addIssue("DRY-RUN: Table NOT dropped");
//                    }
//                    tblMirror.setMigrationStageMessage("Table dropped");
//                    tblMirror.addAction(getEnvironment().toString(), "Table Dropped");
//
//                    String renameTable = MessageFormat.format(MirrorConf.RENAME_TABLE, config.getTransfer().getTransferPrefix() + tableName, tableName);
//                    LOG.debug(getEnvironment() + ":(SQL) " + renameTable);
//                    tblMirror.setMigrationStageMessage("Rename transfer table to original table (complete swap process)");
//                    tblMirror.addSql(renameTable);
//                    if (config.isExecute()) {
//                        stmt.execute(renameTable);
//                    } else {
//                        tblMirror.addIssue("DRY-RUN: Table rename NOT run");
//                    }
//                    tblMirror.setMigrationStageMessage("Rename complete");
//                    tblMirror.addAction(getEnvironment().toString(), "Transfer Renamed");
//
//                    rtn = Boolean.TRUE;
//                }
//            } catch (SQLException throwables) {
//                LOG.error("Issue", throwables);
//                tblMirror.addIssue(throwables.getMessage());
//            } catch (RuntimeException throwables) {
//                LOG.error("Issue", throwables);
//                tblMirror.addIssue(throwables.getMessage());
//            } finally {
//                try {
//                    if (resultSet != null)
//                        resultSet.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (stmt != null)
//                        stmt.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (conn != null)
//                        conn.close();
//                } catch (SQLException throwables) {
//                    //
//                }
//                LOG.debug(getEnvironment().toString() + ":END:" + database + "." + tblMirror.getName());
//            }
//        }
//        return rtn;
//    }

//    public Boolean partitionMaintenance(Config config, DBMirror dbMirror, TableMirror tblMirror) {
//        return partitionMaintenance(config, dbMirror, tblMirror, Boolean.FALSE);
//    }

    /*
    Review the parameters and determine if we will issue an 'MSCK' on the table in the new cluster
     */
//    public Boolean partitionMaintenance(Config config, DBMirror dbMirror, TableMirror tblMirror, Boolean force) {
//        // Need to check if table is partitioned
//        Boolean rtn = Boolean.TRUE;
//        if (this.getEnvironment() == Environment.RIGHT &&
//                TableUtils.isPartitioned(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT))) {
//
//            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));
//
//            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
//            Connection conn = null;
//            Statement stmt = null;
//            ResultSet resultSet = null;
//            try {
//
//                String tableName = tblMirror.getName();
//
//                if (this.getPartitionDiscovery().getInitMSCK() || force) {
//                    conn = getConnection();
//                    if (conn != null) {
//
//                        stmt = conn.createStatement();
//
//                        LOG.debug(getEnvironment() + ":" + database + "." + tableName +
//                                ": Discovering " + tblMirror.getPartitionDefinition(Environment.LEFT).size() + " partitions");
//                        String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, database, tblMirror.getName());
//                        LOG.debug(getEnvironment() + ":(SQL)" + msckStmt);
//                        tblMirror.setMigrationStageMessage("Discovering " + tblMirror.getPartitionDefinition(Environment.LEFT).size() + " partitions with 'MSCK'");
//                        tblMirror.addSql(msckStmt);
//                        if (config.isExecute()) {
//                            stmt.execute(msckStmt);
//                        } else {
//                            tblMirror.addIssue("DRY-RUN: MSCK NOT run");
//                        }
//                        tblMirror.addAction("MSCK ran", Boolean.TRUE);
//                        tblMirror.setMigrationStageMessage("Partition discovery with 'MSCK' complete");
//                        rtn = Boolean.TRUE;
//                    }
//                }
//
//                String msg;
//                if (this.getPartitionDiscovery().getAuto()) {
//                    if (this.getPartitionDiscovery().getInitMSCK()) {
//                        msg = "This table has partitions and is set for 'auto' discovery via table property 'discover.partitions'='true'. " +
//                                "You've requested an immediate 'MSCK' for the table, so the partitions will be current. " +
//                                "For future partition discovery, ensure the Metastore is running the 'PartitionManagementTask' service.";
//                    } else {
//                        msg = "This table has partitions and is set for 'auto' discovery via table property 'discover.partitions'='true'. " +
//                                "Ensure the Metastore is running the 'PartitionManagementTask' service.";
//                    }
//                } else {
//                    if (this.getPartitionDiscovery().getInitMSCK()) {
//                        msg = "You've requested an immediate 'MSCK' for the table, so the partitions will be current. " +
//                                "Future partitions will NOT be auto discovered unless you manually add " +
//                                "'discover.partitions'='true' to the tables properties. " +
//                                "Once done, ensure the Metastore is running the 'PartitionManagementTask' service.";
//                    } else {
//                        msg = "This table has partitions and is NOT set for 'auto' discovery.  Data may not be available until the partition metadata is rebuilt manually.";
//                    }
//                }
//                tblMirror.addIssue(msg);
//                LOG.info(msg);
//
//            } catch (SQLException throwables) {
//                LOG.error("Issue", throwables);
//                tblMirror.addIssue(throwables.getMessage());
//                rtn = Boolean.FALSE;
//            } catch (Throwable throwables) {
//                throwables.printStackTrace();
//            } finally {
//                try {
//                    if (resultSet != null)
//                        resultSet.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (stmt != null)
//                        stmt.close();
//                } catch (SQLException sqlException) {
//                    // ignore
//                }
//                try {
//                    if (conn != null)
//                        conn.close();
//                } catch (SQLException throwables) {
//                    //
//                }
//                LOG.debug(getEnvironment().toString() + ":END:" + database + "." + tblMirror.getName());
//            }
//
//        }
//        return rtn;
//    }

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
