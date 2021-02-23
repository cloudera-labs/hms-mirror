package com.streever.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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

    protected Connection getConnection() throws SQLException {
        Connection conn = null;
        conn = pools.getEnvironmentConnection(getEnvironment());
        return conn;
    }

    public void getTables(Config config, DBMirror dbMirror) throws SQLException {
        Connection conn = null;
        try {
            conn = getConnection();

            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

            LOG.debug(getEnvironment() + ":" + database + ": Loading tables for database");

            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                stmt = conn.createStatement();
                LOG.debug(getEnvironment() + ":" + database + ": Setting Hive DB Session Context");
                stmt.execute(MessageFormat.format(MirrorConf.USE, database));
                LOG.debug(getEnvironment() + ":" + database + ": Getting Table List");
                resultSet = stmt.executeQuery(MirrorConf.SHOW_TABLES);
                while (resultSet.next()) {
                    String tableName = resultSet.getString(1);
                    if (config.getTblRegEx() == null) {
                        TableMirror tableMirror = dbMirror.addTable(tableName);
                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                    } else {
                        // Filter Tables
                        assert (config.getDbFilterPattern() != null);
                        Matcher matcher = config.getDbFilterPattern().matcher(tableName);
                        if (matcher.matches()) {
                            TableMirror tableMirror = dbMirror.addTable(tableName);
                            tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                        }
                    }
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
        } catch (SQLException se) {
            throw se;
        } finally {
            try {
                conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    public Boolean getTableDefinition(String database, TableMirror tableMirror) throws SQLException {
        // The connection should already be in the database;
        Boolean rtn = Boolean.FALSE;
        Connection conn = null;
        try {
            conn = getConnection();

            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                stmt = conn.createStatement();
                LOG.debug(getEnvironment() + ":" + database + "." + tableMirror.getName() +
                        ": Loading Table Definition");
                resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_CREATE_TABLE, database, tableMirror.getName()));
                List<String> tblDef = new ArrayList<String>();
                while (resultSet.next()) {
                    tblDef.add(resultSet.getString(1));
                }
                tableMirror.setTableDefinition(getEnvironment(), tblDef);
                tableMirror.addAction(this.getEnvironment().toString(), "Fetched Schema");
                if (TableUtils.isACID(tableMirror.getName(), tblDef)) {
                    tableMirror.addAction("TRANSACTIONAL", Boolean.TRUE);
//                    tableMirror.addIssue("Transactional/ACID tables are NOT currently supported by 'hms-mirror'");
                }

                Boolean partitioned = TableUtils.isPartitioned(tableMirror.getName(), tblDef);
                tableMirror.setPartitioned(getEnvironment(), partitioned);
                if (partitioned) {
                    loadTablePartitionMetadata(database, tableMirror);
                }
                rtn = Boolean.TRUE;
            } catch (SQLException throwables) {
                if (throwables.getMessage().contains("Table not found")) {
                    // This is ok in the upper cluster where we don't expect the table to exist if the process hadn't run before.
                    rtn = Boolean.TRUE;
                    tableMirror.addAction(this.getEnvironment().toString(), "No Schema");
                } else {
                    throwables.printStackTrace();
                    tableMirror.addIssue(this.getEnvironment().toString() + ":" + throwables.getMessage());
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
        } finally {
            try {
                conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
        return rtn;
    }

    public void createDatabase(Config config, String database) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Connection conn = null;
        try {
            conn = getConnection();

            LOG.debug(getEnvironment() + " - Create Database: " + database);

            Statement stmt = null;
            try {
                try {
                    stmt = conn.createStatement();
                } catch (SQLException throwables) {
                    LOG.error("Issue building statement", throwables);
//                    return false;
                }

                // CREATE DB.
                String createDb = MessageFormat.format(MirrorConf.CREATE_DB, database);
                try {
                    LOG.debug(getEnvironment() + ":" + createDb);
//                    if (config.isExecute()) // on dry-run, without db, hard to get through the rest of the steps.
                    stmt.execute(createDb);
                } catch (SQLException throwables) {
                    LOG.error(getEnvironment() + ":Creating DB", throwables);
//                    return false;
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

    protected Boolean checkAndDoOverwrite(Statement stmt, Config config, DBMirror dbMirror, TableMirror tblMirror) {
        return checkAndDoOverwrite(stmt, config, dbMirror, tblMirror, null);
    }

    protected Boolean checkAndDoOverwrite(Statement stmt, Config config, DBMirror dbMirror, TableMirror tblMirror, String tblPrefix) {
        Boolean rtn = Boolean.TRUE;

        String database = dbMirror.getName();
        tblMirror.setMigrationStageMessage("Checking whether the overwrite");
        String tableName = tblPrefix == null ? tblMirror.getName() : tblPrefix + tblMirror.getName();
        try {
            // Run check for non-prefix (transfer) tables.
            if (tblPrefix == null) {
                // Check if we should overwrite existing table.
                if (tblMirror.getTableDefinition(Environment.RIGHT) != null) {
                    // TODO: If the schemas are different, drop and recreate.  If the same, do nothing.
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Exists in RIGHT cluster. ");
                    tblMirror.addIssue("Schema exists already");
                    tblMirror.addIssue("No Schema action performed");
                    // Compare Schemas
                    if (tblMirror.schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                        tblMirror.addIssue("Schema Fields(name, type, and order), Row Format, and Table Format are consistent between clusters.");
                        tblMirror.addIssue("TBLProperties were NOT considered while comparing schemas");
                    } else {
                        tblMirror.addIssue("SCHEMA HAS CHANGED");
                        tblMirror.addIssue("DROP before running again to MIGRATE FRESH SCHEMA.");
                        tblMirror.addIssue("If you are dropping table, understand the impact on the underlying data (is it attached).");
                    }
                    rtn = Boolean.FALSE;
                }

//                switch (config.getReplicationStrategy()) {
//                    case OVERWRITE:
//                        // Check if table exist or if we're working with a transfer table (in which case, drop)
//                        if (tblMirror.getTableDefinition(Environment.RIGHT) != null) {
//                            LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Overwrite ON: Dropping table (if exists)");
//                            tblMirror.addAction("Overwrite ON", Boolean.TRUE);
//                            // Table Exists.
//                            // Check that it was put there by 'hms-mirror'.  If not, we won't replace it.
//                            if (TableUtils.isHMSConverted(tableName, tblMirror.getTableDefinition(Environment.RIGHT))) {
//                                // Check if the table has been converted to an ACID table.
//                                if (config.getStage().equals(Stage.METADATA) && TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT))) {
//                                    String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Has been converted to " +
//                                            "a 'transactional' table.  And isn't safe to drop before re-importing. " +
//                                            "Review the table and remove manually before running process again.";
//                                    tblMirror.addIssue(errorMsg);
//                                    LOG.error(errorMsg);
//                                    // No need to continue.
//                                    rtn = Boolean.FALSE;
//                                }
//                                // Check if the table is "Managing" data.  If so, we can't safety drop the table without effecting
//                                // the data.
//                                if (!TableUtils.isHive3Standard(tableName, tblMirror.getTableDefinition(Environment.RIGHT))) {
//                                    String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Is a 'Managed Non-Transactional' " +
//                                            "which is a non-standard configuration in Hive 3.  Regardless, the data is " +
//                                            "managed and it's not safe to proceed with converting this table. " +
//                                            "Review the table and remove manually before running process again.";
//                                    tblMirror.addIssue(errorMsg);
//                                    LOG.error(errorMsg);
//                                    // No need to continue.
//                                    rtn = Boolean.FALSE;
//                                }
//
//                                // Ensure the table purge flag hasn't been set.
//                                // ONLY prevent during METADATA stage.
//                                // FOR the STORAGE stage, drop the table and data and import.
//                                if (TableUtils.isExternalPurge(tblMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT))) {
//                                    String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Is a 'Purge' " +
//                                            "enabled 'EXTERNAL' table.  The data is " +
//                                            "managed and it's not safe to proceed with converting this table. " +
//                                            "Review the table and remove manually before running process again.";
//                                    tblMirror.addIssue(errorMsg);
//                                    LOG.error(errorMsg);
//                                    // No need to continue.
//                                    rtn = Boolean.FALSE;
//                                }
//
//                            } else {
//                                // It was NOT created by HMS Mirror.  The table will need to be manually dropped after review.
//                                // No need to continue.
//                                String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Was NOT originally " +
//                                        "created by the 'hms-mirror' process and isn't safe to proceed. " +
//                                        "The table will need to be reviewed and dropped manually and the process run again.";
//                                tblMirror.addIssue(errorMsg);
//                                LOG.error(errorMsg);
//                                rtn = Boolean.FALSE;
//                            }
//                            if (rtn) {
//                                LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Dropping table");
//                                // It was created by HMS Mirror, safe to drop.
//                                String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, database, tableName);
//                                LOG.debug(getEnvironment() + ":(SQL)" + dropTable);
//                                tblMirror.setMigrationStageMessage("Dropping table (overwrite)");
//
//                                if (config.isExecute())
//                                    stmt.execute(dropTable);
//                                // TODO: change current to address tablename so we pickup transfer.
//                                tblMirror.setMigrationStageMessage("Table dropped");
//                                tblMirror.addAction(getEnvironment().toString(), "DROPPED");
//                            }
//                        }
//                        break;
//                    case SYNCHRONIZE:
//                        if (tblMirror.getTableDefinition(Environment.RIGHT) != null) {
//                            // TODO: If the schemas are different, drop and recreate.  If the same, do nothing.
//                            LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Exists in RIGHT cluster. ");
//                            tblMirror.addIssue("Schema exists already");
//                            tblMirror.addIssue("No Schema action performed");
//                            rtn = Boolean.FALSE;
//                        }
//                        break;
//                }
            } else {
                // Always drop the transfer table.
                if (rtn) {
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Dropping table");
                    // It was created by HMS Mirror, safe to drop.
                    String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, database, tableName);
                    LOG.debug(getEnvironment() + ":(SQL)" + dropTable);
                    tblMirror.setMigrationStageMessage("Dropping table (overwrite)");

                    tblMirror.addSql(dropTable);
                    if (config.isExecute()) {
                        stmt.execute(dropTable);
                    } else {
                        tblMirror.addIssue("DRY-RUN: Table NOT dropped");
                    }
                    // TODO: change current to address tablename so we pickup transfer.
                    tblMirror.setMigrationStageMessage("Table dropped");
                    tblMirror.addAction(getEnvironment().toString(), "DROPPED Transfer (if existed)");
                }
            }
        } catch (SQLException throwables) {
            tblMirror.addIssue(this.getEnvironment() + ":" + tableName + "->" + throwables.getMessage());
        }
        return rtn;
    }

    protected void loadTablePartitionMetadata(String database, TableMirror tableMirror) throws SQLException {
        Connection conn = null;
        try {
            conn = getConnection();

            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                stmt = conn.createStatement();
                LOG.debug(getEnvironment() + ":" + database + "." + tableMirror.getName() +
                        ": Loading Partitions");

                resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_PARTITIONS, database, tableMirror.getName()));
                List<String> partDef = new ArrayList<String>();
                while (resultSet.next()) {
                    partDef.add(resultSet.getString(1));
                }
                tableMirror.setPartitionDefinition(getEnvironment(), partDef);
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
        } finally {
            try {
                conn.close();
            } catch (SQLException throwables) {
                //
            }
        }

    }


    //////////////////////////
    //   METADATA MOVEMENT
    //////////////////////////

    public Boolean buildTransferTableSchema(Config config, String transferDatabase, DBMirror dbMirror, TableMirror
            tblMirror) {
        Connection conn = null;
        Statement stmt = null;
        Boolean rtn = Boolean.FALSE;

//        String database = dbMirror.getName();
        String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

        String tableName = tblMirror.getName();
        LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
        try {
            conn = getConnection();

            stmt = conn.createStatement();

            // Get the definition for this environment.
            List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());

            String dropTransferTable = MessageFormat.format(MirrorConf.DROP_TABLE,
                    transferDatabase, tableName);

            LOG.debug(getEnvironment() + ":" + transferDatabase + "." + tableName +
                    ": Dropping existing transfer schema (if exists) for table: ");
            LOG.debug(getEnvironment() + ":(SQL)" + dropTransferTable);

            tblMirror.addSql(dropTransferTable);
            if (config.isExecute()) {
                stmt.execute(dropTransferTable);
            } else {
                tblMirror.addIssue("DRY-RUN: TRANSFER table NOT dropped");
            }
            tblMirror.addAction("TRANSFER(LEFT) Schema", "Built");

            String transferCreateTable = MessageFormat.format(MirrorConf.CREATE_EXTERNAL_LIKE,
                    transferDatabase, tblMirror.getName(),
                    database, tblMirror.getName());

            LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                    ": Creating transition schema for table");
            LOG.debug(getEnvironment() + ":(SQL)" + transferCreateTable);

            tblMirror.addSql(transferCreateTable);
            if (config.isExecute()) {
                stmt.execute(transferCreateTable);
            } else {
                tblMirror.addIssue("DRY-RUN: Transition table NOT created");
            }
            tblMirror.addAction("Transition Created", Boolean.TRUE);

            // Determine if it's a Legacy Managed Table.
            LOG.debug(getEnvironment() + ":" + database + "." + tblMirror.getName() +
                    ": Checking Legacy Configuration of table");

            Boolean legacyManaged = TableUtils.isLegacyManaged(this, tblMirror.getName(), tblDef);
            if (legacyManaged) {
                String legacyFlag = MessageFormat.format(MirrorConf.ADD_TBL_PROP,
                        transferDatabase, tableName,
                        MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG, Boolean.TRUE.toString()
                );
                LOG.debug(getEnvironment() + ":" + transferDatabase + "." + tableName +
                        ": Setting Legacy flag for table");
                LOG.debug(getEnvironment() + ":(SQL)" + legacyFlag);

                tblMirror.addSql(legacyFlag);
                if (config.isExecute()) {
                    stmt.execute(legacyFlag);
                } else {
                    tblMirror.addIssue("DRY-RUN: Legacy Flag NOT set.");
                }

                tblMirror.addProp(MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG + "=" + Boolean.TRUE.toString());

            }
            rtn = Boolean.TRUE;

        } catch (SQLException throwables) {
            LOG.error(throwables);
            tblMirror.addIssue(throwables.getMessage());
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException sqlException) {
                // ignore
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
            LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
        }
        return rtn;
    }

    public Boolean exportSchema(Config config, String database, DBMirror dbMirror, TableMirror tblMirror, String exportBaseDirPrefix) {
        Connection conn = null;
        Statement stmt = null;
        Boolean rtn = Boolean.FALSE;
        LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
        try {
            conn = getConnection();
            String tableName = tblMirror.getName();

            stmt = conn.createStatement();

            // Get the definition for this environment.
            List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
            LOG.debug(getEnvironment() + ":" + database + "." +
                    tableName + ": Exporting Table to " + config.getCluster(Environment.LEFT).hcfsNamespace + exportBaseDirPrefix + database + "/" + tableName);
            String exportTransferSchema = MessageFormat.format(MirrorConf.EXPORT_TABLE,
                    database, tableName,
                    config.getCluster(Environment.LEFT).hcfsNamespace + exportBaseDirPrefix + database + "/" + tableName);
            LOG.debug(getEnvironment() + ":(SQL)" + exportTransferSchema);
            tblMirror.setMigrationStageMessage("Export Schema");

            tblMirror.addSql(exportTransferSchema);
            if (config.isExecute()) {
                stmt.execute(exportTransferSchema);
            } else {
                tblMirror.addIssue("DRY-RUN: EXPORT table not run");
            }
            tblMirror.setMigrationStageMessage("Schema export complete");
            tblMirror.addAction("EXPORT", "CREATED");

            rtn = Boolean.TRUE;
        } catch (SQLException throwables) {
            tblMirror.addIssue(throwables.getMessage());
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException sqlException) {
                // ignore
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
            LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
        }
        return rtn;
    }

    /*
    In the same target db, build a transfer table from the lower cluster.  This table will be the table
    where we push all data to in the SQL method.  The data will be source by the RIGHT clusters shadow
    table (with original name) in the RIGHT cluster, looking at the data in the lower cluster.
    This transfer table should use the RIGHT cluster for storage and the location is the same (relative) as
    where the data was in the LEFT cluster.
    Once the transfer is complete, the two tables can be swapped in a move that is quick.
    */
    public Boolean buildUpperTransferTable(Config config, DBMirror dbMirror, TableMirror tblMirror, String transferPrefix) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.RIGHT) {

            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

//                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                LOG.debug(useDb);
//                if (config.isExecute()) {

                tblMirror.addSql(useDb);
                stmt.execute(useDb);
//                } else {
////                    tblMirror.addIssue("DRY-RUN: USE not issued");
//                }
                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror, transferPrefix)) {

                    Boolean buildUpper = tblMirror.buildUpperSchema(config, Boolean.FALSE);

                    // Don't change namespace when using Shared Storage.
                    if (config.getDataStrategy() != DataStrategy.COMMON) {
                        TableUtils.changeLocationNamespace(tblMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT),
                                config.getCluster(Environment.LEFT).getHcfsNamespace(),
                                config.getCluster(Environment.RIGHT).getHcfsNamespace());
                    }

                    if (TableUtils.isHMSLegacyManaged(this, tableName, tblMirror.getTableDefinition(Environment.RIGHT))) {
                        tblMirror.setMigrationStageMessage("Adding EXTERNAL purge to table properties");
                        TableUtils.upsertTblProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true", tblMirror.getTableDefinition(Environment.RIGHT));
                    }

                    // Get the Create State for this environment.
                    // This create is the transfer table
                    String createTable = tblMirror.getCreateStatement(this.getEnvironment(), transferPrefix);
                    LOG.debug(getEnvironment() + ":(SQL)" + createTable);

                    // The location should NOT have data in it. If it does, this could cause an issue.
                    // TODO: validate target data location.
                    // The transfer table should be an empty shell and the data directory, which points to the permanent
                    //  location for the target table, should be empty.  If NOT, what do we do?
                    tblMirror.setMigrationStageMessage("Creating table");
                    tblMirror.addSql(createTable);
                    if (config.isExecute()) {
                        stmt.execute(createTable);
                    } else {
                        tblMirror.addIssue("DRY-RUN: Table NOT created");
                    }
                    tblMirror.setMigrationStageMessage("Table created");

                    tblMirror.addAction("Transfer Schema Created", Boolean.TRUE);

                    rtn = Boolean.TRUE;
                }
            } catch (SQLException throwables) {
                LOG.error("Issue", throwables);
                tblMirror.addIssue(throwables.getMessage());
            } catch (RuntimeException throwables) {
                LOG.error("Issue", throwables);
                tblMirror.addIssue(throwables.getMessage());
            } finally {
                try {
                    if (resultSet != null)
                        resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
            }
        }
        return rtn;
    }

    /*
    Using the tbl definition in from the lower cluster, build the 'create' table
    statement for the upper cluster and point it to the data in the lower cluster.
    */
    public Boolean buildUpperSchemaUsingLowerData(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.RIGHT) {
            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

//                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Importing Transfer Schema");

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                LOG.debug(useDb);
//                if (config.isExecute())
                tblMirror.addSql(useDb);
                stmt.execute(useDb);

                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {
//                    Boolean buildUpper = tblMirror.buildUpperSchema(config, config.isCommitToUpper());
                    Boolean buildUpper = tblMirror.buildUpperSchema(config, false);

                    // Turn off for the create/alter
//                    TableUtils.upsertTblProperty(MirrorConf.DISCOVER_PARTITIONS, "false",
//                            tblMirror.getTableDefinition(Environment.RIGHT));

                    // WHEN BUILDING RIGHT SCHEMA, WE ARE GOING TO USE THE RIGHT CLUSTER LOCATION
                    // FIRST WHILE CREATING THE TABLE.  THEN SWITCH IT TO THE LEFT AND RUN MSCK.
                    // Adjust the Location to be Relative to the RIGHT cluster.
//                    TableUtils.changeLocationNamespace(dbMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT),
//                            config.getCluster(Environment.LEFT).getHcfsNamespace(),
//                            config.getCluster(Environment.RIGHT).getHcfsNamespace());

                    // Get the Create State for this environment.
                    String createTable = tblMirror.getCreateStatement(this.getEnvironment());
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ":(SQL)" + createTable);
                    tblMirror.setMigrationStageMessage("Creating Table in RIGHT cluster");
                    if (tblMirror.isPartitioned(Environment.LEFT)) {
                        tblMirror.addIssue("If CREATE/ALTER is slow for migration, add `ranger.plugin.hive.urlauth.filesystem.schemes=file`" +
                                " to the HS2 hive-ranger-security.xml configurations.");
                    }
                    tblMirror.addSql(createTable);
                    if (config.isExecute()) {
                        stmt.execute(createTable);
                    } else {
                        tblMirror.addIssue("DRY-RUN: RIGHT Table NOT created");
                    }

                    tblMirror.addAction("Shadow(using LEFT data) Schema Create", Boolean.TRUE);
                    tblMirror.setMigrationStageMessage("Table created in RIGHT cluster");

                    rtn = Boolean.TRUE;

                }
            } catch (SQLException throwables) {
                tblMirror.addIssue(throwables.getMessage());
                LOG.error("Issue", throwables);
            } catch (RuntimeException throwables) {
                tblMirror.addIssue(throwables.getMessage());
                LOG.error("Issue", throwables);
            } finally {
                try {
                    if (resultSet != null)
                        resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
                LOG.debug(getEnvironment().toString() + ":END:" + database + "." + tblMirror.getName());
            }
        }
        return rtn;
    }


    public Boolean buildUpperSchemaWithRelativeData(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.RIGHT) {
            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

//                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Build Upper Schema");

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                LOG.debug(useDb);
                tblMirror.addSql(useDb);
                stmt.execute(useDb);

                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {
                    Boolean buildUpper;
//                    if (config.getStage() == Stage.METADATA && config.getMetadata().isDisasterRecovery()) {
//                        buildUpper = tblMirror.buildUpperSchema(config, Boolean.FALSE);
//                        tblMirror.addIssue("Disaster Recovery was specified. This table has enable `external.table.purge` to ensure a Read-Only copy on the RIGHT cluster");
//                    } else {
                    /*
                    For SCHEMA_ONLY: adjust if readonly is set.
                     */
                    boolean readonly = (config.isReadOnly() && config.getDataStrategy() == DataStrategy.SCHEMA_ONLY) |
                            config.getDataStrategy() == DataStrategy.LINKED | config.getDataStrategy() == DataStrategy.COMMON;
                    buildUpper = tblMirror.buildUpperSchema(config, !readonly);
//                    }

                    // Adjust the Location to be Relative to the RIGHT cluster.
                    // as long as we're not using shared storage.
                    if (config.getDataStrategy() != DataStrategy.COMMON) {
                        TableUtils.changeLocationNamespace(dbMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT),
                                config.getCluster(Environment.LEFT).getHcfsNamespace(),
                                config.getCluster(Environment.RIGHT).getHcfsNamespace());
                    }

                    if (!TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.RIGHT))) {
                        if (config.getCluster(Environment.RIGHT).getPartitionDiscovery().getAuto())
                            TableUtils.upsertTblProperty(MirrorConf.DISCOVER_PARTITIONS, "true",
                                    tblMirror.getTableDefinition(Environment.RIGHT));
                        else
                            TableUtils.upsertTblProperty(MirrorConf.DISCOVER_PARTITIONS, "false",
                                    tblMirror.getTableDefinition(Environment.RIGHT));
                    }

                    // Get the Create State for this environment.
                    String createTable = tblMirror.getCreateStatement(this.getEnvironment());
                    LOG.debug(getEnvironment() + ":(SQL)" + createTable);
                    tblMirror.setMigrationStageMessage("Creating Table in RIGHT cluster");
                    tblMirror.addSql(createTable);
                    if (config.isExecute()) {
                        stmt.execute(createTable);
                    } else {
                        tblMirror.addIssue("DRY-RUN: Schema NOT Transferred");
                    }
                    tblMirror.addAction("RIGHT Schema Create", Boolean.TRUE);
                    tblMirror.setMigrationStageMessage("Table created in RIGHT cluster");
                    rtn = Boolean.TRUE;

                }

            } catch (SQLException throwables) {
                tblMirror.addIssue(throwables.getMessage());
                LOG.error("Issue", throwables);
            } catch (RuntimeException throwables) {
                tblMirror.addIssue(throwables.getMessage());
                LOG.error("Issue", throwables);
            } finally {
                try {
                    if (resultSet != null)
                        resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
            }
        }
        return rtn;
    }

    /*
    Import the table schema from a 'shell' export (transition schema) on the lower cluster
    then redirect to using data in the lower cluster
    */
    public Boolean importTransferSchemaUsingLowerData(Config config, DBMirror dbMirror, TableMirror tblMirror,
                                                      String exportBaseDirPrefix) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.RIGHT) {
            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String tableName = tblMirror.getName();

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                LOG.debug(useDb);
                tblMirror.addSql(useDb);
                stmt.execute(useDb);

                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {
                    // Get the definition for this environment.
                    List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Importing Table from " +
                            config.getCluster(Environment.LEFT).getHcfsNamespace() + exportBaseDirPrefix + dbMirror.getName() + "/" + tableName);

                    String importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE,
                            database, tableName,
                            config.getCluster(Environment.LEFT).getHcfsNamespace() + exportBaseDirPrefix + dbMirror.getName() + "/" + tableName);
                    LOG.debug(getEnvironment() + ":(SQL)" + importTransferSchema);

                    tblMirror.addSql(importTransferSchema);
                    if (config.isExecute()) {
                        stmt.execute(importTransferSchema);
                    } else {
                        tblMirror.addIssue("DRY-RUN: IMPORT table NOT run");
                    }
                    tblMirror.addAction("IMPORTED", Boolean.TRUE);

                    // Alter Table Data Location back to the original table.
                    // TODO: I think this triggers an msck....  need to verify
                    String originalLocation = TableUtils.getLocation(tableName, tblMirror.getTableDefinition(Environment.LEFT));
                    String alterTableLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, database, tableName, originalLocation);
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Altering table Location to original table in lower cluster");
                    LOG.debug(getEnvironment() + ":(SQL)" + alterTableLocSql);

                    tblMirror.addSql(alterTableLocSql);
                    if (config.isExecute()) {
                        stmt.execute(alterTableLocSql);
                    } else {
                        tblMirror.addIssue("DRY-RUN: ALTER Table location NOT run");
                    }
                    tblMirror.addAction("LOCATION Adjusted(to Original LEFT)", Boolean.TRUE);

                    String flagDate = df.format(new Date());
                    String hmsMirrorFlagStage1 = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                            MirrorConf.HMS_MIRROR_METADATA_FLAG, flagDate);
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                            ": Setting table property to identify as 'Stage-1' conversion.");
                    LOG.debug(getEnvironment() + "(SQL)" + hmsMirrorFlagStage1);

                    tblMirror.addSql(hmsMirrorFlagStage1);
                    if (config.isExecute()) {
                        stmt.execute(hmsMirrorFlagStage1);
                    } else {
                        tblMirror.addIssue("DRY-RUN: RIGHT table property not set: " + MirrorConf.HMS_MIRROR_METADATA_FLAG.toString());
                    }
                    tblMirror.addProp(MirrorConf.HMS_MIRROR_METADATA_FLAG + "=" + flagDate);

                    String hmsMirrorConverted = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                            MirrorConf.HMS_MIRROR_CONVERTED_FLAG, "true");
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                            ": Setting table property to identify as 'hms-mirror' converted");
                    LOG.debug(getEnvironment() + ":(SQL)" + hmsMirrorConverted);

                    tblMirror.addSql(hmsMirrorConverted);
                    if (config.isExecute()) {
                        stmt.execute(hmsMirrorConverted);
                    } else {
                        tblMirror.addIssue("DRY-RUN: RIGHT table property not set: " + MirrorConf.HMS_MIRROR_CONVERTED_FLAG.toString());
                    }
                    tblMirror.addProp(MirrorConf.HMS_MIRROR_CONVERTED_FLAG + "=true");

                    // Ensure the partition discovery matches our config directive
                    String dpProp = null;
                    if (config.getCluster(Environment.RIGHT).getPartitionDiscovery().getAuto()) {
                        dpProp = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(), MirrorConf.DISCOVER_PARTITIONS, "true");
                        tblMirror.addProp(MirrorConf.DISCOVER_PARTITIONS, "true");

                    } else {
                        dpProp = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(), MirrorConf.DISCOVER_PARTITIONS, "false");
                        tblMirror.addProp(MirrorConf.DISCOVER_PARTITIONS, "false");
                    }

                    tblMirror.addSql(dpProp);
                    if (config.isExecute()) {
                        stmt.execute(dpProp);
                    } else {
                        tblMirror.addIssue("DRY-RUN: RIGHT table properties not set: " + MirrorConf.DISCOVER_PARTITIONS.toString());
                    }


                    rtn = Boolean.TRUE;
                } else {
                    tblMirror.addIssue("Table exists before transfer and 'OVERWRITE' was not ON");
                }
            } catch (SQLException throwables) {
                LOG.error("Issue", throwables);
            } finally {
                try {
                    if (resultSet != null)
                        resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
                LOG.debug(getEnvironment().toString() + ":END:" + database + "." + tblMirror.getName());
            }
        }
        return rtn;
    }

    /*
    Import the table schema AND data from the LEFT cluster.  This is a migration of the schema
    and data from the lower cluster.
    */
    public Boolean importSchemaWithData(Config config, DBMirror dbMirror, TableMirror tblMirror,
                                        String exportBaseDirPrefix) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.RIGHT) {
            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String tableName = tblMirror.getName();

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                LOG.debug(useDb);
                tblMirror.addSql(useDb);
                stmt.execute(useDb);

                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {

                    // Get the definition for this environment.
                    List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Importing Table from " +
                            config.getCluster(Environment.LEFT).getHcfsNamespace() + exportBaseDirPrefix + dbMirror.getName() + "/" + tableName);

                    // Import for the location of the export on the lower cluster, to the same relative location
                    // on the upper cluster.
                    String importTransferSchema = null;
                    if (TableUtils.isACID(tableName, tblMirror.getTableDefinition(Environment.LEFT))) {
                        // Support ACID table migrations. Location here isn't relevant and will
                        // go to the default managed location.
                        importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_TABLE,
                                database, tableName,
                                config.getCluster(Environment.LEFT).getHcfsNamespace() + exportBaseDirPrefix +
                                        dbMirror.getName() + "/" + tableName);
                        tblMirror.addAction("ACID Table", Boolean.TRUE);
                    } else {
                        // Get Original Location
                        String originalLowerLocation = TableUtils.getLocation(tableName, tblMirror.getTableDefinition(Environment.LEFT));
                        String upperLocation = originalLowerLocation.replace(config.getCluster(Environment.LEFT).getHcfsNamespace(),
                                config.getCluster(Environment.RIGHT).getHcfsNamespace());
                        importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION,
                                database, tableName,
                                config.getCluster(Environment.LEFT).getHcfsNamespace() + exportBaseDirPrefix +
                                        dbMirror.getName() + "/" + tableName, upperLocation);
                    }
                    LOG.debug(getEnvironment() + ":(SQL)" + importTransferSchema);
                    tblMirror.setMigrationStageMessage("Import Schema w/Data.  This could take a while, be patient. Partition Count and Data Volume have an impact");

                    tblMirror.addSql(importTransferSchema);
                    if (config.isExecute()) {
                        stmt.execute(importTransferSchema);
                    } else {
                        tblMirror.addIssue("DRY-RUN: IMPORT transfer schema NOT run");
                    }
                    tblMirror.setMigrationStageMessage("Import Complete");
                    tblMirror.addAction("IMPORTED (w/data)", Boolean.TRUE);

                    if (TableUtils.isLegacyManaged(config.getCluster(Environment.LEFT), tblMirror.getName(),
                            tblMirror.getTableDefinition(Environment.LEFT))) {
                        String externalPurge = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                                MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                        LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                                ": Setting table property " + MirrorConf.EXTERNAL_TABLE_PURGE +
                                " because this table was a legacy Managed Table.");
                        LOG.debug(getEnvironment() + ":(SQL) " + externalPurge);

                        tblMirror.addSql(externalPurge);
                        if (config.isExecute()) {
                            stmt.execute(externalPurge);
                        } else {
                            tblMirror.addIssue("DRY-RUN: RIGHT table properties not set: " + MirrorConf.EXTERNAL_TABLE_PURGE);
                        }
                        tblMirror.addProp(MirrorConf.EXTERNAL_TABLE_PURGE, "true");

                    } else {
                        // TODO: RERUNNING THE PROCESS WILL BREAK BECAUSE THE TABLE DATA WON'T BE DROP WHEN THE TABLE
                        //         IS DROPPED, WHICH PREVENTS THE IMPORT FROM SUCCEEDING.
                    }

                    // Trigger Discover Partitions.
                    // for legacy managed tables
                    // if auto discovery flag set.
                    if (TableUtils.isLegacyManaged(this, tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT)) ||
                            config.getCluster(Environment.RIGHT).getPartitionDiscovery().getAuto()) {

                        String discoverPartitions = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                                MirrorConf.DISCOVER_PARTITIONS, "true");
                        LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                                ": Setting table property " + MirrorConf.DISCOVER_PARTITIONS +
                                " because this table was a legacy Managed Table or 'auto' discovery set in migration config.");
                        LOG.debug(getEnvironment() + ":(SQL) " + discoverPartitions);
                        tblMirror.setMigrationStageMessage("Adding 'discover.partitions' to table properties");

                        tblMirror.addSql(discoverPartitions);
                        if (config.isExecute()) {
                            stmt.execute(discoverPartitions);
                        } else {
                            tblMirror.addIssue("DRY-RUN: RIGHT table properties not set: " + MirrorConf.DISCOVER_PARTITIONS);
                        }
                        tblMirror.setMigrationStageMessage("'discover.partitions' table property added");
                        tblMirror.addProp(MirrorConf.DISCOVER_PARTITIONS, "true");
                    }

                    // Set IMPORT Flag Table Property
                    String flagDate = df.format(new Date());
                    String hmsMirrorImportFlag = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                            MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG, flagDate);
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                            ": Setting table property to identify as 'IMPORT' conversion.");
                    LOG.debug(getEnvironment() + ":(SQL)" + hmsMirrorImportFlag);
                    tblMirror.setMigrationStageMessage("Adding ' " + MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG + "' to table properties");

                    tblMirror.addSql(hmsMirrorImportFlag);
                    if (config.isExecute()) {
                        stmt.execute(hmsMirrorImportFlag);
                    } else {
                        tblMirror.addIssue("DRY-RUN: RIGHT table properties not set: " + MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG );
                    }
                    tblMirror.setMigrationStageMessage("'" + MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG + "' table property added");

                    tblMirror.addProp(MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG, flagDate);

                    // Set Converted Table Property
                    String hmsMirrorConverted = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                            MirrorConf.HMS_MIRROR_CONVERTED_FLAG, "true");
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                            ": Setting table property to identify as 'hms-mirror' converted");
                    LOG.debug(getEnvironment() + ":(SQL)" + hmsMirrorConverted);
                    tblMirror.setMigrationStageMessage("Adding ' " + MirrorConf.HMS_MIRROR_CONVERTED_FLAG + "' to table properties");

                    tblMirror.addSql(hmsMirrorConverted);
                    if (config.isExecute()) {
                        stmt.execute(hmsMirrorConverted);
                    } else {
                        tblMirror.addIssue("DRY-RUN: RIGHT table properties not set: " + MirrorConf.HMS_MIRROR_CONVERTED_FLAG);
                    }
                    tblMirror.setMigrationStageMessage("'" + MirrorConf.HMS_MIRROR_CONVERTED_FLAG + "' table property added");

                    tblMirror.addProp(MirrorConf.HMS_MIRROR_CONVERTED_FLAG, "true");

                    rtn = Boolean.TRUE;
                }
            } catch (SQLException throwables) {
                LOG.error("Issue", throwables);
                tblMirror.addIssue(throwables.getMessage());
            } finally {
                try {
                    if (resultSet != null)
                        resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
            }
        }
        return rtn;
    }

    //////////////////////////
    //   DATA MOVEMENT
    /////////////////////////

    /*

     */
    public Boolean doSqlDataTransfer(Config config, DBMirror dbMirror, TableMirror tblMirror, String transferPrefix) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.RIGHT) {
            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

//                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);
                LOG.debug(useDb);
                tblMirror.addSql(useDb);
                stmt.execute(useDb);

                String sqlDataTransfer = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER, tableName, transferPrefix + tableName);

                LOG.debug(getEnvironment() + ":(SQL)" + sqlDataTransfer);
                tblMirror.setMigrationStageMessage("Migrating data using Hive SQL.  Please wait. Check Hive Service for Job Details");

                tblMirror.addSql(sqlDataTransfer);
                if (config.isExecute()) {
                    stmt.execute(sqlDataTransfer);
                } else {
                    tblMirror.addIssue("DRY-RUN: SQL Transfer NOT run");
                }
                tblMirror.setMigrationStageMessage("Hive SQL Migration Complete");
                tblMirror.addAction("IMPORTED (via SQL)", Boolean.TRUE);
                rtn = Boolean.TRUE;
            } catch (SQLException throwables) {
                LOG.error("Issue", throwables);
                tblMirror.addIssue(throwables.getMessage());
            } catch (RuntimeException throwables) {
                LOG.error("Issue", throwables);
                tblMirror.addIssue(throwables.getMessage());
            } finally {
                try {
                    if (resultSet != null)
                        resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
                LOG.debug(getEnvironment().toString() + ":END:" + database + "." + tblMirror.getName());
            }
        }
        return rtn;
    }

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
    public Boolean completeSqlTransfer(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.RIGHT) {

            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String tableName = tblMirror.getName();

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);
                LOG.debug(useDb);
                tblMirror.addSql(useDb);
                stmt.execute(useDb);

                String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, database, tableName);

                LOG.debug(getEnvironment() + ":(SQL) " + dropTable);
                tblMirror.setMigrationStageMessage("Dropping original table (start of swap process)");

                tblMirror.addSql(dropTable);
                if (config.isExecute()) {
                    stmt.execute(dropTable);
                } else {
                    tblMirror.addIssue("DRY-RUN: Table NOT dropped");
                }
                tblMirror.setMigrationStageMessage("Table dropped");
                tblMirror.addAction(getEnvironment().toString(), "Table Dropped");

                String renameTable = MessageFormat.format(MirrorConf.RENAME_TABLE, config.getTransfer().getTransferPrefix() + tableName, tableName);
                LOG.debug(getEnvironment() + ":(SQL) " + renameTable);
                tblMirror.setMigrationStageMessage("Rename transfer table to original table (complete swap process)");
                tblMirror.addSql(renameTable);
                if (config.isExecute()) {
                    stmt.execute(renameTable);
                } else {
                    tblMirror.addIssue("DRY-RUN: Table rename NOT run");
                }
                tblMirror.setMigrationStageMessage("Rename complete");
                tblMirror.addAction(getEnvironment().toString(), "Transfer Renamed");

                rtn = Boolean.TRUE;
            } catch (SQLException throwables) {
                LOG.error("Issue", throwables);
                tblMirror.addIssue(throwables.getMessage());
            } catch (RuntimeException throwables) {
                LOG.error("Issue", throwables);
                tblMirror.addIssue(throwables.getMessage());
            } finally {
                try {
                    if (resultSet != null)
                        resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
                LOG.debug(getEnvironment().toString() + ":END:" + database + "." + tblMirror.getName());
            }
        }
        return rtn;
    }

    public Boolean partitionMaintenance(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        return partitionMaintenance(config, dbMirror, tblMirror, Boolean.FALSE);
    }

    /*
    Review the parameters and determine if we will issue an 'MSCK' on the table in the new cluster
     */
    public Boolean partitionMaintenance(Config config, DBMirror dbMirror, TableMirror tblMirror, Boolean force) {
        // Need to check if table is partitioned
        Boolean rtn = Boolean.TRUE;
        if (this.getEnvironment() == Environment.RIGHT &&
                TableUtils.isPartitioned(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT))) {

            String database = (getEnvironment() == Environment.LEFT ? dbMirror.getName() : config.getResolvedDB(dbMirror.getName()));

            LOG.debug(getEnvironment().toString() + ":START:" + database + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {

                String tableName = tblMirror.getName();

                if (this.getPartitionDiscovery().getInitMSCK() || force) {
                    conn = getConnection();
                    stmt = conn.createStatement();

                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                            ": Discovering " + tblMirror.getPartitionDefinition(Environment.LEFT).size() + " partitions");
                    String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, database, tblMirror.getName());
                    LOG.debug(getEnvironment() + ":(SQL)" + msckStmt);
                    tblMirror.setMigrationStageMessage("Discovering " + tblMirror.getPartitionDefinition(Environment.LEFT).size() + " partitions with 'MSCK'");
                    tblMirror.addSql(msckStmt);
                    if (config.isExecute()) {
                        stmt.execute(msckStmt);
                    } else {
                        tblMirror.addIssue("DRY-RUN: MSCK NOT run");
                    }
                    tblMirror.addAction("MSCK ran", Boolean.TRUE);
                    tblMirror.setMigrationStageMessage("Partition discovery with 'MSCK' complete");
                    rtn = Boolean.TRUE;
                }

                String msg;
                if (this.getPartitionDiscovery().getAuto()) {
                    if (this.getPartitionDiscovery().getInitMSCK()) {
                        msg = "This table has partitions and is set for 'auto' discovery via table property 'discover.partitions'='true'. " +
                                "You've requested an immediate 'MSCK' for the table, so the partitions will be current. " +
                                "For future partition discovery, ensure the Metastore is running the 'PartitionManagementTask' service.";
                    } else {
                        msg = "This table has partitions and is set for 'auto' discovery via table property 'discover.partitions'='true'. " +
                                "Ensure the Metastore is running the 'PartitionManagementTask' service.";
                    }
                } else {
                    if (this.getPartitionDiscovery().getInitMSCK()) {
                        msg = "You've requested an immediate 'MSCK' for the table, so the partitions will be current. " +
                                "Future partitions will NOT be auto discovered unless you manually add " +
                                "'discover.partitions'='true' to the tables properties. " +
                                "Once done, ensure the Metastore is running the 'PartitionManagementTask' service.";
                    } else {
                        msg = "This table has partitions and is NOT set for 'auto' discovery.  Data may not be available until the partition metadata is rebuilt manually.";
                    }
                }
                tblMirror.addIssue(msg);
                LOG.info(msg);

            } catch (SQLException throwables) {
                LOG.error("Issue", throwables);
                tblMirror.addIssue(throwables.getMessage());
                rtn = Boolean.FALSE;
            } catch (Throwable throwables) {
                throwables.printStackTrace();
            } finally {
                try {
                    if (resultSet != null)
                        resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
                LOG.debug(getEnvironment().toString() + ":END:" + database + "." + tblMirror.getName());
            }

        }
        return rtn;
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
