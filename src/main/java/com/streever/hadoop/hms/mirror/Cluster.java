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
//    private DistcpConfig distcp;

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

//    public DistcpConfig getDistcp() {
//        return distcp;
//    }
//
//    public void setDistcp(DistcpConfig distcp) {
//        this.distcp = distcp;
//    }

    public ConnectionPools getPools() {
        return pools;
    }

    public void setPools(ConnectionPools pools) {
        this.pools = pools;
    }

    protected Connection getConnection() throws SQLException {
        return pools.getEnvironmentConnection(getEnvironment());
    }

    public void getTables(Config config, DBMirror dbMirror) throws SQLException {
        Connection conn = null;
        try {
            conn = getConnection();

            LOG.debug(getEnvironment() + ":" + dbMirror.getName() + ": Loading tables for database");

            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                stmt = conn.createStatement();
                LOG.debug(getEnvironment() + ":" + dbMirror.getName() + ": Setting Hive DB Session Context");
                stmt.execute(MessageFormat.format(MirrorConf.USE, dbMirror.getName()));
                LOG.debug(getEnvironment() + ":" + dbMirror.getName() + ": Getting Table List");
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
        // Open the connection and ensure we are running this on the "UPPER" cluster.
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
                    LOG.debug(createDb);
                    if (!config.isDryrun())
                        stmt.execute(createDb);
                } catch (SQLException throwables) {
                    LOG.error("Creating DB", throwables);
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
                if (config.isOverwriteTable()) {
                    // Check if table exist or if we're working with a transfer table (in which case, drop)
                    if (tblMirror.getTableDefinition(Environment.UPPER) != null) {
                        LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Overwrite ON: Dropping table (if exists)");
                        tblMirror.addAction("Overwrite ON", Boolean.TRUE);
                        // Table Exists.
                        // Check that it was put there by 'hms-mirror'.  If not, we won't replace it.
                        if (TableUtils.isHMSConverted(tableName, tblMirror.getTableDefinition(Environment.UPPER))) {
                            // Check if the table has been converted to an ACID table.
                            if (config.getStage().equals(Stage.METADATA) && TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.UPPER))) {
                                String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Has been converted to " +
                                        "a 'transactional' table.  And isn't safe to drop before re-importing. " +
                                        "Review the table and remove manually before running process again.";
                                tblMirror.addIssue(errorMsg);
                                LOG.error(errorMsg);
                                // No need to continue.
                                rtn = Boolean.FALSE;
                            }
                            // Check if the table is "Managing" data.  If so, we can't safety drop the table without effecting
                            // the data.
                            if (!TableUtils.isHive3Standard(tableName, tblMirror.getTableDefinition(Environment.UPPER))) {
                                String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Is a 'Managed Non-Transactional' " +
                                        "which is a non-standard configuration in Hive 3.  Regardless, the data is " +
                                        "managed and it's not safe to proceed with converting this table. " +
                                        "Review the table and remove manually before running process again.";
                                tblMirror.addIssue(errorMsg);
                                LOG.error(errorMsg);
                                // No need to continue.
                                rtn = Boolean.FALSE;
                            }

                            // Ensure the table purge flag hasn't been set.
                            // ONLY prevent during METADATA stage.
                            // FOR the STORAGE stage, drop the table and data and import.
                            if (config.getStage().equals(Stage.METADATA) && TableUtils.isExternalPurge(tblMirror.getName(), tblMirror.getTableDefinition(Environment.UPPER))) {
                                String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Is a 'Purge' " +
                                        "enabled 'EXTERNAL' table.  The data is " +
                                        "managed and it's not safe to proceed with converting this table. " +
                                        "Review the table and remove manually before running process again.";
                                tblMirror.addIssue(errorMsg);
                                LOG.error(errorMsg);
                                // No need to continue.
                                rtn = Boolean.FALSE;
                            }

                            // Attempt to set a table property for current external tables that 'exist' and we're
                            // trying to replace during the STORAGE phase.
                            if (rtn && config.getStage().equals(Stage.STORAGE) &&
                                    !TableUtils.isExternalPurge(tblMirror.getName(),
                                            tblMirror.getTableDefinition(Environment.UPPER))) {
                                String externalPurge = MessageFormat.format(MirrorConf.ADD_TBL_PROP,
                                        database, tblMirror.getName(),
                                        MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                                if (!config.isDryrun())
                                    stmt.execute(externalPurge);
                                tblMirror.addIssue(getEnvironment().toString() + ":" +
                                        MirrorConf.EXTERNAL_TABLE_PURGE +
                                        "=true set to facilitate table/data drop for NEW IMPORT");
                            }
                        } else {
                            // It was NOT created by HMS Mirror.  The table will need to be manually dropped after review.
                            // No need to continue.
                            String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Was NOT originally " +
                                    "created by the 'hms-mirror' process and isn't safe to proceed. " +
                                    "The table will need to be reviewed and dropped manually and the process run again.";
                            tblMirror.addIssue(errorMsg);
                            LOG.error(errorMsg);
                            rtn = Boolean.FALSE;
                        }
                        if (rtn) {
                            LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Dropping table");
                            // It was created by HMS Mirror, safe to drop.
                            String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, database, tableName);
                            LOG.debug(getEnvironment() + ":(SQL)" + dropTable);
                            tblMirror.setMigrationStageMessage("Dropping table (overwrite)");

                            if (!config.isDryrun())
                                stmt.execute(dropTable);
                            // TODO: change current to address tablename so we pickup transfer.
                            tblMirror.setMigrationStageMessage("Table dropped");
                            tblMirror.addAction(getEnvironment().toString(), "DROPPED");
                        }
                    }

                } else {
                    if (tblMirror.getTableDefinition(Environment.UPPER) != null) {
                        // Overwrite is not on and table exists.  Skip.
                        String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Exists already " +
                                "and the 'overwrite' flag isn't set.  This import was NOT done. ";
                        tblMirror.addIssue(errorMsg);
                        LOG.error(errorMsg);
                        // No need to continue.
                        rtn = Boolean.FALSE;
                    }
                }
            } else {
                // Always drop the transfer table.
                if (rtn) {
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Dropping table");
                    // It was created by HMS Mirror, safe to drop.
                    String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, database, tableName);
                    LOG.debug(getEnvironment() + ":(SQL)" + dropTable);
                    tblMirror.setMigrationStageMessage("Dropping table (overwrite)");

                    if (!config.isDryrun())
                        stmt.execute(dropTable);
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

        String database = dbMirror.getName();
        String tableName = tblMirror.getName();
        LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
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

            if (!config.isDryrun())
                stmt.execute(dropTransferTable);
            tblMirror.addAction("TRANSFER(LOWER) Schema", "Built");

            String transferCreateTable = MessageFormat.format(MirrorConf.CREATE_EXTERNAL_LIKE,
                    transferDatabase, tblMirror.getName(),
                    dbMirror.getName(), tblMirror.getName());

            LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                    ": Creating transfer schema for table");
            LOG.debug(getEnvironment() + ":(SQL)" + transferCreateTable);

            if (!config.isDryrun())
                stmt.execute(transferCreateTable);
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

                if (!config.isDryrun())
                    stmt.execute(legacyFlag);
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
                    tableName + ": Exporting Table to " + config.getCluster(Environment.LOWER).hcfsNamespace + exportBaseDirPrefix + database + "/" + tableName);
            String exportTransferSchema = MessageFormat.format(MirrorConf.EXPORT_TABLE,
                    database, tableName,
                    config.getCluster(Environment.LOWER).hcfsNamespace + exportBaseDirPrefix + database + "/" + tableName);
            LOG.debug(getEnvironment() + ":(SQL)" + exportTransferSchema);
            tblMirror.setMigrationStageMessage("Export Schema");
            if (!config.isDryrun())
                stmt.execute(exportTransferSchema);
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
    where we push all data to in the SQL method.  The data will be source by the UPPER clusters shadow
    table (with original name) in the UPPER cluster, looking at the data in the lower cluster.
    This transfer table should use the UPPER cluster for storage and the location is the same (relative) as
    where the data was in the LOWER cluster.
    Once the transfer is complete, the two tables can be swapped in a move that is quick.
    */
    public Boolean buildUpperTransferTable(Config config, DBMirror dbMirror, TableMirror tblMirror, String transferPrefix) {
        // Open the connection and ensure we are running this on the "UPPER" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.UPPER) {
            LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                LOG.debug(useDb);
                if (!config.isDryrun())
                    stmt.execute(useDb);

                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror, transferPrefix)) {

                    Boolean buildUpper = tblMirror.buildUpperSchema(config, Boolean.FALSE);

                    TableUtils.changeLocationNamespace(tblMirror.getName(), tblMirror.getTableDefinition(Environment.UPPER),
                            config.getCluster(Environment.LOWER).getHcfsNamespace(),
                            config.getCluster(Environment.UPPER).getHcfsNamespace());

                    if (TableUtils.isHMSLegacyManaged(this, tableName, tblMirror.getTableDefinition(Environment.UPPER))) {
                        tblMirror.setMigrationStageMessage("Adding EXTERNAL purge to table properties");
                        TableUtils.upsertTblProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true", tblMirror.getTableDefinition(Environment.UPPER));
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
                    if (!config.isDryrun()) {
                        stmt.execute(createTable);
                    }
                    tblMirror.setMigrationStageMessage("Table created");

                    tblMirror.addAction("Transfer Schema Created", Boolean.TRUE);

//                    // Need to check if table is partitioned
//                    if (TableUtils.isPartitioned(tableName, tblMirror.getTableDefinition(Environment.LOWER))) {
//
//                        if (this.getPartitionDiscovery().getInitMSCK()) {
//                            LOG.debug(getEnvironment() + ":" + database + "." + tableName +
//                                    ": Discovering " + tblMirror.getPartitionDefinition(Environment.LOWER).size() + " partitions");
//                            String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, database, tblMirror.getName());
//                            LOG.debug(getEnvironment() + ":(SQL)" + msckStmt);
//                            if (!config.isDryrun())
//                                stmt.execute(msckStmt);
//                            tblMirror.addAction("MSCK ran", Boolean.TRUE);
//                        } else {
//                            String msg = getEnvironment() + ":" + database + "." + tableName + ": Has " + tblMirror.getPartitionDefinition(Environment.LOWER).size() +
//                                    " partitions.  Data may not be available until the partition metadata is rebuilt.";
//                            tblMirror.addIssue(msg);
//                            LOG.info(msg);
//                        }
//                    }
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
        // Open the connection and ensure we are running this on the "UPPER" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.UPPER) {
            LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Importing Transfer Schema");

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                LOG.debug(useDb);
                if (!config.isDryrun())
                    stmt.execute(useDb);

                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {
                    Boolean buildUpper = tblMirror.buildUpperSchema(config, Boolean.FALSE);

                    // Turn off for the create/alter
//                    TableUtils.upsertTblProperty(MirrorConf.DISCOVER_PARTITIONS, "false",
//                            tblMirror.getTableDefinition(Environment.UPPER));

                    // WHEN BUILDING UPPER SCHEMA, WE ARE GOING TO USE THE UPPER CLUSTER LOCATION
                    // FIRST WHILE CREATING THE TABLE.  THEN SWITCH IT TO THE LOWER AND RUN MSCK.
                    // Adjust the Location to be Relative to the UPPER cluster.
//                    TableUtils.changeLocationNamespace(dbMirror.getName(), tblMirror.getTableDefinition(Environment.UPPER),
//                            config.getCluster(Environment.LOWER).getHcfsNamespace(),
//                            config.getCluster(Environment.UPPER).getHcfsNamespace());

                    // Get the Create State for this environment.
                    String createTable = tblMirror.getCreateStatement(this.getEnvironment());
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ":(SQL)" + createTable);
                    tblMirror.setMigrationStageMessage("Creating Table in UPPER cluster");
                    if (tblMirror.isPartitioned(Environment.LOWER)) {
                        tblMirror.addIssue("If CREATE/ALTER is slow for migration, add `ranger.plugin.hive.urlauth.filesystem.schemes=file`" +
                                " to the HS2 hive-ranger-security.xml configurations.");
                    }
                    if (!config.isDryrun())
                        stmt.execute(createTable);

                    // ALTER TABLE TO USE LOWER LOCATION.
//                    String originalLocation = TableUtils.getLocation(tableName, tblMirror.getTableDefinition(Environment.LOWER));
//                    String alterTableLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, database, tableName, originalLocation);
//                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Altering table Location to original table in lower cluster");
//                    LOG.debug(getEnvironment() + ":(SQL)" + alterTableLocSql);
//                    tblMirror.setMigrationStageMessage("Altering table location to LOWER cluster");
//                    if (!config.isDryrun())
//                        stmt.execute(alterTableLocSql);

                    tblMirror.addAction("Shadow(using LOWER data) Schema Create", Boolean.TRUE);
                    tblMirror.setMigrationStageMessage("Table created in UPPER cluster");

                    rtn = Boolean.TRUE;

                }

                // TODO: Wrong place for this.  Need a SWAP action for the SQL Data Import process in Storage.
//                String originalLocation = TableUtils.getLocation(tableName, tblMirror.getTableDefinition(Environment.LOWER));
//                String alterTableLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, database, tableName, originalLocation);
//                LOG.info(getEnvironment() + ":" + database + "." + tableName + ": Altering table Location to original table in lower cluster");
//                LOG.debug(getEnvironment() + ":(SQL)" + alterTableLocSql);
//                try {
//                    if (!config.isDryrun())
//                        stmt.execute(alterTableLocSql);
//                    tblMirror.addAction("Location adjusted", Boolean.TRUE);
//                    LOG.info(getEnvironment() + ":" + database + "." + tableName + ": Altering table Location Complete.");
//                } catch (SQLException throwables) {
//                    String errorMsg = getEnvironment() + ":" + database + "." + tableName +
//                            ": Issue adjusting table location, data may not be available for new table";
//                    tblMirror.addIssue(errorMsg + ": " + throwables.getMessage());
//                    LOG.error(errorMsg, throwables);
//                }

//                // Need to check if table is partitioned
//                if (TableUtils.isPartitioned(tableName, tblMirror.getTableDefinition(Environment.LOWER))) {
//                    if (this.getPartitionDiscovery().getInitMSCK()) {
//                        LOG.info(getEnvironment() + ":" + database + "." + tableName +
//                                ": Discovering " + tblMirror.getPartitionDefinition(Environment.LOWER).size() + " partitions");
//                        String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, database, tblMirror.getName());
//                        LOG.debug(getEnvironment() + ":(SQL)" + msckStmt);
//                        try {
//                            if (!config.isDryrun())
//                                stmt.execute(msckStmt);
//                            tblMirror.addAction("MSCK ran", Boolean.TRUE);
//                            LOG.info(getEnvironment() + ":" + database + "." + tableName +
//                                    ": Partitions Discovered");
//                        } catch (SQLException throwables) {
//                            String errorMsg = getEnvironment() + ":" + database + "." + tableName +
//                                    ": Issue discovering partitions, data may not be available for new table";
//                            tblMirror.addIssue(errorMsg + ": " + throwables.getMessage());
//                            LOG.error(errorMsg, throwables);
//                        }
//                    } else if (this.getPartitionDiscovery().getAuto()) {
//                        String msg = getEnvironment() + ":" + database + "." + tableName + ": is set to " +
//                                "'discovery.partitions'. Ensure 'metastore.housekeeping.threads.on=true' is set for " +
//                                "the metastore and the metastore 'metastore.partition.management.task.thread.pool.size' is " +
//                                "sufficient for processing this request.  Default settings may take 5 mins. before " +
//                                "partition discovery is started. Check back later for partitions.";
//                        tblMirror.addIssue(msg);
//                        LOG.info(msg);
//                    } else {
//                        String msg = getEnvironment() + ":" + database + "." + tableName + ": Has " + tblMirror.getPartitionDefinition(Environment.LOWER).size() +
//                                " partitions.  Data may not be available until the partition metadata is rebuilt.";
//                        tblMirror.addIssue(msg);
//                        LOG.info(msg);
//                    }
//                }
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


    public Boolean buildUpperSchemaWithRelativeData(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        // Open the connection and ensure we are running this on the "UPPER" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.UPPER) {
            LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Build Upper Schema");

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                LOG.debug(useDb);
                if (!config.isDryrun())
                    stmt.execute(useDb);

                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {
                    Boolean buildUpper = tblMirror.buildUpperSchema(config, Boolean.TRUE);

                    // Adjust the Location to be Relative to the UPPER cluster.
                    TableUtils.changeLocationNamespace(dbMirror.getName(), tblMirror.getTableDefinition(Environment.UPPER),
                            config.getCluster(Environment.LOWER).getHcfsNamespace(),
                            config.getCluster(Environment.UPPER).getHcfsNamespace());

                    if (!TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.UPPER))) {
                        if (config.getCluster(Environment.UPPER).getPartitionDiscovery().getAuto())
                            TableUtils.upsertTblProperty(MirrorConf.DISCOVER_PARTITIONS, "true",
                                    tblMirror.getTableDefinition(Environment.UPPER));
                        else
                            TableUtils.upsertTblProperty(MirrorConf.DISCOVER_PARTITIONS, "false",
                                    tblMirror.getTableDefinition(Environment.UPPER));
                    }

                    // Get the Create State for this environment.
                    String createTable = tblMirror.getCreateStatement(this.getEnvironment());
                    LOG.debug(getEnvironment() + ":(SQL)" + createTable);
                    tblMirror.setMigrationStageMessage("Creating Table in UPPER cluster");
                    if (!config.isDryrun())
                        stmt.execute(createTable);
                    tblMirror.addAction("Upper Schema Create", Boolean.TRUE);
                    tblMirror.setMigrationStageMessage("Table created in UPPER cluster");
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
        // Open the connection and ensure we are running this on the "UPPER" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.UPPER) {
            LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String database = dbMirror.getName();
                String tableName = tblMirror.getName();


                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                LOG.debug(useDb);
                if (!config.isDryrun())
                    stmt.execute(useDb);

                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {
                    // Get the definition for this environment.
                    List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Importing Table from " +
                            config.getCluster(Environment.LOWER).getHcfsNamespace() + exportBaseDirPrefix + database + "/" + tableName);

                    String importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE,
                            database, tableName,
                            config.getCluster(Environment.LOWER).getHcfsNamespace() + exportBaseDirPrefix + database + "/" + tableName);
                    LOG.debug(getEnvironment() + ":(SQL)" + importTransferSchema);

                    if (!config.isDryrun())
                        stmt.execute(importTransferSchema);
                    tblMirror.addAction("IMPORTED", Boolean.TRUE);

                    // Alter Table Data Location back to the original table.
                    // TODO: I think this triggers an msck....  need to verify
                    String originalLocation = TableUtils.getLocation(tableName, tblMirror.getTableDefinition(Environment.LOWER));
                    String alterTableLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, database, tableName, originalLocation);
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Altering table Location to original table in lower cluster");
                    LOG.debug(getEnvironment() + ":(SQL)" + alterTableLocSql);

                    if (!config.isDryrun())
                        stmt.execute(alterTableLocSql);
                    tblMirror.addAction("LOCATION Adjusted(to Original LOWER)", Boolean.TRUE);

                    String flagDate = df.format(new Date());
                    String hmsMirrorFlagStage1 = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                            MirrorConf.HMS_MIRROR_METADATA_FLAG, flagDate);
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                            ": Setting table property to identify as 'Stage-1' conversion.");
                    LOG.debug(getEnvironment() + "(SQL)" + hmsMirrorFlagStage1);

                    if (!config.isDryrun())
                        stmt.execute(hmsMirrorFlagStage1);
                    tblMirror.addProp(MirrorConf.HMS_MIRROR_METADATA_FLAG + "=" + flagDate);

                    String hmsMirrorConverted = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                            MirrorConf.HMS_MIRROR_CONVERTED_FLAG, "true");
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                            ": Setting table property to identify as 'hms-mirror' converted");
                    LOG.debug(getEnvironment() + ":(SQL)" + hmsMirrorConverted);

                    if (!config.isDryrun())
                        stmt.execute(hmsMirrorConverted);
                    tblMirror.addProp(MirrorConf.HMS_MIRROR_CONVERTED_FLAG + "=true");

                    // Ensure the partition discovery matches our config directive
                    String dpProp = null;
                    if (config.getCluster(Environment.UPPER).getPartitionDiscovery().getAuto()) {
                        dpProp = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(), MirrorConf.DISCOVER_PARTITIONS, "true");
                        tblMirror.addProp(MirrorConf.DISCOVER_PARTITIONS, "true");

                    } else {
                        dpProp = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(), MirrorConf.DISCOVER_PARTITIONS, "false");
                        tblMirror.addProp(MirrorConf.DISCOVER_PARTITIONS, "false");
                    }

                    if (!config.isDryrun())
                        stmt.execute(dpProp);


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
                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
            }
        }
        return rtn;
    }

    /*
    Import the table schema AND data from the LOWER cluster.  This is a migration of the schema
    and data from the lower cluster.
    */
    public Boolean importSchemaWithData(Config config, DBMirror dbMirror, TableMirror tblMirror,
                                        String exportBaseDirPrefix) {
        // Open the connection and ensure we are running this on the "UPPER" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.UPPER) {
            LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);

                LOG.debug(useDb);
                if (!config.isDryrun())
                    stmt.execute(useDb);

                if (checkAndDoOverwrite(stmt, config, dbMirror, tblMirror)) {

                    // Get the definition for this environment.
                    List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Importing Table from " +
                            config.getCluster(Environment.LOWER).getHcfsNamespace() + exportBaseDirPrefix + database + "/" + tableName);

                    // Import for the location of the export on the lower cluster, to the same relative location
                    // on the upper cluster.
                    String importTransferSchema = null;
                    if (TableUtils.isACID(tableName, tblMirror.getTableDefinition(Environment.LOWER))) {
                        // Support ACID table migrations. Location here isn't relevant and will
                        // go to the default managed location.
                        importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_TABLE,
                                database, tableName,
                                config.getCluster(Environment.LOWER).getHcfsNamespace() + exportBaseDirPrefix +
                                        database + "/" + tableName);
                        tblMirror.addAction("ACID Table", Boolean.TRUE);
                    } else {
                        // Get Original Location
                        String originalLowerLocation = TableUtils.getLocation(tableName, tblMirror.getTableDefinition(Environment.LOWER));
                        String upperLocation = originalLowerLocation.replace(config.getCluster(Environment.LOWER).getHcfsNamespace(),
                                config.getCluster(Environment.UPPER).getHcfsNamespace());
                        importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION,
                                database, tableName,
                                config.getCluster(Environment.LOWER).getHcfsNamespace() + exportBaseDirPrefix +
                                        database + "/" + tableName, upperLocation);
                    }
                    LOG.debug(getEnvironment() + ":(SQL)" + importTransferSchema);
                    tblMirror.setMigrationStageMessage("Import Schema w/Data.  This could take a while, be patient. Partition Count and Data Volume have an impact");

                    if (!config.isDryrun())
                        stmt.execute(importTransferSchema);
                    tblMirror.setMigrationStageMessage("Import Complete");
                    tblMirror.addAction("IMPORTED (w/data)", Boolean.TRUE);

                    if (TableUtils.isLegacyManaged(config.getCluster(Environment.LOWER), tblMirror.getName(),
                            tblMirror.getTableDefinition(Environment.LOWER))) {
                        String externalPurge = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                                MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                        LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                                ": Setting table property " + MirrorConf.EXTERNAL_TABLE_PURGE +
                                " because this table was a legacy Managed Table.");
                        LOG.debug(getEnvironment() + ":(SQL) " + externalPurge);

                        if (!config.isDryrun())
                            stmt.execute(externalPurge);
                        tblMirror.addProp(MirrorConf.EXTERNAL_TABLE_PURGE, "true");

                    } else {
                        // TODO: RERUNNING THE PROCESS WILL BREAK BECAUSE THE TABLE DATA WON'T BE DROP WHEN THE TABLE
                        //         IS DROPPED, WHICH PREVENTS THE IMPORT FROM SUCCEEDING.
                    }

                    // Trigger Discover Partitions.
                    // for legacy managed tables
                    // if auto discovery flag set.
                    if (TableUtils.isLegacyManaged(this, tblMirror.getName(), tblMirror.getTableDefinition(Environment.LOWER)) ||
                            config.getCluster(Environment.UPPER).getPartitionDiscovery().getAuto()) {

                        String discoverPartitions = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                                MirrorConf.DISCOVER_PARTITIONS, "true");
                        LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                                ": Setting table property " + MirrorConf.DISCOVER_PARTITIONS +
                                " because this table was a legacy Managed Table or 'auto' discovery set in migration config.");
                        LOG.debug(getEnvironment() + ":(SQL) " + discoverPartitions);
                        tblMirror.setMigrationStageMessage("Adding 'discover.partitions' to table properties");

                        if (!config.isDryrun())
                            stmt.execute(discoverPartitions);
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

                    if (!config.isDryrun())
                        stmt.execute(hmsMirrorImportFlag);
                    tblMirror.setMigrationStageMessage("'" + MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG + "' table property added");

                    tblMirror.addProp(MirrorConf.HMS_MIRROR_STORAGE_IMPORT_FLAG, flagDate);

                    // Set Converted Table Property
                    String hmsMirrorConverted = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                            MirrorConf.HMS_MIRROR_CONVERTED_FLAG, "true");
                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                            ": Setting table property to identify as 'hms-mirror' converted");
                    LOG.debug(getEnvironment() + ":(SQL)" + hmsMirrorConverted);
                    tblMirror.setMigrationStageMessage("Adding ' " + MirrorConf.HMS_MIRROR_CONVERTED_FLAG + "' to table properties");

                    if (!config.isDryrun())
                        stmt.execute(hmsMirrorConverted);
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
        // Open the connection and ensure we are running this on the "UPPER" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.UPPER) {
            LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);
                LOG.debug(useDb);
                if (!config.isDryrun())
                    stmt.execute(useDb);

                String sqlDataTransfer = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER, tableName, transferPrefix + tableName);

                LOG.debug(getEnvironment() + ":(SQL)" + sqlDataTransfer);
                tblMirror.setMigrationStageMessage("Migrating data using Hive SQL.  Please wait. Check Hive Service for Job Details");
                if (!config.isDryrun())
                    stmt.execute(sqlDataTransfer);
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
                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
            }
        }
        return rtn;
    }

    /*
    Prior to this process the following should be true:
     - A transition table has been create and points to the same 'relative' location on the NEW cluster as the LOWER cluster
     - An original table has been create on the UPPER cluster and points to the LOWER dataset.  This table is 'external' and 'non' purgeable.
     - A SQL data transfer has happened to move the data into the 'transfer' table.
     - The transfer table has been set to match the legacy managed behavior ('external.table.purge')

     In this process, we will:
      - "DROP" the 'original' table that points to the LOWER cluster
      - RENAME the transfer table to the 'original' table name.
     */
    public Boolean completeSqlTransfer(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        // Open the connection and ensure we are running this on the "UPPER" cluster.
        Boolean rtn = Boolean.FALSE;
        if (this.getEnvironment() == Environment.UPPER) {
            LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                stmt = conn.createStatement();

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);
                LOG.debug(useDb);
                if (!config.isDryrun())
                    stmt.execute(useDb);

                String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, database, tableName);

                LOG.debug(getEnvironment() + ":(SQL) " + dropTable);
                tblMirror.setMigrationStageMessage("Dropping original table (start of swap process)");
                if (!config.isDryrun())
                    stmt.execute(dropTable);
                tblMirror.setMigrationStageMessage("Table dropped");
                tblMirror.addAction(getEnvironment().toString(), "Table Dropped");

                String renameTable = MessageFormat.format(MirrorConf.RENAME_TABLE, config.getMetadata().getTransferPrefix() + tableName, tableName);
                LOG.debug(getEnvironment() + ":(SQL) " + renameTable);
                tblMirror.setMigrationStageMessage("Rename transfer table to original table (complete swap process)");
                if (!config.isDryrun())
                    stmt.execute(renameTable);
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
                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
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
        if (this.getEnvironment() == Environment.UPPER &&
                TableUtils.isPartitioned(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LOWER))) {
            LOG.debug(getEnvironment().toString() + ":START:" + dbMirror.getName() + "." + tblMirror.getName());
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {

                String database = dbMirror.getName();
                String tableName = tblMirror.getName();

                if (this.getPartitionDiscovery().getInitMSCK() || force) {
                    conn = getConnection();
                    stmt = conn.createStatement();

                    LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                            ": Discovering " + tblMirror.getPartitionDefinition(Environment.LOWER).size() + " partitions");
                    String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, database, tblMirror.getName());
                    LOG.debug(getEnvironment() + ":(SQL)" + msckStmt);
                    tblMirror.setMigrationStageMessage("Discovering partitions with 'MSCK'");
                    if (!config.isDryrun())
                        stmt.execute(msckStmt);
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
                LOG.debug(getEnvironment().toString() + ":END:" + dbMirror.getName() + "." + tblMirror.getName());
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
