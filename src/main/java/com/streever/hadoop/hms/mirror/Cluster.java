package com.streever.hadoop.hms.mirror;

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
import java.util.Set;

public class Cluster implements Comparable<Cluster> {
    private static Logger LOG = LogManager.getLogger(Cluster.class);
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private ConnectionPools pools = null;

    private Environment environment = null;
    private Boolean legacyHive = Boolean.TRUE;
    private String hcfsNamespace = null;
    private String jarFile = null;
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

    public String getJarFile() {
        return jarFile;
    }

    public void setJarFile(String jarFile) {
        this.jarFile = jarFile;
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
        return pools.getEnvironmentConnection(getEnvironment());
    }

    public void getTables(DBMirror dbMirror) throws SQLException {
        Connection conn = null;
        try {
            conn = getConnection();

            LOG.debug(getEnvironment() + ":" + dbMirror.getDatabase() + ": Loading tables for database");

            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                stmt = conn.createStatement();
                LOG.debug(getEnvironment() + ":" + dbMirror.getDatabase() + ": Setting Hive DB Session Context");
                stmt.execute(MessageFormat.format(MirrorConf.USE, dbMirror.getDatabase()));
                LOG.debug(getEnvironment() + ":" + dbMirror.getDatabase() + ": Getting Table List");
                resultSet = stmt.executeQuery(MirrorConf.SHOW_TABLES);
                while (resultSet.next()) {
                    TableMirror tableMirror = dbMirror.addTable(resultSet.getString(1));
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

    public void getTableDefinition(String database, TableMirror tableMirror) throws SQLException {
        // The connection should already be in the database;
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
                if (TableUtils.isACID(tableMirror.getName(), tblDef)) {
                    tableMirror.setTransactional(Boolean.TRUE);
                    tableMirror.addIssue("Transactional/ACID tables are NOT currently supported by 'hms-mirror'");
                }

                Boolean partitioned = TableUtils.isPartitioned(tableMirror.getName(), tblDef);
                tableMirror.setPartitioned(getEnvironment(), partitioned);
                if (partitioned) {
                    loadTablePartitionMetadata(database, tableMirror);
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

    public void buildTransferTableSchema(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        Connection conn = null;
        Statement stmt = null;
        String database = dbMirror.getDatabase();
        String tableName = tblMirror.getName();
        try {
            conn = getConnection();

            LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                    ": Building Transfer Schema");
            try {
                stmt = conn.createStatement();
            } catch (SQLException throwables) {
                String msg = getEnvironment() + ":" + database + "." + tableName +
                        ": Issue building statement";
                LOG.error(msg, throwables);
                tblMirror.addIssue(msg + ": " + throwables.getMessage());
                return;
            }

            // Get the definition for this environment.
            List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());

            String dropTransferTable = MessageFormat.format(MirrorConf.DROP_TABLE,
                    config.getTransferDbPrefix() + database, tableName);

            LOG.debug(getEnvironment() + ":" + config.getTransferDbPrefix() + database + "." + tableName +
                    ": Dropping existing transfer schema (if exists) for table: ");
            LOG.debug(getEnvironment() + "(SQL)" + dropTransferTable);
            try {
                if (!config.isDryrun())
                    stmt.execute(dropTransferTable);
            } catch (SQLException throwables) {
                String msg = getEnvironment() + ":" + config.getTransferDbPrefix() + database + "." + tableName +
                        ": Issue dropping existing transfer schema";
                LOG.error(msg, throwables);
                tblMirror.addIssue(msg + ": " + throwables.getMessage());
                return;
            }

            String transferCreateTable = MessageFormat.format(MirrorConf.CREATE_EXTERNAL_LIKE,
                    config.getTransferDbPrefix() + dbMirror.getDatabase(), tblMirror.getName(),
                    dbMirror.getDatabase(), tblMirror.getName());

            LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                    ": Creating transfer schema for table");
            LOG.debug(getEnvironment() + "(SQL)" + transferCreateTable);
            try {
                if (!config.isDryrun())
                    stmt.execute(transferCreateTable);
                tblMirror.setTransitionCreated(Boolean.TRUE);
            } catch (SQLException throwables) {
                String msg = getEnvironment() + ":" + database + "." + tableName +
                        ": Issue creating transfer schema for table";
                LOG.error(msg, throwables);
                tblMirror.addIssue(msg + ": " + throwables.getMessage());
                return;
            }

            // Determine if it's a Legacy Managed Table.
            LOG.debug(getEnvironment() + ":" + dbMirror.getDatabase() + "." + tblMirror.getName() +
                    ": Checking Legacy Configuration of table");

            Boolean legacyManaged = TableUtils.isLegacyManaged(this, tblMirror.getName(), tblDef);
            if (legacyManaged) {
                String legacyFlag = MessageFormat.format(MirrorConf.ADD_TBL_PROP,
                        config.getTransferDbPrefix() + dbMirror.getDatabase(), tblMirror.getName(),
                        MirrorConf.LEGACY_MANAGED_FLAG, Boolean.TRUE.toString()
                );
                LOG.debug(getEnvironment() + ":" + config.getTransferDbPrefix() + database + "." + tableName +
                        ": Setting Legacy flag for table");
                LOG.debug(getEnvironment() + "(SQL)" + legacyFlag);
                try {
                    if (!config.isDryrun())
                        stmt.execute(legacyFlag);
                    tblMirror.addProp(MirrorConf.LEGACY_MANAGED_FLAG + "=" + Boolean.TRUE.toString());
                } catch (SQLException throwables) {
                    String msg = getEnvironment() + ":" + config.getTransferDbPrefix() + database + "." + tableName +
                            ": Issue setting legacy flag for table";
                    LOG.error(msg, throwables);
                    tblMirror.addIssue(msg + ": " + throwables.getMessage());
                    return;
                }
            }
        } catch (SQLException throwables) {
            String msg = "Issue";
            LOG.error(msg, throwables);
            tblMirror.addIssue(msg + ": " + throwables.getMessage());
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
        }
    }

    public void exportTransferSchema(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            String database = dbMirror.getDatabase();
            String tableName = tblMirror.getName();

            LOG.debug(getEnvironment() + ":" + config.getTransferDbPrefix() + database + "." + tableName +
                    ": Exporting Transfer Schema");

            try {
                stmt = conn.createStatement();
            } catch (SQLException throwables) {
                String msg = getEnvironment() + ":" + config.getTransferDbPrefix() + database + "." + tableName +
                        ": Issue building statement";
                LOG.error(msg, throwables);
                tblMirror.addIssue(msg + ": " + throwables.getMessage());
                return;
            }

            // Get the definition for this environment.
            List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
            LOG.debug(getEnvironment() + ":" + config.getTransferDbPrefix() + database + "." +
                    tableName + ": Exporting Table to " + config.getCluster(Environment.LOWER).hcfsNamespace + config.getExportBaseDirPrefix() + database + "/" + tableName);
            String exportTransferSchema = MessageFormat.format(MirrorConf.EXPORT_TABLE,
                    config.getTransferDbPrefix() + database, tableName,
                    config.getCluster(Environment.LOWER).hcfsNamespace + config.getExportBaseDirPrefix() + database + "/" + tableName);
            LOG.debug(getEnvironment() + "(SQL)" + exportTransferSchema);
            try {
                if (!config.isDryrun())
                    stmt.execute(exportTransferSchema);
                tblMirror.setExportCreated(Boolean.TRUE);
            } catch (SQLException throwables) {
                String msg = getEnvironment() + ":" + config.getTransferDbPrefix() + database + "." +
                        tableName + ": Issue Exporting table (Check Location Permissions for User running command)";
                LOG.error(msg, throwables);
                tblMirror.addIssue(msg + ": " + throwables.getMessage());
            }
        } catch (SQLException throwables) {
            String msg = "Issue ";
            LOG.error(msg, throwables);
            tblMirror.addIssue(msg + ": " + throwables.getMessage());
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
        }
    }

    public void createDatabase(Config config, String database) {
        // Open the connection and ensure we are running this on the "UPPER" cluster.
        Connection conn = null;
        try {
            conn = getConnection();


            LOG.debug(getEnvironment() + " - Create Database: " +
                    database);

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

    public void importTransferSchema(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        // Open the connection and ensure we are running this on the "UPPER" cluster.
        if (this.getEnvironment() == Environment.UPPER) {
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                conn = getConnection();

                String database = dbMirror.getDatabase();
                String tableName = tblMirror.getName();

                LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Importing Transfer Schema");

                try {
                    stmt = conn.createStatement();
                } catch (SQLException throwables) {
                    LOG.error("Issue building statement", throwables);
                    tblMirror.addIssue("Issue building statement: " + throwables.getMessage());
                    return;
                }

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);
                try {
                    LOG.debug(useDb);
                    if (!config.isDryrun())
                        stmt.execute(useDb);
                } catch (SQLException throwables) {
                    LOG.error("USE DB", throwables);
                    tblMirror.addIssue("(SQL)" + useDb + throwables.getMessage());
                    return;
                }

                // Check if we should overwrite existing table.
                if (config.isOverwriteTable()) {
                    // Check if table exist.
                    if (tblMirror.getTableDefinition(Environment.UPPER) != null) {
                        LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Overwrite ON: Dropping table (if exists)");
                        tblMirror.setOverwrite(Boolean.TRUE);
                        // Table Exists.
                        // Check that it was put there by 'hms-mirror'.  If not, we won't replace it.
                        if (TableUtils.isHMSConverted(tblMirror.getName(), tblMirror.getTableDefinition(Environment.UPPER))) {
                            // Check if the table has been converted to an ACID table.
                            if (TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.UPPER))) {
                                String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Has been converted to " +
                                        "a 'transactional' table.  And isn't safe to drop before re-importing. " +
                                        "Review the table and remove manually before running process again.";
                                tblMirror.addIssue(errorMsg);
                                LOG.error(errorMsg);
                                // No need to continue.
                                return;
                            }
                            // Check if the table is "Managing" data.  If so, we can't safety drop the table without effecting
                            // the data.
                            if (!TableUtils.isHive3Standard(tblMirror.getName(), tblMirror.getTableDefinition(Environment.UPPER))) {
                                String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Is a 'Managed Non-Transactional' " +
                                        "which is a non-standard configuration in Hive 3.  Regardless, the data is " +
                                        "managed and it's not safe to proceed with converting this table. " +
                                        "Review the table and remove manually before running process again.";
                                tblMirror.addIssue(errorMsg);
                                LOG.error(errorMsg);
                                // No need to continue.
                                return;
                            }

                            // Ensure the table purge flag hasn't been set.
                            if (TableUtils.isExternalPurge(tblMirror.getName(), tblMirror.getTableDefinition(Environment.UPPER))) {
                                String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Is a 'Purge' " +
                                        "enabled 'EXTERNAL' table.  The data is " +
                                        "managed and it's not safe to proceed with converting this table. " +
                                        "Review the table and remove manually before running process again.";
                                tblMirror.addIssue(errorMsg);
                                LOG.error(errorMsg);
                                // No need to continue.
                                return;

                            }

                            LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Dropping table");
                            // It was created by HMS Mirror, safe to drop.
                            String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, database, tableName);
                            LOG.debug(getEnvironment() + "(SQL)" + dropTable);
                            try {
                                if (!config.isDryrun())
                                    stmt.execute(dropTable);
                                tblMirror.setExistingTableDropped(Boolean.TRUE);
                            } catch (SQLException throwables) {
                                String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": DROP Table";
                                tblMirror.addIssue(errorMsg + throwables.getMessage());
                                LOG.error(errorMsg, throwables);
                                // No need to continue.
                                return;
                            }
                        } else {
                            // It was NOT created by HMS Mirror.  The table will need to be manually dropped after review.
                            // No need to continue.
                            String errorMsg = getEnvironment() + ":" + database + "." + tableName + ": Was NOT originally " +
                                    "created by the 'hms-mirror' process and isn't safe to proceed. " +
                                    "The table will need to be reviewed and dropped manually and the process run again.";
                            tblMirror.addIssue(errorMsg);
                            LOG.error(errorMsg);
                            return;
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
                        return;

                    }
                }

                // Get the definition for this environment.
                List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
                LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Importing Table from " +
                        config.getCluster(Environment.LOWER).getHcfsNamespace() + config.getExportBaseDirPrefix() + database + "/" + tableName);

                String importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE,
                        database, tableName,
                        config.getCluster(Environment.LOWER).getHcfsNamespace() + config.getExportBaseDirPrefix() + database + "/" + tableName);
                LOG.debug(getEnvironment() + "(SQL)" + importTransferSchema);
                try {
                    if (!config.isDryrun())
                        stmt.execute(importTransferSchema);
                    tblMirror.setImported(Boolean.TRUE);
                } catch (SQLException throwables) {
                    String errorMsg = getEnvironment() + ":" + config.getTransferDbPrefix() + database + "." +
                            tableName + ": Issue Importing table";
                    tblMirror.addIssue(errorMsg + ": " + throwables.getMessage());
                    LOG.error(errorMsg, throwables);

                }


                // Alter Table Data Location back to the original table.
                String originalLocation = TableUtils.getLocation(tableName, tblMirror.getTableDefinition(Environment.LOWER));
                String alterTableLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, database, tableName, originalLocation);
                LOG.debug(getEnvironment() + ":" + database + "." + tableName + ": Altering table Location to original table in lower cluster");
                LOG.debug(getEnvironment() + "(SQL)" + alterTableLocSql);
                try {
                    if (!config.isDryrun())
                        stmt.execute(alterTableLocSql);
                    tblMirror.setLocationAdjusted(Boolean.TRUE);
                } catch (SQLException throwables) {
                    String errorMsg = getEnvironment() + ":" + database + "." + tableName +
                            ": Issue adjusting table location, data may not be available for new table";
                    tblMirror.addIssue(errorMsg + ": " + throwables.getMessage());
                    LOG.error(errorMsg, throwables);
                }

                String flagDate = df.format(new Date());
                String hmsMirrorFlagStage1 = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                        MirrorConf.HMS_MIRROR_STAGE_ONE_FLAG, flagDate);
                LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                        ": Setting table property to identify as 'Stage-1' conversion.");
                LOG.debug(getEnvironment() + "(SQL)" + hmsMirrorFlagStage1);
                try {
                    if (!config.isDryrun())
                        stmt.execute(hmsMirrorFlagStage1);
                    tblMirror.addProp(MirrorConf.HMS_MIRROR_STAGE_ONE_FLAG + "=" + flagDate);
                } catch (SQLException throwables) {
                    String errorMsg = getEnvironment() + ":" + database + "." + tableName +
                            ": Issue setting hms-mirror stage-1 flag for table";
                    tblMirror.addIssue(errorMsg + ": " + throwables.getMessage());
                    LOG.error(errorMsg, throwables);
                }

                String hmsMirrorConverted = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                        MirrorConf.HMS_MIRROR_CONVERTED_FLAG, "true");
                LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                        ": Setting table property to identify as 'hms-mirror' converted");
                LOG.debug(getEnvironment() + "(SQL)" + hmsMirrorConverted);
                try {
                    if (!config.isDryrun())
                        stmt.execute(hmsMirrorConverted);
                    tblMirror.addProp(MirrorConf.HMS_MIRROR_CONVERTED_FLAG + "=true");
                } catch (SQLException throwables) {
                    String errorMsg = getEnvironment() + ":" + database + "." + tableName +
                            ": Issue setting hms-mirror converted flag for table";
                    tblMirror.addIssue(errorMsg + ": " + throwables.getMessage());
                    LOG.error(errorMsg, throwables);

                }

                // Need to check if table is partitioned
                if (TableUtils.isPartitioned(tableName, tblMirror.getTableDefinition(Environment.LOWER))) {

                    // TODO: Address the "discover.partitions"="true" setting.  We have it in the configs, but something
                    //       else needs to be configured in CDP for it to be picked up.
                    //       When the tables are imported, the setting is automatically added in CDP 7.1.4

                    if (this.getPartitionDiscovery().getInitMSCK()) {
                        LOG.debug(getEnvironment() + ":" + database + "." + tableName +
                                ": Discovering " + tblMirror.getPartitionDefinition(Environment.LOWER).size() + " partitions");
                        String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, database, tblMirror.getName());
                        LOG.debug(getEnvironment() + "(SQL)" + msckStmt);
                        try {
                            if (!config.isDryrun())
                                stmt.execute(msckStmt);
                            tblMirror.setDiscoverPartitions(Boolean.TRUE);
                        } catch (SQLException throwables) {
                            String errorMsg = getEnvironment() + ":" + database + "." + tableName +
                                    ": Issue discovering partitions, data may not be available for new table";
                            tblMirror.addIssue(errorMsg + ": " + throwables.getMessage());
                            LOG.error(errorMsg, throwables);
                        }
                    } else {
                        String msg = getEnvironment() + ":" + database + "." + tableName + ": Has " + tblMirror.getPartitionDefinition(Environment.LOWER).size() +
                                " partitions.  Data may not be available until the partition metadata is rebuilt.";
                        tblMirror.addIssue(msg);
                        LOG.info(msg);
                    }
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
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Cluster cluster = (Cluster) o;

        if (!legacyHive.equals(cluster.legacyHive)) return false;
        if (!hcfsNamespace.equals(cluster.hcfsNamespace)) return false;
        if (!jarFile.equals(cluster.jarFile)) return false;
        return hiveServer2.equals(cluster.hiveServer2);
    }

    @Override
    public int hashCode() {
        int result = legacyHive.hashCode();
        result = 31 * result + hcfsNamespace.hashCode();
        result = 31 * result + jarFile.hashCode();
        result = 31 * result + hiveServer2.hashCode();
        return result;
    }

    @Override
    public int compareTo(Cluster o) {
        return 0;
    }
}
