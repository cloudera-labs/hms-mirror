package com.streever.hadoop.hms.mirror;

import com.streever.hadoop.hms.util.DriverUtils;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.*;
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

    private Environment environment = null;
    private Boolean legacyHive = Boolean.TRUE;
    private String hcfsNamespace = null;
    private String jarFile = null;
    private HiveServer2Config hiveServer2 = null;
    private PartitionDiscovery partitionDiscovery = new PartitionDiscovery();
    private Connection conn = null;
    private Boolean open = Boolean.FALSE;

    public Cluster() {
    }

//    public Cluster(Environment environment, String hcfsNamespace, String jarFile, String url) {
//        LOG.debug(getEnvironment() + " - Create Cluster: " + environment + ":" + hcfsNamespace + ":" + jarFile + ":" + url);
//        this.environment = environment;
//        this.hcfsNamespace = hcfsNamespace;
//        this.jarFile = jarFile;
//        this.url = url;
//    }


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

    protected boolean open() {
        if (!open) {
            LOG.debug(getEnvironment() + " - Opening JDBC connection");
            Driver driver = DriverUtils.getDriver(jarFile);
            try {
                conn = DriverManager.getConnection(hiveServer2.getUrl(), hiveServer2.getUser(), hiveServer2.getPassword());
                open = Boolean.TRUE;
            } catch (SQLException throwables) {
                LOG.debug(getEnvironment() + " - Issue opening connection", throwables);
                open = Boolean.FALSE;
            } finally {
                try {
                    DriverManager.deregisterDriver(driver);
                } catch (SQLException throwables) {
                    LOG.debug(getEnvironment() + " - Issue de-registering driver", throwables);
                }
            }
        }
        return open;
    }


    public void loadTables(Conversion conversion, String database) throws SQLException {
        if (open()) {
            LOG.info(getEnvironment() + " - Loading tables for database: " + database);
            DBMirror dbMirror = conversion.addDatabase(database);
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                stmt = conn.createStatement();
                LOG.debug(getEnvironment() + " - Setting Hive DB Session Context");
                stmt.execute(MessageFormat.format(MirrorConf.USE, database));
                LOG.debug(getEnvironment() + " - Getting Table List");
                resultSet = stmt.executeQuery(MirrorConf.SHOW_TABLES);
                while (resultSet.next()) {
                    TableMirror tableMirror = dbMirror.addTable(resultSet.getString(1));
                    loadTableDefinition(database, tableMirror);
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
    }

    protected void loadTableDefinition(String database, TableMirror tableMirror) throws SQLException {
        // The connection should already be in the database;
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            stmt = conn.createStatement();
            LOG.info(getEnvironment() + " - Loading Table Definition: " + tableMirror.getName());
            resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_CREATE_TABLE, database, tableMirror.getName()));
            List<String> tblDef = new ArrayList<String>();
            while (resultSet.next()) {
                tblDef.add(resultSet.getString(1));
            }
            tableMirror.setTableDefinition(getEnvironment(), tblDef);
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
    }

    protected void loadTablePartitionMetadata(String database, TableMirror tableMirror) throws SQLException {
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            stmt = conn.createStatement();
            LOG.info(getEnvironment() + " - Loading Partitions for " + tableMirror.getName());
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

    }

    public void buildTransferSchema(Conversion conversion, String database, Config config) {
        if (open()) {
            DBMirror dbm = conversion.getDatabase(database);
            LOG.info(getEnvironment() + " - Building Transfer Schema for database: " + database);
            Statement stmt = null;
            try {
                try {
                    stmt = conn.createStatement();
                } catch (SQLException throwables) {
                    LOG.error("Issue building statement", throwables);
                    return;
                }
                LOG.debug(getEnvironment() + " - Building Transfer Schema Database:  " +
                        config.getTransferDbPrefix() + database);
                try {
                    stmt.execute(MessageFormat.format(MirrorConf.CREATE_DB,
                            config.getTransferDbPrefix() + database));
                } catch (SQLException throwables) {
                    LOG.error("Issue building transfer database: " +
                            config.getTransferDbPrefix() + database, throwables);
                    return;
                }
                // Loop through Tables
                Set<String> tableNames = dbm.getTableMirrors().keySet();
                for (String tableName : tableNames) {
                    // Get Table
                    LOG.debug(getEnvironment() + " - Getting Table Definition: " + database + "." + tableName);
                    TableMirror tblMirror = dbm.getTable(tableName);
                    // Get the definition for this environment.
                    List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());

                    String dropTransferTable = MessageFormat.format(MirrorConf.DROP_TABLE,
                            config.getTransferDbPrefix() + database, tableName);
                    LOG.debug(getEnvironment() + " - Dropping existing transfer schema (if exists) for table: " +
                            config.getTransferDbPrefix() + database + "." + tableName);
                    try {
                        stmt.execute(dropTransferTable);
                    } catch (SQLException throwables) {
                        LOG.error("Issue dropping existing transfer schema for table: " +
                                config.getTransferDbPrefix() + database + "." + tableName);
                    }

                    String transferCreateTable = MessageFormat.format(MirrorConf.CREATE_EXTERNAL_LIKE,
                            config.getTransferDbPrefix() + database, tableName,
                            database, tableName);
                    LOG.info(getEnvironment() + " - Creating transfer schema for table: " + database + "." + tableName);

                    try {
                        stmt.execute(transferCreateTable);
                    } catch (SQLException throwables) {
                        LOG.error("Issue creating transfer schema for table: " +
                                database + "." + tableName, throwables);
                    }

                    // Determine if it's a Legacy Managed Table.
                    LOG.debug(getEnvironment() + " - Checking Legacy Configuration for table: " +
                            database + "." + tableName);
                    Boolean legacyManaged = TableUtils.isLegacyManaged(this, tableName, tblDef);
                    if (legacyManaged) {
                        String legacyFlag = MessageFormat.format(MirrorConf.ADD_TBL_PROP,
                                config.getTransferDbPrefix() + database, tableName,
                                MirrorConf.LEGACY_MANAGED_FLAG, Boolean.TRUE.toString()
                        );
                        LOG.debug(getEnvironment() + " - Setting Legacy flag for table: " +
                                config.getTransferDbPrefix() + database + "." + tableName);
                        try {
                            stmt.execute(legacyFlag);
                        } catch (SQLException throwables) {
                            LOG.error("Issue setting legacy flag for table: " +
                                    config.getTransferDbPrefix() + database + "." + tableName, throwables);
                        }
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
            // woops

        }
    }

    public void exportTransferSchema(Conversion conversion, String database, Config config) {
        if (open()) {
            LOG.info(getEnvironment() + " - Exporting Transfer Schema for: " +
                    config.getTransferDbPrefix() + database);
            DBMirror dbm = conversion.getDatabase(database);

            Statement stmt = null;
            try {
                try {
                    stmt = conn.createStatement();
                } catch (SQLException throwables) {
                    LOG.error("Issue building statement", throwables);
                    return;
                }

                // Loop through Tables
                Set<String> tableNames = dbm.getTableMirrors().keySet();
                for (String tableName : tableNames) {
                    // Get Table
                    TableMirror tblMirror = dbm.getTable(tableName);
                    // Get the definition for this environment.
                    List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
                    LOG.info(getEnvironment() + " - Exporting Table: " + config.getTransferDbPrefix() + database + "." +
                            tableName + " to " + config.getExportBaseDirPrefix() + database + "/" + tableName);
                    String exportTransferSchema = MessageFormat.format(MirrorConf.EXPORT_TABLE,
                            config.getTransferDbPrefix() + database, tableName,
                            config.getExportBaseDirPrefix() + database + "/" + tableName);
                    LOG.debug(exportTransferSchema);
                    try {
                        stmt.execute(exportTransferSchema);
                    } catch (SQLException throwables) {
                        LOG.error("Issue Exporting table: " + config.getTransferDbPrefix() + database + "." +
                                tableName + " (Check Location Permissions for User running command)", throwables);
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

        }
    }

    public boolean importTransferSchema(Conversion conversion, String database, Config config) {
        // Open the connection and ensure we are running this on the "UPPER" cluster.
        if (open() && this.getEnvironment() == Environment.UPPER) {
            LOG.info(getEnvironment() + " - Importing Transfer Schema for: " +
                    database);
            DBMirror dbm = conversion.getDatabase(database);

            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                try {
                    stmt = conn.createStatement();
                } catch (SQLException throwables) {
                    LOG.error("Issue building statement", throwables);
                    return false;
                }

                // CREATE DB.
                String createDb = MessageFormat.format(MirrorConf.CREATE_DB, database);
                try {
                    LOG.info(createDb);
                    stmt.execute(createDb);
                } catch (SQLException throwables) {
                    LOG.error("Creating DB", throwables);
                    return false;
                }

                // Set db context;
                String useDb = MessageFormat.format(MirrorConf.USE, database);
                try {
                    LOG.debug(useDb);
                    stmt.execute(useDb);
                } catch (SQLException throwables) {
                    LOG.error("USE DB", throwables);
                    return false;
                }

                // Get Current Tables;
                List<String> currentTables = new ArrayList<String>();
                try {
                    LOG.debug(MirrorConf.SHOW_TABLES);
                    resultSet = stmt.executeQuery(MirrorConf.SHOW_TABLES);
                    while (resultSet.next()) {
                        currentTables.add(resultSet.getString(1));
                    }
                } catch (SQLException throwables) {
                    LOG.error("SHOW TABLES", throwables);
                    return false;
                }

                // Loop through Tables
                Set<String> tableNames = dbm.getTableMirrors().keySet();
                for (String tableName : tableNames) {
                    // Check if table exists already.
                    if (currentTables.contains(tableName)) {
                        if (config.isOverwriteTable()) {
                            LOG.info("Overwrite ON: Dropping existing table: " + tableName);
                            String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, database, tableName);
                            try {
                                stmt.execute(dropTable);
                            } catch (SQLException throwables) {
                                LOG.error("DROP Table: " + database + "." + tableName);
                            }
                        } else {
                            LOG.warn("Table exists and Overwrite is OFF. Skipping table: " + tableName);
                            continue;
                        }
                    }

                    // Get Table
                    TableMirror tblMirror = dbm.getTable(tableName);
                    // Get the definition for this environment.
                    List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
                    LOG.info(getEnvironment() + " - Importing Table: " + database + "." +
                            tableName + " from " + config.getCluster(Environment.LOWER).getHcfsNamespace() + config.getExportBaseDirPrefix() + database + "/" + tableName);

                    String importTransferSchema = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE,
                            database, tableName,
                            config.getCluster(Environment.LOWER).getHcfsNamespace() + config.getExportBaseDirPrefix() + database + "/" + tableName);
                    LOG.debug(importTransferSchema);
                    try {
                        stmt.execute(importTransferSchema);
                    } catch (SQLException throwables) {
                        LOG.error("Issue Importing table: " + config.getTransferDbPrefix() + database + "." +
                                tableName, throwables);
                    }

                    // Alter Table Data Location back to the original table.
                    String originalLocation = TableUtils.getLocation(tableName, tblMirror.getTableDefinition(Environment.LOWER));
                    String alterTableLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, database, tableName, originalLocation);
                    LOG.info("Altering table Location to original table in lower cluster.");
                    LOG.debug(alterTableLocSql);
                    try {
                        stmt.execute(alterTableLocSql);
                    } catch (SQLException throwables) {
                        LOG.error("Issue adjusting table location, data may not be available for new table: " +
                                database + "." + tableName, throwables);
                    }

                    String hmsMirrorFlagStage1 = MessageFormat.format(MirrorConf.ADD_TBL_PROP, database, tblMirror.getName(),
                            MirrorConf.HMS_MIRROR_STAGE_ONE_FLAG, df.format(new Date()));
                    LOG.info("Setting table property to identify as 'Stage-1' conversion.");
                    LOG.debug(hmsMirrorFlagStage1);
                    try {
                        stmt.execute(hmsMirrorFlagStage1);
                    } catch (SQLException throwables) {
                        LOG.error("Issue setting hms-mirror stage-1 flag for table: " + database + "." + tblMirror.getName(), throwables);
                    }

                    // Need to check if table is partitioned
                    if (TableUtils.isPartitioned(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LOWER))) {

                        // TODO: Address the "discover.partitions"="true" setting.  We have it in the configs, but something
                        //       else needs to be configured in CDP for it to be picked up.
                        //       When the tables are imported, the setting is automatically added in CDP 7.1.4

                        if (this.getPartitionDiscovery().getInitMSCK()) {
                            LOG.info("Discovering " + tblMirror.getPartitionDefinition(Environment.LOWER).size() + " partitions " +
                                    "for table: " + tblMirror.getName());
                            String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, database, tblMirror.getName());
                            try {
                                stmt.execute(msckStmt);
                            } catch (SQLException throwables) {
                                LOG.error("Issue discovering partitions, data may not be available for new table: " +
                                        database + "." + tableName, throwables);
                            }
                        } else {
                            LOG.info(tblMirror.getName() + " has " + tblMirror.getPartitionDefinition(Environment.LOWER).size() +
                                    " partitions.  Data may not be available until the partition metadata is rebuilt.");
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
        }
        return true;
    }

    public String getTableLocation(Conversion conversion, String database, String tableName) {
        LOG.debug(getEnvironment() + " - Database: " + database + " Table: " + tableName);
        TableMirror tblMirror = conversion.getDatabase(database).getTable(tableName);
        List<String> tblDef = tblMirror.getTableDefinition(getEnvironment());
        String location = TableUtils.getLocation(tableName, tblDef);
        LOG.debug(getEnvironment() + " - Location: " + location);
        return location;
    }

    public void close() {
        if (conn != null) {
            try {
                open = Boolean.FALSE;
                conn.close();
            } catch (SQLException sqlException) {
                // ignore
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
