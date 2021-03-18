package com.streever.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streever.hadoop.hms.Mirror;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Pattern;

public class Config {

    private static Logger LOG = LogManager.getLogger(Mirror.class);

    @JsonIgnore
    private ScheduledExecutorService transferThreadPool;

    private DataStrategy dataStrategy = DataStrategy.SCHEMA_ONLY;
    private HybridConfig hybrid = new HybridConfig();
    private Boolean migrateACID = Boolean.FALSE;

    private boolean execute = Boolean.FALSE;
    /*
    Used when a schema is transferred and has 'purge' properties for the table.
    When this is 'true', we'll remove the 'purge' option.
    This is helpful for datasets that are in DR, where the table doesn't
    control the Filesystem and we don't want to mess that up.
     */
    private boolean readOnly = Boolean.FALSE;

    /*
    Sync is valid for SCHEMA_ONLY, LINKED, COMMON, and INTERMEDIATE data strategies.
    This will compare the tables between LEFT and RIGHT to ensure that they are in SYNC.
    SYNC means: If a table on the LEFT:
    - it will be created on the RIGHT
    - exists on the right and has changed (Fields and/or Serde), it will be dropped and recreated
    - missing and exists on the RIGHT, it will be dropped.

    If the -ro option is used, the tables that are changed or dropped will be disconnected from the data before
    the drop to ensure they don't modify the FileSystem.

    Transactional tables are NOT considered in this process.
     */
    private boolean sync = Boolean.FALSE;

    /*
    Output SQL to report
     */
    private boolean sqlOutput = Boolean.FALSE;

    private Acceptance acceptance = new Acceptance();

    @JsonIgnore // wip
    private String dbRegEx = null;
    @JsonIgnore
    private Pattern dbFilterPattern = null;
    /*
   Prefix the DB with this to create an alternate db.
   Good for testing.

   Should leave null for like replication.
    */
    private String dbPrefix = null;

    private String[] databases = null;
    private String tblRegEx = null;

    private TransferConfig transfer = new TransferConfig();

    private Map<Environment, Cluster> clusters = new TreeMap<Environment, Cluster>();

    public Boolean getMigrateACID() {
        return migrateACID;
    }

    public void setMigrateACID(Boolean migrateACID) {
        this.migrateACID = migrateACID;
    }

    public DataStrategy getDataStrategy() {
        return dataStrategy;
    }

    public void setDataStrategy(DataStrategy dataStrategy) {
        this.dataStrategy = dataStrategy;
    }

    public HybridConfig getHybrid() {
        return hybrid;
    }

    public void setHybrid(HybridConfig hybrid) {
        this.hybrid = hybrid;
    }

    public boolean isExecute() {
        if (!execute) {
            LOG.debug("Dry-run: ON");
        }
        return execute;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isSqlOutput() {
        return sqlOutput;
    }

    public void setSqlOutput(boolean sqlOutput) {
        this.sqlOutput = sqlOutput;
    }

    public Acceptance getAcceptance() {
        return acceptance;
    }

    public void setAcceptance(Acceptance acceptance) {
        this.acceptance = acceptance;
    }

    public String getDbPrefix() {
        return dbPrefix;
    }

    public void setDbPrefix(String dbPrefix) {
        this.dbPrefix = dbPrefix;
    }

    public String getResolvedDB(String database) {
        String rtn = null;
        rtn = (dbPrefix != null ? dbPrefix + database : database);
        return rtn;
    }

    public ScheduledExecutorService getTransferThreadPool() {
        if (transferThreadPool == null) {
            transferThreadPool = Executors.newScheduledThreadPool(getTransfer().getConcurrency());
        }
        return transferThreadPool;
    }

    public String getDbRegEx() {
        return dbRegEx;
    }

    public void setDbRegEx(String dbRegEx) {
        this.dbRegEx = dbRegEx;
    }

    public String getTblRegEx() {
        return tblRegEx;
    }

    public void setTblRegEx(String tblRegEx) {
        this.tblRegEx = tblRegEx;
        if (this.tblRegEx != null)
            dbFilterPattern = Pattern.compile(tblRegEx);
        else
            dbFilterPattern = null;
    }

    public Boolean isConnectionKerberized() {
        Boolean rtn = Boolean.FALSE;
        Set<Environment> envs = clusters.keySet();
        for (Environment env : envs) {
            Cluster cluster = clusters.get(env);
            if (cluster.getHiveServer2().getUri().contains("principal")) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    public Boolean checkConnections() {
        Boolean rtn = Boolean.TRUE;
        Set<Environment> envs = clusters.keySet();
        for (Environment env : envs) {
            Cluster cluster = clusters.get(env);
            Connection conn = null;
            try {
                conn = cluster.getConnection();

                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    LOG.debug(env + ":" + ": Checking Hive Connection");
                    stmt = conn.createStatement();
                    resultSet = stmt.executeQuery("SHOW DATABASES");
                    LOG.debug(env + ":" + ": Hive Connection Successful");
                } catch (SQLException sql) {
                    // DB Doesn't Exists.
                    LOG.error(env + ": Hive Connection check failed.", sql);
                    rtn = Boolean.FALSE;
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
                rtn = Boolean.FALSE;
                LOG.error(env + ": Hive Connection check failed.", se);
                se.printStackTrace();
            } finally {
                try {
                    conn.close();
                } catch (Throwable throwables) {
                    //
                }
            }
        }
        return rtn;
    }

    public Pattern getDbFilterPattern() {
        return dbFilterPattern;
    }

    public String[] getDatabases() {
        return databases;
    }

    public void setDatabases(String[] databases) {
        this.databases = databases;
    }

    public void setExecute(boolean execute) {
        this.execute = execute;
    }

    public TransferConfig getTransfer() {
        return transfer;
    }

    public void setTransfer(TransferConfig transfer) {
        this.transfer = transfer;
    }

    public Map<Environment, Cluster> getClusters() {
        return clusters;
    }

    public void setClusters(Map<Environment, Cluster> clusters) {
        this.clusters = clusters;
    }

    public Cluster getCluster(Environment environment) {
        Cluster cluster = getClusters().get(environment);
        if (cluster.getEnvironment() == null) {
            cluster.setEnvironment(environment);
        }
        return cluster;
    }

    public void init() {
        // Link Cluster and it's Environment Type.
        Set<Environment> environmentSet = this.getClusters().keySet();
        for (Environment environment : environmentSet) {
            Cluster cluster = clusters.get(environment);
            cluster.setEnvironment(environment);
        }
    }

}
