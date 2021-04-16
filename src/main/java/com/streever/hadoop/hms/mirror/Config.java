package com.streever.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.hadoop.hms.Mirror;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
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

    @JsonIgnore
    private List<String> issues = new ArrayList<String>();

    private boolean execute = Boolean.FALSE;
    /*
    Used when a schema is transferred and has 'purge' properties for the table.
    When this is 'true', we'll remove the 'purge' option.
    This is helpful for datasets that are in DR, where the table doesn't
    control the Filesystem and we don't want to mess that up.
     */
    private boolean readOnly = Boolean.FALSE;

    /*
    Sync is valid for SCHEMA_ONLY, LINKED, and COMMON data strategies.
    This will compare the tables between LEFT and RIGHT to ensure that they are in SYNC.
    SYNC means: If a table on the LEFT:
    - it will be created on the RIGHT
    - exists on the right and has changed (Fields and/or Serde), it will be dropped and recreated
    - missing and exists on the RIGHT, it will be dropped.

    If the -ro option is used, the tables that are changed or dropped will be disconnected from the data before
    the drop to ensure they don't modify the FileSystem.

    This option can NOT be used with -tf (table filter).

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

    private String tblRegEx = null;
    @JsonIgnore
    private Pattern tblFilterPattern = null;

    /*
   Prefix the DB with this to create an alternate db.
   Good for testing.

   Should leave null for like replication.
    */
    private String dbPrefix = null;

    private String[] databases = null;

    private TransferConfig transfer = new TransferConfig();

    private Map<Environment, Cluster> clusters = new TreeMap<Environment, Cluster>();

    public List<String> getIssues() {
        return issues;
    }

    public void setIssues(List<String> issues) {
        this.issues = issues;
    }

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

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
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
        if (this.dbRegEx != null)
            dbFilterPattern = Pattern.compile(dbRegEx);
        else
            dbFilterPattern = null;

    }

    public Pattern getDbFilterPattern() {
        return dbFilterPattern;
    }

    public String getTblRegEx() {
        return tblRegEx;
    }

    public void setTblRegEx(String tblRegEx) {
        this.tblRegEx = tblRegEx;
        if (this.tblRegEx != null)
            tblFilterPattern = Pattern.compile(tblRegEx);
        else
            tblFilterPattern = null;
    }

    public Pattern getTblFilterPattern() {
        return tblFilterPattern;
    }

    @JsonIgnore
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

    /*
    Before processing, validate the config for issues and warn.  A valid configuration will return 'null'.  An invalid
    config will return an array of String representing the issues.
     */
    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;
        issues.clear();
        if (sync && tblRegEx != null) {
            String issue = "'sync' can NOT be used with a 'table filter'";
            issues.add(issue);
            System.err.println(issue);
            rtn = Boolean.FALSE;
        }
        if (sync && !(dataStrategy == DataStrategy.SCHEMA_ONLY || dataStrategy == DataStrategy.LINKED ||
                 dataStrategy == DataStrategy.LINKED)) {
            String issue = "'sync' only valid for SCHEMA_ONLY, LINKED, and COMMON data strategies";
            issues.add(issue);
            System.err.println(issue);
            rtn = Boolean.FALSE;
        }
        if (migrateACID && !(dataStrategy == DataStrategy.EXPORT_IMPORT || dataStrategy == DataStrategy.HYBRID)) {
            String issue = "Migrating ACID tables only valid for EXPORT_IMPORT and HYBRID data strategies";
            issues.add(issue);
            System.err.println(issue);
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    /*
    Use this to initialize a default config.
     */
    public static void setup(String configFile) {
        Config config = new Config();
        Scanner scanner = new Scanner(System.in);

        //  prompt for the user's name
        System.out.println("----------------------------------------------------------------");
        System.out.println(".... Default Config not found.  Setup default config.");
        System.out.println("----------------------------------------------------------------");
        Boolean kerb = Boolean.FALSE;
        for (Environment env: Environment.values()) {
            System.out.println("");
            System.out.println("Setup " + env.toString() + " cluster....");
            System.out.println("");


            // get their input as a String
            // Legacy?
            System.out.print("Is the " + env.toString() + " hive instance Hive 1 or Hive 2? (Y/N)");
            String response = scanner.next();
            if (response.equalsIgnoreCase("y")) {
                config.getCluster(env).setLegacyHive(Boolean.TRUE);
            } else {
                config.getCluster(env).setLegacyHive(Boolean.FALSE);
            }

            // hcfsNamespace
            System.out.print("What is the namespace for the " + env.toString() + " cluster? ");
            response = scanner.next();
            config.getCluster(env).setHcfsNamespace(response);

            // HS2 URI
            System.out.print("What is the JDBC URI for the " + env.toString() + " cluster? ");
            response = scanner.next();
            HiveServer2Config hs2Cfg = config.getCluster(env).getHiveServer2();
            hs2Cfg.setUri(response);

            // If Kerberized, notify to include hive jar in 'aux_libs'
            if (!kerb && response.contains("principal")) {
                // appears the connection is kerberized.
                System.out.println("----------------------------------------------------------------------------------------");
                System.out.println("The connection appears to be Kerberized.\n\t\tPlace the 'hive standalone' driver in '$HOME/.hms-mirror/aux_libs'");
                System.out.println("----------------------------------------------------------------------------------------");
                kerb = Boolean.TRUE;
            } else if (response.contains("principal")) {
                System.out.println("----------------------------------------------------------------------------------------");
                System.out.println("The connection ALSO appears to be Kerberized.\n");
                System.out.println(" >> Will your Kerberos Ticket be TRUSTED for BOTH JDBC Kerberos Connections? (Y/N)");
                response = scanner.next();
                if (!response.equalsIgnoreCase("y")) {
                    throw new RuntimeException("Both JDBC connection must trust your kerberos ticket.");
                }
            } else {
                //    get jarFile location.
                //    get username
                //    get password
                System.out.println("----------------------------------------------------------------------------------------");
                System.out.println("What is the location (local) of the 'hive standalone' jar file?");
                response = scanner.next();
                hs2Cfg.setJarFile(response);
                System.out.println("Connection username?");
                response = scanner.next();
                hs2Cfg.getConnectionProperties().put("user", response);
                System.out.println("Connection password?");
                response = scanner.next();
                hs2Cfg.getConnectionProperties().put("password", response);
            }
            // Partition Discovery
            // Only on the RIGHT cluster.
            if (env == Environment.RIGHT) {
                PartitionDiscovery pd = config.getCluster(env).getPartitionDiscovery();
                if (!config.getCluster(env).getLegacyHive()) {
                    // Can only auto-discover in Hive 3
                    System.out.println("Set created tables to 'auto-discover' partitions?(Y/N)");
                    response = scanner.next();
                    if (response.equalsIgnoreCase("y")) {
                        pd.setAuto(Boolean.TRUE);
                    }
                }
                System.out.println("Run 'MSCK' after table creation?(Y/N)");
                response = scanner.next();
                if (response.equalsIgnoreCase("y")) {
                    pd.setInitMSCK(Boolean.TRUE);
                }
            }
        }

        try {
            ObjectMapper mapper;
            mapper = new ObjectMapper(new YAMLFactory());
            mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            String configStr = mapper.writeValueAsString(config);
            File cfgFile = new File(configFile);
            FileWriter cfgFileWriter = null;
            try {
                cfgFileWriter = new FileWriter(cfgFile);
                cfgFileWriter.write(configStr);
                LOG.debug("Default Config 'saved' to: " + cfgFile.getPath());
            } catch (IOException ioe) {
                LOG.error("Problem 'writing' default config", ioe);
            } finally {
                cfgFileWriter.close();
            }
        } catch (JsonProcessingException e) {
            LOG.error("Problem 'saving' default config", e);
        } catch (IOException ioe) {
            LOG.error("Problem 'closing' default config file", ioe);
        }
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
        if (cluster == null) {
            cluster = new Cluster();
            getClusters().put(environment, cluster);
        }
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
