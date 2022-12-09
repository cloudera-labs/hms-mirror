/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.HadoopSession;
import com.cloudera.utils.hadoop.HadoopSessionFactory;
import com.cloudera.utils.hadoop.HadoopSessionPool;
import com.cloudera.utils.hadoop.hms.mirror.feature.LegacyTranslations;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Sets;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Pattern;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.*;

@JsonIgnoreProperties({"featureList"})
public class Config {

    private static final Logger LOG = LogManager.getLogger(Config.class);
    private Acceptance acceptance = new Acceptance();
    @JsonIgnore
    private HadoopSessionPool cliPool;
    private Map<Environment, Cluster> clusters = new TreeMap<Environment, Cluster>();
    private String commandLineOptions = null;
    private boolean copyAvroSchemaUrls = Boolean.FALSE;
    private DataStrategy dataStrategy = DataStrategy.SCHEMA_ONLY;
    private Boolean databaseOnly = Boolean.FALSE;
    private String[] databases = null;
    private LegacyTranslations legacyTranslations = new LegacyTranslations();
    @JsonIgnore
    private final String runMarker = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

    @JsonIgnore
    private Pattern dbFilterPattern = null;
    /*
   Prefix the DB with this to create an alternate db.
   Good for testing.

   Should leave null for like replication.
    */
    private String dbPrefix = null;
    private String dbRename = null;
    @JsonIgnore // wip
    private String dbRegEx = null;
    private Environment dumpSource = null;
    @JsonIgnore
    private final Messages errors = new Messages(100);
    private boolean execute = Boolean.FALSE;
    @JsonIgnore
    private final List<String> flags = new LinkedList<String>();
    /*
    Use 'flip' to switch LEFT and RIGHT cluster definitions.  Allows you to change the direction of the calls.
     */
    private Boolean flip = Boolean.FALSE;
    private HybridConfig hybrid = new HybridConfig();
    private MigrateACID migrateACID = new MigrateACID();
    private MigrateVIEW migrateVIEW = new MigrateVIEW();
    private Boolean migratedNonNative = Boolean.FALSE;
    private Optimization optimization = new Optimization();
    /*
    Used when a schema is transferred and has 'purge' properties for the table.
    When this is 'true', we'll remove the 'purge' option.
    This is helpful for datasets that are in DR, where the table doesn't
    control the Filesystem and we don't want to mess that up.
     */
    private boolean readOnly = Boolean.FALSE;
    /*
    When Common Storage is used with the SQL Data Strategy, this will 'replace' the original table
    with a table by the same name but who's data lives in the common storage location.
     */
    private Boolean replace = Boolean.FALSE;
    private Boolean resetRight = Boolean.FALSE;

    private Boolean resetToDefaultLocation = Boolean.FALSE;

    private Boolean skipFeatures = Boolean.FALSE;
    private Boolean skipLegacyTranslation = Boolean.FALSE;
    /*
    Always true.  leaving to ensure config serialization compatibility.
     */
    @Deprecated
    private Boolean sqlOutput = Boolean.TRUE;
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
    @JsonIgnore
    private Pattern tblFilterPattern = null;
    @JsonIgnore
    private Pattern tblExcludeFilterPattern = null;
    private String tblRegEx = null;

    private String tblExcludeRegEx = null;

    private TransferConfig transfer = new TransferConfig();
    private Boolean transferOwnership = Boolean.FALSE;
    @JsonIgnore
    private ScheduledExecutorService transferThreadPool;
    @JsonIgnore
    private ScheduledExecutorService metadataThreadPool;
    @JsonIgnore
    private Translator translator = new Translator();
    @JsonIgnore
    private final Messages warnings = new Messages(100);

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
        for (Environment env : Environment.values()) {
            if (env.isVisible()) {
                System.out.println();
                System.out.println("Setup " + env + " cluster....");
                System.out.println();


                // get their input as a String
                // Legacy?
                System.out.print("Is the " + env + " hive instance Hive 1 or Hive 2? (Y/N)");
                String response = scanner.next();
                if (response.equalsIgnoreCase("y")) {
                    config.getCluster(env).setLegacyHive(Boolean.TRUE);
                } else {
                    config.getCluster(env).setLegacyHive(Boolean.FALSE);
                }

                // hcfsNamespace
                System.out.print("What is the namespace for the " + env + " cluster? ");
                response = scanner.next();
                config.getCluster(env).setHcfsNamespace(response);

                // HS2 URI
                System.out.print("What is the JDBC URI for the " + env + " cluster? ");
                response = scanner.next();
                HiveServer2Config hs2Cfg = new HiveServer2Config();
                config.getCluster(env).setHiveServer2(hs2Cfg);
                hs2Cfg.setUri(response);

                // If Kerberized, notify to include hive jar in 'aux_libs'
                if (!kerb && response.contains("principal")) {
                    // appears the connection is kerberized.
                    System.out.println("----------------------------------------------------------------------------------------");
                    System.out.println("The connection appears to be Kerberized.\n\t\tPlace the 'hive standalone' driver in '$HOME/.hms-mirror/aux_libs'");
                    System.out.println("\tSPECIAL RUN INSTRUCTIONS for Legacy Kerberos Connections.");
                    System.out.println("\thttps://github.com/dstreev/hms-mirror#running-against-a-legacy-non-cdp-kerberized-hiveserver2");
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
                    System.out.println(" >> Are both clusters running the same version of Hadoop/Hive? (Y/N)");
                    response = scanner.next();
                    if (!response.equalsIgnoreCase("y")) {
                        throw new RuntimeException("Both JDBC connections must be running the same version.");
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

    public Boolean isTranslateLegacy() {
        Boolean rtn = Boolean.FALSE;
        if (!skipLegacyTranslation) {
            // contribs can be in legacy and non-legacy envs.
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public HadoopSessionPool getCliPool() {
        if (cliPool == null) {
            GenericObjectPoolConfig<HadoopSession> hspCfg = new GenericObjectPoolConfig<HadoopSession>();
            hspCfg.setMaxTotal(transfer.getConcurrency());
            this.cliPool = new HadoopSessionPool(new GenericObjectPool<HadoopSession>(new HadoopSessionFactory(), hspCfg));
        }
        return cliPool;
    }

    public void setCliPool(HadoopSessionPool cliPool) {
        this.cliPool = cliPool;
    }

    public String getRunMarker() {
        return runMarker;
    }

    public Boolean isFlip() {
        return flip;
    }

    public void setFlip(Boolean flip) {
        this.flip = flip;
        if (this.flip) {
            Cluster origLeft = getCluster(Environment.LEFT);
            origLeft.setEnvironment(Environment.RIGHT);
            Cluster origRight = getCluster(Environment.RIGHT);
            origRight.setEnvironment(Environment.LEFT);
            getClusters().put(Environment.RIGHT, origLeft);
            getClusters().put(Environment.LEFT, origRight);
        }
    }

    public Boolean isReplace() {
        return replace;
    }

    public void setReplace(Boolean replace) {
        this.replace = replace;
    }

    public Boolean getSkipLegacyTranslation() {
        return skipLegacyTranslation;
    }

    public void setSkipLegacyTranslation(Boolean skipLegacyTranslation) {
        this.skipLegacyTranslation = skipLegacyTranslation;
    }

    public Boolean getSqlOutput() {
        return sqlOutput;
    }

    public void setSqlOutput(Boolean sqlOutput) {
        this.sqlOutput = sqlOutput;
    }

    public Optimization getOptimization() {
        return optimization;
    }

    public void setOptimization(Optimization optimization) {
        this.optimization = optimization;
    }

    public boolean convertManaged() {
        if (getCluster(Environment.LEFT).getLegacyHive() && !getCluster(Environment.RIGHT).getLegacyHive()) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public Translator getTranslator() {
        return translator;
    }

    public void setTranslator(Translator translator) {
        this.translator = translator;
    }

    public String getCommandLineOptions() {
        return commandLineOptions;
    }

    public void setCommandLineOptions(String[] commandLineOptions) {
        this.commandLineOptions = Arrays.toString(commandLineOptions);
    }

    public void setCommandLineOptions(String commandLineOptions) {
        this.commandLineOptions = commandLineOptions;
    }

    public Messages getErrors() {
        return errors;
    }

    public Messages getWarnings() {
        return warnings;
    }

    public LegacyTranslations getLegacyTranslations() {
        return legacyTranslations;
    }

    public void setLegacyTranslations(LegacyTranslations legacyTranslations) {
        this.legacyTranslations = legacyTranslations;
    }

    public MigrateACID getMigrateACID() {
        return migrateACID;
    }

    public void setMigrateACID(MigrateACID migrateACID) {
        this.migrateACID = migrateACID;
    }

    public MigrateVIEW getMigrateVIEW() {
        return migrateVIEW;
    }

    public void setMigrateVIEW(MigrateVIEW migrateVIEW) {
        this.migrateVIEW = migrateVIEW;
    }

    public Boolean getMigratedNonNative() {
        return migratedNonNative;
    }

    public void setMigratedNonNative(Boolean migratedNonNative) {
        this.migratedNonNative = migratedNonNative;
    }

    public Boolean getDatabaseOnly() {
        return databaseOnly;
    }

    public void setDatabaseOnly(Boolean databaseOnly) {
        this.databaseOnly = databaseOnly;
    }

    public DataStrategy getDataStrategy() {
        return dataStrategy;
    }

    public void setDataStrategy(DataStrategy dataStrategy) {
        this.dataStrategy = dataStrategy;
        if (this.dataStrategy != null && this.dataStrategy == DataStrategy.DUMP) {
            this.getMigrateACID().setOn(Boolean.TRUE);
            this.getMigrateVIEW().setOn(Boolean.TRUE);
            this.setMigratedNonNative(Boolean.TRUE);
        }
    }

    public Environment getDumpSource() {
        return dumpSource;
    }

    public void setDumpSource(Environment dumpSource) {
        this.dumpSource = dumpSource;
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

    public void setExecute(boolean execute) {
        this.execute = execute;
    }

    public boolean isCopyAvroSchemaUrls() {
        return copyAvroSchemaUrls;
    }

    public void setCopyAvroSchemaUrls(boolean copyAvroSchemaUrls) {
        this.copyAvroSchemaUrls = copyAvroSchemaUrls;
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

    public String getDbRename() {
        return dbRename;
    }

    public void setDbRename(String dbRename) {
        this.dbRename = dbRename;
    }

    public String getResolvedDB(String database) {
        String rtn = null;
        rtn = (dbPrefix != null ? dbPrefix + database : database);
        rtn = (dbRename != null ? dbRename : database);
        return rtn;
    }

    public ScheduledExecutorService getMetadataThreadPool() {
        if (metadataThreadPool == null) {
            metadataThreadPool = Executors.newScheduledThreadPool(getTransfer().getConcurrency());
        }
        return metadataThreadPool;
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

    public String getTblExcludeRegEx() {
        return tblExcludeRegEx;
    }

    public void setTblExcludeRegEx(String tblExcludeRegEx) {
        this.tblExcludeRegEx = tblExcludeRegEx;
        if (this.tblExcludeRegEx != null)
            tblExcludeFilterPattern = Pattern.compile(tblExcludeRegEx);
        else
            tblExcludeFilterPattern = null;

    }

    public Boolean getSkipFeatures() {
        return skipFeatures;
    }

    public void setSkipFeatures(Boolean skipFeatures) {
        this.skipFeatures = skipFeatures;
    }

    public Pattern getTblFilterPattern() {
        return tblFilterPattern;
    }

    public Pattern getTblExcludeFilterPattern() {
        return tblExcludeFilterPattern;
    }

    public Boolean getResetRight() {
        return resetRight;
    }

    public void setResetRight(Boolean resetRight) {
        this.resetRight = resetRight;
    }

    public Boolean getResetToDefaultLocation() {
        return resetToDefaultLocation;
    }

    public void setResetToDefaultLocation(Boolean resetToDefaultLocation) {
        this.resetToDefaultLocation = resetToDefaultLocation;
    }

    @JsonIgnore
    public Boolean isConnectionKerberized() {
        Boolean rtn = Boolean.FALSE;
        Set<Environment> envs = clusters.keySet();
        for (Environment env : envs) {
            Cluster cluster = clusters.get(env);
            if (cluster.getHiveServer2() != null && cluster.getHiveServer2().isValidUri() && cluster.getHiveServer2().getUri().contains("principal")) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    public Boolean canDeriveDistcpPlan() {
        Boolean rtn = Boolean.FALSE;
        if (getTransfer().getStorageMigration().isDistcp()) {
            rtn = Boolean.TRUE;
        } else {
            warnings.set(DISTCP_OUTPUT_NOT_REQUESTED.getCode());
        }
        if (rtn && resetToDefaultLocation && getTransfer().getWarehouse().getExternalDirectory() == null) {
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    /*
    Before processing, validate the config for issues and warn.  A valid configuration will return 'null'.  An invalid
    config will return an array of String representing the issues.
     */
    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;

        // Set distcp options.
        canDeriveDistcpPlan();

        if (getCluster(Environment.RIGHT).isInitialized()) {
            switch (getDataStrategy()) {
                case DUMP:
                case STORAGE_MIGRATION:
                    break;
                default:
                    if (getCluster(Environment.RIGHT).getLegacyHive() && !getCluster(Environment.LEFT).getLegacyHive()) {
                        errors.set(LEGACY_TO_NON_LEGACY.getCode());
                        rtn = Boolean.FALSE;
                    }
            }
        }

        if (resetToDefaultLocation) {
            if (!(dataStrategy == DataStrategy.SCHEMA_ONLY ||
                    dataStrategy == DataStrategy.STORAGE_MIGRATION ||
                    dataStrategy == DataStrategy.SQL ||
                    dataStrategy == DataStrategy.EXPORT_IMPORT ||
                    dataStrategy == DataStrategy.HYBRID)) {
                errors.set(RESET_TO_DEFAULT_LOCATION.getCode());
                rtn = Boolean.FALSE;
            }
            if (getTransfer().getWarehouse().getManagedDirectory() == null || getTransfer().getWarehouse().getExternalDirectory() == null) {
                errors.set(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getCode());
                rtn = Boolean.FALSE;
            }
        }

        // When RIGHT is defined
        switch (getDataStrategy()) {
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
            case LINKED:
            case SCHEMA_ONLY:
            case CONVERT_LINKED:
                // When the storage on LEFT and RIGHT match, we need to specify both rdl (resetDefaultLocation)
                //   and use -dbp (db prefix) to identify a new db name (hence a location).
                if (getCluster(Environment.RIGHT) != null &&
                        (getCluster(Environment.LEFT).getHcfsNamespace()
                                .equalsIgnoreCase(getCluster(Environment.RIGHT).getHcfsNamespace()))) {
                    if (!resetToDefaultLocation) {
                        errors.set(SAME_CLUSTER_COPY_WITHOUT_RDL.getCode());
                        rtn = Boolean.FALSE;
                    }
                    if (getDbPrefix() == null && getDbRename() == null) {
                        errors.set(SAME_CLUSTER_COPY_WITHOUT_DBPR.getCode());
                        rtn = Boolean.FALSE;
                    }
                }
        }

        // Only allow db rename with a single database.
        if (getDbRename() != null && getDatabases().length > 1) {
            errors.set(DB_RENAME_ONLY_WITH_SINGLE_DB_OPTION.getCode());
            rtn = Boolean.FALSE;
        }

        if (isFlip() && getCluster(Environment.LEFT) == null) {
            errors.set(FLIP_WITHOUT_RIGHT.getCode());
            rtn = Boolean.FALSE;
        }

        if (getTransfer().getConcurrency() > 4) {
            // We need to pass on a few scale parameters to the hs2 configs so the connection pools can handle the scale requested.
            if (getCluster(Environment.LEFT) != null) {
                Cluster cluster = getCluster(Environment.LEFT);
                cluster.getHiveServer2().getConnectionProperties().setProperty( "initialSize", Integer.toString((Integer)getTransfer().getConcurrency()/2));
                cluster.getHiveServer2().getConnectionProperties().setProperty( "maxTotal", Integer.toString(getTransfer().getConcurrency() + 1));
                cluster.getHiveServer2().getConnectionProperties().setProperty( "maxIdle", Integer.toString(getTransfer().getConcurrency()));
                cluster.getHiveServer2().getConnectionProperties().setProperty( "minIdle", Integer.toString((Integer)getTransfer().getConcurrency()/2));
                cluster.getHiveServer2().getConnectionProperties().setProperty( "maxWaitMillis", "10000" );
//                cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", "-1");
            }
            if (getCluster(Environment.RIGHT) != null) {
                Cluster cluster = getCluster(Environment.RIGHT);
                cluster.getHiveServer2().getConnectionProperties().setProperty( "initialSize", Integer.toString((Integer)getTransfer().getConcurrency()/2));
                cluster.getHiveServer2().getConnectionProperties().setProperty( "maxTotal", Integer.toString(getTransfer().getConcurrency() + 1));
                cluster.getHiveServer2().getConnectionProperties().setProperty( "maxIdle", Integer.toString(getTransfer().getConcurrency()));
                cluster.getHiveServer2().getConnectionProperties().setProperty( "minIdle", Integer.toString((Integer)getTransfer().getConcurrency()/2));
                cluster.getHiveServer2().getConnectionProperties().setProperty( "maxWaitMillis", "10000" );
//                cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", "-1");
            }
        }

        if (getTransfer().getStorageMigration().isDistcp()) {
//            if (resetToDefaultLocation && (getTransfer().getWarehouse().getManagedDirectory() == null || getTransfer().getWarehouse().getExternalDirectory() == null)) {
//                errors.set(DISTCP_VALID_DISTCP_RESET_TO_DEFAULT_LOCATION.getCode());
//                rtn = Boolean.FALSE;
//            }
            if (getDataStrategy() == DataStrategy.EXPORT_IMPORT
                    || getDataStrategy() == DataStrategy.COMMON
                    || getDataStrategy() == DataStrategy.DUMP
                    || getDataStrategy() == DataStrategy.LINKED
                    || getDataStrategy() == DataStrategy.CONVERT_LINKED
                    || getDataStrategy() == DataStrategy.HYBRID) {
                errors.set(DISTCP_VALID_STRATEGY.getCode());
                rtn = Boolean.FALSE;
            }
            if (getDataStrategy() == DataStrategy.STORAGE_MIGRATION
                    && isExecute()) {
                errors.set(STORAGE_MIGRATION_DISTCP_NO_EXECUTE.getCode());
                rtn = Boolean.FALSE;
            }
            if (getDataStrategy() == DataStrategy.STORAGE_MIGRATION
                    && getMigrateACID().isOn()) {
                errors.set(STORAGE_MIGRATION_DISTCP_ACID.getCode());
                rtn = Boolean.FALSE;
            }
            if (getDataStrategy() == DataStrategy.SQL
                    && getMigrateACID().isOn()
                    && getMigrateACID().isDowngrade()
                    && (getTransfer().getWarehouse().getExternalDirectory() == null)) {
                errors.set(SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE.getCode());
                rtn = Boolean.FALSE;
            }
            if (getDataStrategy() == DataStrategy.SQL) {
                if (!(getMigrateACID().isOn() && getMigrateACID().isOnly() && getMigrateACID().isDowngrade())) {
                    errors.set(SQL_DISTCP_ONLY_W_DA_ACID.getCode());
                    rtn = Boolean.FALSE;
                }
                if (getTransfer().getCommonStorage() != null
//                        || getTransfer().getIntermediateStorage() != null)
                ) {
                    errors.set(SQL_DISTCP_ACID_W_STORAGE_OPTS.getCode());
                    rtn = Boolean.FALSE;
                }
            }
        }

        // Because the ACID downgrade requires some SQL transformation, we can't do this via SCHEMA_ONLY.
        if (getDataStrategy() == DataStrategy.SCHEMA_ONLY && getMigrateACID().isOn() && getMigrateACID().isDowngrade()) {
            errors.set(ACID_DOWNGRADE_SCHEMA_ONLY.getCode());
            rtn = Boolean.FALSE;
        }

        if (getDataStrategy() == DataStrategy.SCHEMA_ONLY) {
            if (!getTransfer().getStorageMigration().isDistcp()) {
                if (resetToDefaultLocation) {
                    // requires distcp.
                    errors.set(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL.getCode());
                    rtn = Boolean.FALSE;
                }
                if (getTransfer().getIntermediateStorage() != null) {
                    // requires distcp.
                    errors.set(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_IS.getCode());
                    rtn = Boolean.FALSE;
                }
            }
        }

        if (resetToDefaultLocation && (getTransfer().getWarehouse().getExternalDirectory() == null)) {
            warnings.set(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getCode());
        }

        if (sync && tblRegEx != null) {
            warnings.set(SYNC_TBL_FILTER.getCode());
        }
        if (sync && !(dataStrategy == DataStrategy.SCHEMA_ONLY || dataStrategy == DataStrategy.LINKED ||
                dataStrategy == DataStrategy.LINKED)) {
            errors.set(VALID_SYNC_STRATEGIES.getCode());
            rtn = Boolean.FALSE;
        }
        if (migrateACID.isOn() && !(dataStrategy == DataStrategy.SCHEMA_ONLY || dataStrategy == DataStrategy.DUMP ||
                dataStrategy == DataStrategy.EXPORT_IMPORT || dataStrategy == DataStrategy.HYBRID ||
                dataStrategy == DataStrategy.SQL || dataStrategy == DataStrategy.STORAGE_MIGRATION)) {
            errors.set(VALID_ACID_STRATEGIES.getCode());
            rtn = Boolean.FALSE;
        }

        // DUMP does require Execute.
        if (isExecute() && dataStrategy == DataStrategy.DUMP) {
            setExecute(Boolean.FALSE);
        }

        if (migrateACID.isOn() && migrateACID.isInplace()) {
            if (!(dataStrategy == DataStrategy.SQL || dataStrategy == DataStrategy.EXPORT_IMPORT ||
                    dataStrategy == DataStrategy.HYBRID)) {
                errors.set(VALID_ACID_DA_IP_STRATEGIES.getCode());
                rtn = Boolean.FALSE;
            }
            if (this.getTransfer().getCommonStorage() != null) {
                errors.set(COMMON_STORAGE_WITH_DA_IP.getCode());
                rtn = Boolean.FALSE;
            }
            if (this.getTransfer().getIntermediateStorage() != null) {
                errors.set(INTERMEDIATE_STORAGE_WITH_DA_IP.getCode());
                rtn = Boolean.FALSE;
            }
            if (this.getTransfer().getStorageMigration().isDistcp()) {
                errors.set(DISTCP_W_DA_IP_ACID.getCode());
                rtn = Boolean.FALSE;
            }
            if (getCluster(Environment.LEFT).getLegacyHive()) {
                errors.set(DA_IP_NON_LEGACY.getCode());
                rtn = Boolean.FALSE;
            }
        }

        if (dataStrategy == DataStrategy.STORAGE_MIGRATION) {
            // The commonStorage and Storage Migration Namespace are the same thing.
            if (this.getTransfer().getCommonStorage() == null) {
                errors.set(STORAGE_MIGRATION_REQUIRED_NAMESPACE.getCode());
                rtn = Boolean.FALSE;
            }
            if (this.getTransfer().getWarehouse() == null ||
                    (this.getTransfer().getWarehouse().getManagedDirectory() == null ||
                            this.getTransfer().getWarehouse().getExternalDirectory() == null)) {
                errors.set(STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS.getCode());
                rtn = Boolean.FALSE;
            }
        }

        if (dataStrategy == DataStrategy.ACID) {
            errors.set(ACID_NOT_TOP_LEVEL_STRATEGY.getCode());
            rtn = Boolean.FALSE;
        }

        // Test to ensure the clusters are LINKED to support underlying functions.
        switch (dataStrategy) {
            case LINKED:
                if (this.getTransfer().getCommonStorage() != null) {
                    errors.set(COMMON_STORAGE_WITH_LINKED.getCode());
                    rtn = Boolean.FALSE;
                }
                if (this.getTransfer().getIntermediateStorage() != null) {
                    errors.set(INTERMEDIATE_STORAGE_WITH_LINKED.getCode());
                    rtn = Boolean.FALSE;
                }
            case HYBRID:
            case EXPORT_IMPORT:
            case SQL:
                // Only do link test when NOT using intermediate storage.
                if (this.getTransfer().getIntermediateStorage() == null && this.getTransfer().getCommonStorage() == null) {
                    if (!getMigrateACID().isDowngradeInPlace() && !linkTest()) {
                        errors.set(LINK_TEST_FAILED.getCode());
                        rtn = Boolean.FALSE;

                    }
                } else {
                    warnings.set(LINK_TEST_SKIPPED_WITH_IS.getCode());
                }
                break;
            case SCHEMA_ONLY:
                if (this.isCopyAvroSchemaUrls()) {
                    LOG.info("CopyAVROSchemaUrls is TRUE, so the cluster must be linked to do this.  Testing...");
                    if (!linkTest()) {
                        errors.set(LINK_TEST_FAILED.getCode());
                        rtn = Boolean.FALSE;
                    }
                }
                break;
            case DUMP:
                if (getDumpSource() == Environment.RIGHT) {
                    warnings.set(DUMP_ENV_FLIP.getCode());
                }
            case COMMON:
                break;
            case CONVERT_LINKED:
                // Check that the RIGHT cluster is NOT a legacy cluster.  No testing done in this scenario.
                if (getCluster(Environment.RIGHT).getLegacyHive()) {
                    errors.set(LEGACY_HIVE_RIGHT_CLUSTER.getCode());
                    rtn = Boolean.FALSE;
                }
                break;
        }

        // Check the use of downgrades and replace.
        if (getMigrateACID().isDowngrade()) {
            if (!getMigrateACID().isOn()) {
                errors.set(DOWNGRADE_ONLY_FOR_ACID.getCode());
                rtn = Boolean.FALSE;
            }
        }

        if (isReplace()) {
            if (getDataStrategy() != DataStrategy.SQL) {
                errors.set(REPLACE_ONLY_WITH_SQL.getCode());
                rtn = Boolean.FALSE;
            }
            if (getMigrateACID().isOn()) {
                if (!getMigrateACID().isDowngrade()) {
                    errors.set(REPLACE_ONLY_WITH_DA.getCode());
                    rtn = Boolean.FALSE;
                }
            }
        }

        // TODO: Check the connections.
        // If the environments are mix of legacy and non-legacy, check the connections for kerberos or zk.

        // Set maxConnections to Concurrency.
        HiveServer2Config leftHS2 = this.getCluster(Environment.LEFT).getHiveServer2();
        if (!leftHS2.isValidUri()) {
            rtn = Boolean.FALSE;
            errors.set(LEFT_HS2_URI_INVALID.getCode());
        }
        leftHS2.getConnectionProperties().setProperty("maxTotal", Integer.toString(getTransfer().getConcurrency()));
        leftHS2.getConnectionProperties().setProperty("initialSize", Integer.toString(getTransfer().getConcurrency()));
        leftHS2.getConnectionProperties().setProperty("maxIdle", Integer.toString(getTransfer().getConcurrency() / 2));
        leftHS2.getConnectionProperties().setProperty("validationQuery", "SELECT 1");
        leftHS2.getConnectionProperties().setProperty("validationQueryTimeout", "5");
        leftHS2.getConnectionProperties().setProperty("testOnCreate", "true");

        if (leftHS2.isKerberosConnection() && leftHS2.getJarFile() != null) {
            rtn = Boolean.FALSE;
            errors.set(LEFT_KERB_JAR_LOCATION.getCode());
        }

        HiveServer2Config rightHS2 = this.getCluster(Environment.RIGHT).getHiveServer2();

        if (rightHS2 != null) {
            // TODO: Add validation for -rid (right-is-disconnected) option.
            // - Only applies to SCHEMA_ONLY, SQL, EXPORT_IMPORT, and HYBRID data strategies.
            // -
            //
            if (getDataStrategy() != DataStrategy.STORAGE_MIGRATION && !rightHS2.isValidUri()) {
                if (!this.getDataStrategy().equals(DataStrategy.DUMP)) {
                    rtn = Boolean.FALSE;
                    errors.set(RIGHT_HS2_URI_INVALID.getCode());
                }
            } else {

                rightHS2.getConnectionProperties().setProperty("maxTotal", Integer.toString(getTransfer().getConcurrency()));
                rightHS2.getConnectionProperties().setProperty("initialSize", Integer.toString(getTransfer().getConcurrency()));
                rightHS2.getConnectionProperties().setProperty("maxIdle", Integer.toString(getTransfer().getConcurrency() / 2));
                rightHS2.getConnectionProperties().setProperty("validationQuery", "SELECT 1");
                rightHS2.getConnectionProperties().setProperty("validationQueryTimeout", "5");
                rightHS2.getConnectionProperties().setProperty("testOnCreate", "true");

                if (rightHS2.isKerberosConnection() && rightHS2.getJarFile() != null) {
                    rtn = Boolean.FALSE;
                    errors.set(RIGHT_KERB_JAR_LOCATION.getCode());
                }

                if (leftHS2.isKerberosConnection() && rightHS2.isKerberosConnection() &&
                        (this.getCluster(Environment.LEFT).getLegacyHive() != this.getCluster(Environment.RIGHT).getLegacyHive())) {
                    rtn = Boolean.FALSE;
                    errors.set(KERB_ACROSS_VERSIONS.getCode());
                }
            }
        } else {
            if (!(getDataStrategy() == DataStrategy.STORAGE_MIGRATION || getDataStrategy() == DataStrategy.DUMP)) {
                if (!getMigrateACID().isDowngradeInPlace()) {
                    rtn = Boolean.FALSE;
                    errors.set(RIGHT_HS2_DEFINITION_MISSING.getCode());
                }
            }

        }

        if (rtn) {
            // Last check for errors.
            if (errors.getReturnCode() != 0) {
                rtn = Boolean.FALSE;
            }
        }
        return rtn;
    }

    protected Boolean linkTest() {
        Boolean rtn = Boolean.FALSE;
        HadoopSession session = null;
        try {
            session = getCliPool().borrow();
            LOG.info("Performing Cluster Link Test to validate cluster 'hcfsNamespace' availability.");
            // TODO: develop a test to copy data between clusters.
            String leftHCFSNamespace = this.getCluster(Environment.LEFT).getHcfsNamespace();
            String rightHCFSNamespace = this.getCluster(Environment.RIGHT).getHcfsNamespace();

            // List User Directories on LEFT
            String leftlsTestLine = "ls " + leftHCFSNamespace + "/user";
            String rightlsTestLine = "ls " + rightHCFSNamespace + "/user";
            LOG.info("LEFT ls testline: " + leftlsTestLine);
            LOG.info("RIGHT ls testline: " + rightlsTestLine);

            CommandReturn lcr = session.processInput(leftlsTestLine);
            if (lcr.isError()) {
                throw new RuntimeException("Link to RIGHT cluster FAILED.\n " + lcr.getError() +
                        "\nCheck configuration and hcfsNamespace value.  " +
                        "Check the documentation about Linking clusters: https://github.com/dstreev/hms-mirror#linking-clusters-storage-layers");
            }
            CommandReturn rcr = session.processInput(rightlsTestLine);
            if (rcr.isError()) {
                throw new RuntimeException("Link to LEFT cluster FAILED.\n " + rcr.getError() +
                        "\nCheck configuration and hcfsNamespace value.  " +
                        "Check the documentation about Linking clusters: https://github.com/dstreev/hms-mirror#linking-clusters-storage-layers");
            }
            rtn = Boolean.TRUE;
        } finally {
            if (session != null)
                getCliPool().returnSession(session);
        }


        return rtn;
    }

    public Boolean checkConnections() {
        Boolean rtn = Boolean.FALSE;
        Set<Environment> envs = Sets.newHashSet(Environment.LEFT, Environment.RIGHT);
        for (Environment env : envs) {
            Cluster cluster = clusters.get(env);
            if (cluster != null && cluster.getHiveServer2() != null && cluster.getHiveServer2().isValidUri() &&
                    !cluster.getHiveServer2().getDisconnected()) {
                Connection conn = null;
                try {
                    conn = cluster.getConnection();
                    // May not be set for DUMP strategy (RIGHT cluster)
                    if (conn != null) {
                        Statement stmt = null;
                        ResultSet resultSet = null;
                        try {
                            LOG.debug(env + ":" + ": Checking Hive Connection");
                            stmt = conn.createStatement();
                            resultSet = stmt.executeQuery("SHOW DATABASES");
                            LOG.debug(env + ":" + ": Hive Connection Successful");
                            rtn = Boolean.TRUE;
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
                    }
                } catch (SQLException se) {
                    rtn = Boolean.FALSE;
                    LOG.error(env + ": Hive Connection check failed.", se);
                    se.printStackTrace();
                } finally {
                    try {
                        if (conn != null)
                            conn.close();
                    } catch (Throwable throwables) {
                        //
                    }
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

    public TransferConfig getTransfer() {
        return transfer;
    }

    public void setTransfer(TransferConfig transfer) {
        this.transfer = transfer;
    }

    public Boolean getTransferOwnership() {
        return transferOwnership;
    }

    public void setTransferOwnership(Boolean transferOwnership) {
        this.transferOwnership = transferOwnership;
    }

    public Map<Environment, Cluster> getClusters() {
        return clusters;
    }

    public void setClusters(Map<Environment, Cluster> clusters) {
        this.clusters = clusters;
        for (Map.Entry<Environment, Cluster> entry : clusters.entrySet()) {
            entry.getValue().setConfig(this);
        }
    }

    public Cluster getCluster(Environment environment) {
        Cluster cluster = getClusters().get(environment);
        if (cluster == null) {
            cluster = new Cluster();
            switch (environment) {
                case TRANSFER:
                    cluster.setLegacyHive(getCluster(Environment.LEFT).getLegacyHive());
                    break;
                case SHADOW:
                    cluster.setLegacyHive(getCluster(Environment.RIGHT).getLegacyHive());
                    break;
            }
            getClusters().put(environment, cluster);
        }
        if (cluster.getEnvironment() == null) {
            cluster.setEnvironment(environment);
        }
        return cluster;
    }

    /*
    Legacy is when one of the clusters is legacy.
     */
    public Boolean legacyMigration() {
        Boolean rtn = Boolean.FALSE;
        if (getCluster(Environment.LEFT).getLegacyHive() != getCluster(Environment.RIGHT).getLegacyHive()) {
            if (getCluster(Environment.LEFT).getLegacyHive()) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }
}
