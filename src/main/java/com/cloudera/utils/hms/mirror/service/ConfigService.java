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

import com.cloudera.utils.hadoop.HadoopSession;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum.STORAGE_MIGRATION;

@Service
@Slf4j
@Getter
public class ConfigService {

    private Config config = null;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public Boolean canDeriveDistcpPlan() {
        Boolean rtn = Boolean.FALSE;
        if (getConfig().getTransfer().getStorageMigration().isDistcp()) {
            rtn = Boolean.TRUE;
        } else {
            getConfig().addWarning(DISTCP_OUTPUT_NOT_REQUESTED);
        }
        if (rtn && getConfig().isResetToDefaultLocation() &&
                getConfig().getTransfer().getWarehouse().getExternalDirectory() == null) {
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    public Boolean checkConnections() {
        boolean rtn = Boolean.FALSE;
        Set<Environment> envs = new HashSet<>();
        if (!(getConfig().getDataStrategy() == DataStrategyEnum.DUMP ||
                getConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION ||
                getConfig().getDataStrategy() == DataStrategyEnum.ICEBERG_CONVERSION)) {
            envs.add(Environment.LEFT);
            envs.add(Environment.RIGHT);
        } else {
            envs.add(Environment.LEFT);
        }

        for (Environment env : envs) {
            Cluster cluster = getConfig().getCluster(env);
            if (cluster != null
                    && cluster.getHiveServer2() != null
                    && cluster.getHiveServer2().isValidUri()
                    && !cluster.getHiveServer2().isDisconnected()) {
                Connection conn = null;
                try {
                    conn = cluster.getConnection();
                    // May not be set for DUMP strategy (RIGHT cluster)
                    log.debug(env + ":" + ": Checking Hive Connection");
                    if (conn != null) {
//                        Statement stmt = null;
//                        ResultSet resultSet = null;
//                        try {
//                            stmt = conn.createStatement();
//                            resultSet = stmt.executeQuery("SHOW DATABASES");
//                            resultSet = stmt.executeQuery("SELECT 'HIVE CONNECTION TEST PASSED' AS STATUS");
                        log.debug(env + ":" + ": Hive Connection Successful");
                        rtn = Boolean.TRUE;
//                        } catch (SQLException sql) {
                        // DB Doesn't Exists.
//                            log.error(env + ": Hive Connection check failed.", sql);
//                            rtn = Boolean.FALSE;
//                        } finally {
//                            if (resultSet != null) {
//                                try {
//                                    resultSet.close();
//                                } catch (SQLException sqlException) {
//                                     ignore
//                                }
//                            }
//                            if (stmt != null) {
//                                try {
//                                    stmt.close();
//                                } catch (SQLException sqlException) {
                        // ignore
//                                }
//                            }
//                        }
                    } else {
                        log.error(env + ": Hive Connection check failed.  Connection is null.");
                        rtn = Boolean.FALSE;
                    }
                } catch (SQLException se) {
                    rtn = Boolean.FALSE;
                    log.error(env + ": Hive Connection check failed.", se);
                    se.printStackTrace();
                } finally {
                    if (conn != null) {
                        try {
                            log.info(env + ": Closing Connection");
                            conn.close();
                        } catch (Throwable throwables) {
                            log.error(env + ": Error closing connection.", throwables);
                        }
                    }
                }
            }
        }
        return rtn;
    }

    public String getResolvedDB(String database) {
        String rtn = null;
        // Set Local Value for adjustments
        String lclDb = database;
        // When dbp, set new value
        lclDb = (getConfig().getDbPrefix() != null ? getConfig().getDbPrefix() + lclDb : lclDb);
        // Rename overrides prefix, otherwise use lclDb as its been set.
        rtn = (getConfig().getDbRename() != null ? getConfig().getDbRename() : lclDb);
        return rtn;
    }

    public Boolean getSkipStatsCollection() {
        // Reset skipStatsCollection to true if we're doing a dump or schema only. (and a few other conditions)
        if (!getConfig().getOptimization().isSkipStatsCollection()) {
            try {
                switch (getConfig().getDataStrategy()) {
                    case DUMP:
                    case SCHEMA_ONLY:
                    case EXPORT_IMPORT:
                        getConfig().getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        break;
                    case STORAGE_MIGRATION:
                        if (getConfig().getTransfer().getStorageMigration().isDistcp()) {
                            getConfig().getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        }
                        break;
                }
            } catch (NullPointerException npe) {
                // Ignore: Caused during 'setup' since the context and config don't exist.
            }
        }
        return getConfig().getOptimization().isSkipStatsCollection();
    }

    @JsonIgnore
    public Boolean isConnectionKerberized() {
        boolean rtn = Boolean.FALSE;
        Set<Environment> envs = getConfig().getClusters().keySet();
        for (Environment env : envs) {
            Cluster cluster = getConfig().getClusters().get(env);
            if (cluster.getHiveServer2() != null &&
                    cluster.getHiveServer2().isValidUri() &&
                    cluster.getHiveServer2().getUri() != null &&
                    cluster.getHiveServer2().getUri().contains("principal")) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    public Boolean legacyMigration() {
        Boolean rtn = Boolean.FALSE;
        if (getConfig().getCluster(Environment.LEFT).isLegacyHive() != getConfig().getCluster(Environment.RIGHT).isLegacyHive()) {
            if (getConfig().getCluster(Environment.LEFT).isLegacyHive()) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    protected Boolean linkTest() {
        Boolean rtn = Boolean.FALSE;
        if (getConfig().isSkipLinkCheck() || getConfig().isLoadingTestData()) {
            log.warn("Skipping Link Check.");
            rtn = Boolean.TRUE;
        } else {
            HadoopSession session = null;
            try {
                session = getConfig().getCliPool().borrow();
                log.info("Performing Cluster Link Test to validate cluster 'hcfsNamespace' availability.");
                // TODO: develop a test to copy data between clusters.
                String leftHCFSNamespace = getConfig().getCluster(Environment.LEFT).getHcfsNamespace();
                String rightHCFSNamespace = getConfig().getCluster(Environment.RIGHT).getHcfsNamespace();

                // List User Directories on LEFT
                String leftlsTestLine = "ls " + leftHCFSNamespace + "/user";
                String rightlsTestLine = "ls " + rightHCFSNamespace + "/user";
                log.info("LEFT ls testline: " + leftlsTestLine);
                log.info("RIGHT ls testline: " + rightlsTestLine);

                CommandReturn lcr = session.processInput(leftlsTestLine);
                if (lcr.isError()) {
                    throw new RuntimeException("Link to RIGHT cluster FAILED.\n " + lcr.getError() +
                            "\nCheck configuration and hcfsNamespace value.  " +
                            "Check the documentation about Linking clusters: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
                }
                CommandReturn rcr = session.processInput(rightlsTestLine);
                if (rcr.isError()) {
                    throw new RuntimeException("Link to LEFT cluster FAILED.\n " + rcr.getError() +
                            "\nCheck configuration and hcfsNamespace value.  " +
                            "Check the documentation about Linking clusters: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
                }
                rtn = Boolean.TRUE;
            } finally {
                if (session != null)
                    getConfig().getCliPool().returnSession(session);
            }
        }
        return rtn;
    }

    public Boolean loadPartitionMetadata() {
        if (getConfig().isEvaluatePartitionLocation() ||
                (getConfig().getDataStrategy() == STORAGE_MIGRATION &&
                        getConfig().getTransfer().getStorageMigration().isDistcp())) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    @Autowired
    public void setConfig(Config config) {
        this.config = config;
    }

    public void setupGSS() {
        try {
            String CURRENT_USER_PROP = "current.user";

            String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
            String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};

            // Get a value that over rides the default, if nothing then use default.
            String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");

            // Set a default
            if (hadoopConfDirProp == null)
                hadoopConfDirProp = "/etc/hadoop/conf";

            Configuration hadoopConfig = new Configuration(true);

            File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
            for (String file : HADOOP_CONF_FILES) {
                File f = new File(hadoopConfDir, file);
                if (f.exists()) {
                    log.debug("Adding conf resource: '" + f.getAbsolutePath() + "'");
                    try {
                        // I found this new Path call failed on the Squadron Clusters.
                        // Not sure why.  Anyhow, the above seems to work the same.
                        hadoopConfig.addResource(new Path(f.getAbsolutePath()));
                    } catch (Throwable t) {
                        // This worked for the Squadron Cluster.
                        // I think it has something to do with the Docker images.
                        hadoopConfig.addResource("file:" + f.getAbsolutePath());
                    }
                }
            }

            // hadoop.security.authentication
            if (hadoopConfig.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
                try {
                    UserGroupInformation.setConfiguration(hadoopConfig);
                } catch (Throwable t) {
                    // Revert to non JNI. This happens in Squadron (Docker Imaged Hosts)
                    log.error("Failed GSS Init.  Attempting different Group Mapping");
                    hadoopConfig.set("hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");
                    UserGroupInformation.setConfiguration(hadoopConfig);
                }
            }
        } catch (Throwable t) {
            log.error("Issue initializing Kerberos", t);
            t.printStackTrace();
            throw t;
        }
    }

    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;

        // Set distcp options.
        canDeriveDistcpPlan();

        switch (getConfig().getDataStrategy()) {
            case DUMP:
            case STORAGE_MIGRATION:
            case ICEBERG_CONVERSION:
                break;
            default:
                if (getConfig().getCluster(Environment.RIGHT).isHdpHive3()) {
                    getConfig().getTranslator().setForceExternalLocation(Boolean.TRUE);
                    getConfig().addWarning(HDP3_HIVE);

                }
                // Check for INPLACE DOWNGRADE, in which case no RIGHT needs to be defined or check.
                if (!getConfig().getMigrateACID().isDowngradeInPlace()) {
                    if (getConfig().getCluster(Environment.RIGHT).isLegacyHive() &&
                            !getConfig().getCluster(Environment.LEFT).isLegacyHive() &&
                            !getConfig().isDumpTestData()) {
                        getConfig().addError(NON_LEGACY_TO_LEGACY);
                        rtn = Boolean.FALSE;
                    }
                }
        }

        if (getConfig().getCluster(Environment.LEFT).isHdpHive3() &&
                getConfig().getCluster(Environment.LEFT).isLegacyHive()) {
            getConfig().addError(LEGACY_AND_HIVE3);
            rtn = Boolean.FALSE;
        }

        if (getConfig().getCluster(Environment.RIGHT).isHdpHive3() &&
                getConfig().getCluster(Environment.RIGHT).isLegacyHive()) {
            getConfig().addError(LEGACY_AND_HIVE3);
            rtn = Boolean.FALSE;
        }

        if (getConfig().getCluster(Environment.LEFT).isHdpHive3() &&
                getConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
            getConfig().getTranslator().setForceExternalLocation(Boolean.TRUE);
            if (getConfig().getMigrateACID().isOn() &&
                    !getConfig().getTransfer().getStorageMigration().isDistcp()) {
                getConfig().addError(HIVE3_ON_HDP_ACID_TRANSFERS);
                rtn = Boolean.FALSE;
            }
        }

        if (getConfig().isResetToDefaultLocation()) {
            if (!(getConfig().getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY ||
                    getConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION ||
                    getConfig().getDataStrategy() == DataStrategyEnum.SQL ||
                    getConfig().getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT ||
                    getConfig().getDataStrategy() == DataStrategyEnum.HYBRID)) {
                getConfig().addError(RESET_TO_DEFAULT_LOCATION);
                rtn = Boolean.FALSE;
            }
            if (getConfig().getTransfer().getWarehouse().getManagedDirectory() == null ||
                    getConfig().getTransfer().getWarehouse().getExternalDirectory() == null) {
                getConfig().addError(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
                rtn = Boolean.FALSE;
            }
            if (getConfig().getTransfer().getStorageMigration().isDistcp()) {
                getConfig().addWarning(RDL_DC_WARNING_TABLE_ALIGNMENT);
//                if (getEvaluatePartitionLocation()) {

//                }
            }
            if (getConfig().getTranslator().isForceExternalLocation()) {
                getConfig().addWarning(RDL_FEL_OVERRIDES);
            }
        }

        if (getConfig().getDataStrategy() == DataStrategyEnum.LINKED) {
            if (getConfig().getMigrateACID().isOn()) {
                log.error("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
                // TODO: Add to errors.
                throw new RuntimeException("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
            }
        }

        // When RIGHT is defined
        switch (getConfig().getDataStrategy()) {
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
            case LINKED:
            case SCHEMA_ONLY:
            case CONVERT_LINKED:
                // When the storage on LEFT and RIGHT match, we need to specify both rdl (resetDefaultLocation)
                //   and use -dbp (db prefix) to identify a new db name (hence a location).
                if (getConfig().getCluster(Environment.RIGHT) != null &&
                        (getConfig().getCluster(Environment.LEFT).getHcfsNamespace()
                                .equalsIgnoreCase(getConfig().getCluster(Environment.RIGHT).getHcfsNamespace()))) {
                    if (!getConfig().isResetToDefaultLocation()) {
                        getConfig().addError(SAME_CLUSTER_COPY_WITHOUT_RDL);
                        rtn = Boolean.FALSE;
                    }
                    if (getConfig().getDbPrefix() == null &&
                            getConfig().getDbRename() == null) {
                        getConfig().addError(SAME_CLUSTER_COPY_WITHOUT_DBPR);
                        rtn = Boolean.FALSE;
                    }
                }
        }

        if (getConfig().isEvaluatePartitionLocation() && !getConfig().isLoadingTestData()) {
            switch (getConfig().getDataStrategy()) {
                case SCHEMA_ONLY:
                case DUMP:
                    // Check the metastore_direct config on the LEFT.
                    if (getConfig().getCluster(Environment.LEFT).getMetastoreDirect() == null) {
                        getConfig().addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
                        rtn = Boolean.FALSE;
                    }
                    getConfig().addWarning(EVALUATE_PARTITION_LOCATION);
                    break;
                case STORAGE_MIGRATION:
                    if (getConfig().getCluster(Environment.LEFT).getMetastoreDirect() == null) {
                        getConfig().addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
                        rtn = Boolean.FALSE;
                    }
                    if (!getConfig().getTransfer().getStorageMigration().isDistcp()) {
                        getConfig().addError(EVALUATE_PARTITION_LOCATION_STORAGE_MIGRATION, "LEFT");
                        rtn = Boolean.FALSE;
                    }
                    break;
                default:
                    getConfig().addError(EVALUATE_PARTITION_LOCATION_USE);
                    rtn = Boolean.FALSE;
            }
        }

        // Only allow db rename with a single database.
        if (getConfig().getDbRename() != null &&
                getConfig().getDatabases().length > 1) {
            getConfig().addError(DB_RENAME_ONLY_WITH_SINGLE_DB_OPTION);
            rtn = Boolean.FALSE;
        }

        if (getConfig().isLoadingTestData()) {
            if (getConfig().getFilter().isTableFiltering()) {
                getConfig().addWarning(IGNORING_TBL_FILTERS_W_TEST_DATA);
            }
        }

        if (getConfig().isFlip() &&
                getConfig().getCluster(Environment.LEFT) == null) {
            getConfig().addError(FLIP_WITHOUT_RIGHT);
            rtn = Boolean.FALSE;
        }

        if (getConfig().getTransfer().getConcurrency() > 4 &&
                !getConfig().isLoadingTestData()) {
            // We need to pass on a few scale parameters to the hs2 configs so the connection pools can handle the scale requested.
            if (getConfig().getCluster(Environment.LEFT) != null) {
                Cluster cluster = getConfig().getCluster(Environment.LEFT);
                cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(getConfig().getTransfer().getConcurrency() / 2));
                cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(getConfig().getTransfer().getConcurrency() / 2));
                if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(getConfig().getTransfer().getConcurrency()));
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "10000");
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(getConfig().getTransfer().getConcurrency()));
                }
            }
            if (getConfig().getCluster(Environment.RIGHT) != null) {
                Cluster cluster = getConfig().getCluster(Environment.RIGHT);
                if (cluster.getHiveServer2() != null) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(getConfig().getTransfer().getConcurrency() / 2));
                    cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(getConfig().getTransfer().getConcurrency() / 2));
                    if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(getConfig().getTransfer().getConcurrency()));
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "10000");
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(getConfig().getTransfer().getConcurrency()));
                    }
                }
            }
        }

        if (getConfig().getTransfer().getStorageMigration().isDistcp()) {
            if (getConfig().getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT
                    || getConfig().getDataStrategy() == DataStrategyEnum.COMMON
                    || getConfig().getDataStrategy() == DataStrategyEnum.DUMP
                    || getConfig().getDataStrategy() == DataStrategyEnum.LINKED
                    || getConfig().getDataStrategy() == DataStrategyEnum.CONVERT_LINKED
                    || getConfig().getDataStrategy() == DataStrategyEnum.HYBRID) {
                getConfig().addError(DISTCP_VALID_STRATEGY);
                rtn = Boolean.FALSE;
            }
            if (getConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                    && getConfig().getTransfer().getStorageMigration().isDistcp()) {
                getConfig().addWarning(STORAGE_MIGRATION_DISTCP_EXECUTE);
            }

            if (getConfig().getFilter().isTableFiltering()) {
                getConfig().addWarning(DISTCP_W_TABLE_FILTERS);
            } else {
                getConfig().addWarning(DISTCP_WO_TABLE_FILTERS);
            }
            if (getConfig().getDataStrategy() == DataStrategyEnum.SQL
                    && getConfig().getMigrateACID().isOn()
                    && getConfig().getMigrateACID().isDowngrade()
                    && (getConfig().getTransfer().getWarehouse().getExternalDirectory() == null)) {
                getConfig().addError(SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE);
                rtn = Boolean.FALSE;
            }
            if (getConfig().getDataStrategy() == DataStrategyEnum.SQL) {
                // For SQL, we can only migrate ACID tables with `distcp` if we're downgrading of them.
                if (getConfig().getMigrateACID().isOn() ||
                        getConfig().getMigrateACID().isOnly()) {
                    if (!getConfig().getMigrateACID().isDowngrade()) {
                        getConfig().addError(SQL_DISTCP_ONLY_W_DA_ACID);
                        rtn = Boolean.FALSE;
                    }
                }
                if (getConfig().getTransfer().getCommonStorage() != null) {
                    getConfig().addError(SQL_DISTCP_ACID_W_STORAGE_OPTS);
                    rtn = Boolean.FALSE;
                }
            }
        }

        // Because the ACID downgrade requires some SQL transformation, we can't do this via SCHEMA_ONLY.
        if (getConfig().getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY &&
                getConfig().getMigrateACID().isOn() &&
                getConfig().getMigrateACID().isDowngrade()) {
            getConfig().addError(ACID_DOWNGRADE_SCHEMA_ONLY);
            rtn = Boolean.FALSE;
        }

        if (getConfig().getMigrateACID().isDowngradeInPlace()) {
            if (getConfig().getDataStrategy() != DataStrategyEnum.SQL) {
                getConfig().addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn = Boolean.FALSE;
            }
        }

        if (getConfig().getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY) {
            if (!getConfig().getTransfer().getStorageMigration().isDistcp()) {
                if (getConfig().isResetToDefaultLocation()) {
                    // requires distcp.
                    getConfig().addError(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL);
                    rtn = Boolean.FALSE;
                }
                if (getConfig().getTransfer().getIntermediateStorage() != null) {
                    // requires distcp.
                    getConfig().addError(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_IS);
                    rtn = Boolean.FALSE;
                }
            }
        }

        if (getConfig().isResetToDefaultLocation()
                && (getConfig().getTransfer().getWarehouse().getExternalDirectory() == null)) {
            getConfig().addWarning(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
        }

        if (getConfig().isSync()
                && (getConfig().getFilter().getTblRegEx() != null
                || getConfig().getFilter().getTblExcludeRegEx() != null)) {
            getConfig().addWarning(SYNC_TBL_FILTER);
        }
        if (getConfig().isSync()
                && !(getConfig().getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || getConfig().getDataStrategy() == DataStrategyEnum.LINKED ||
                getConfig().getDataStrategy() == DataStrategyEnum.SQL ||
                getConfig().getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT ||
                getConfig().getDataStrategy() == DataStrategyEnum.HYBRID)) {
            getConfig().addError(VALID_SYNC_STRATEGIES);
            rtn = Boolean.FALSE;
        }
        if (getConfig().getMigrateACID().isOn()
                && !(getConfig().getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || getConfig().getDataStrategy() == DataStrategyEnum.DUMP
                || getConfig().getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT
                || getConfig().getDataStrategy() == DataStrategyEnum.HYBRID
                || getConfig().getDataStrategy() == DataStrategyEnum.SQL
                || getConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
            getConfig().addError(VALID_ACID_STRATEGIES);
            rtn = Boolean.FALSE;
        }

        // DUMP does require Execute.
        if (getConfig().isExecute()
                && getConfig().getDataStrategy() == DataStrategyEnum.DUMP) {
            getConfig().setExecute(Boolean.FALSE);
        }

        if (getConfig().getMigrateACID().isOn()
                && getConfig().getMigrateACID().isInplace()) {
            if (!(getConfig().getDataStrategy() == DataStrategyEnum.SQL)) {
                getConfig().addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn = Boolean.FALSE;
            }
            if (getConfig().getTransfer().getCommonStorage() != null) {
                getConfig().addError(COMMON_STORAGE_WITH_DA_IP);
                rtn = Boolean.FALSE;
            }
            if (getConfig().getTransfer().getIntermediateStorage() != null) {
                getConfig().addError(INTERMEDIATE_STORAGE_WITH_DA_IP);
                rtn = Boolean.FALSE;
            }
            if (getConfig().getTransfer().getStorageMigration().isDistcp()) {
                getConfig().addError(DISTCP_W_DA_IP_ACID);
                rtn = Boolean.FALSE;
            }
            if (getConfig().getCluster(Environment.LEFT).isLegacyHive()) {
                getConfig().addError(DA_IP_NON_LEGACY);
                rtn = Boolean.FALSE;
            }
        }

        if (getConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
            // The commonStorage and Storage Migration Namespace are the same thing.
            if (getConfig().getTransfer().getCommonStorage() == null) {
                // Use the same namespace, we're assuming that was the intent.
                getConfig().getTransfer().setCommonStorage(getConfig().getCluster(Environment.LEFT).getHcfsNamespace());
                // Force reset to default location.
//                this.setResetToDefaultLocation(Boolean.TRUE);
                getConfig().addWarning(STORAGE_MIGRATION_NAMESPACE_LEFT, getConfig().getCluster(Environment.LEFT).getHcfsNamespace());
                if (!getConfig().isResetToDefaultLocation()
                        && getConfig().getTranslator().getGlobalLocationMap().isEmpty()) {
                    getConfig().addError(STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM);
                    rtn = Boolean.FALSE;
                }
            }
            if (getConfig().getTransfer().getWarehouse() == null ||
                    (getConfig().getTransfer().getWarehouse().getManagedDirectory() == null ||
                            getConfig().getTransfer().getWarehouse().getExternalDirectory() == null)) {
                getConfig().addError(STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS);
                rtn = Boolean.FALSE;
            }
        }

        // Because some just don't get you can't do this...
        if (getConfig().getTransfer().getWarehouse().getManagedDirectory() != null &&
                getConfig().getTransfer().getWarehouse().getExternalDirectory() != null) {
            // Make sure these aren't set to the same location.
            if (getConfig().getTransfer().getWarehouse().getManagedDirectory().equals(getConfig().getTransfer().getWarehouse().getExternalDirectory())) {
                getConfig().addError(WAREHOUSE_DIRS_SAME_DIR, getConfig().getTransfer().getWarehouse().getExternalDirectory()
                        , getConfig().getTransfer().getWarehouse().getManagedDirectory());
                rtn = Boolean.FALSE;
            }
        }

        if (getConfig().getDataStrategy() == DataStrategyEnum.ACID) {
            getConfig().addError(ACID_NOT_TOP_LEVEL_STRATEGY);
            rtn = Boolean.FALSE;
        }

        // Test to ensure the clusters are LINKED to support underlying functions.
        switch (getConfig().getDataStrategy()) {
            case LINKED:
                if (getConfig().getTransfer().getCommonStorage() != null) {
                    getConfig().addError(COMMON_STORAGE_WITH_LINKED);
                    rtn = Boolean.FALSE;
                }
                if (getConfig().getTransfer().getIntermediateStorage() != null) {
                    getConfig().addError(INTERMEDIATE_STORAGE_WITH_LINKED);
                    rtn = Boolean.FALSE;
                }
            case HYBRID:
            case EXPORT_IMPORT:
            case SQL:
                // Only do link test when NOT using intermediate storage.
                if (getConfig().getCluster(Environment.RIGHT).getHiveServer2() != null
                        && !getConfig().getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()
                        && getConfig().getTransfer().getIntermediateStorage() == null
                        && getConfig().getTransfer().getCommonStorage() == null) {
                    if (!getConfig().getMigrateACID().isDowngradeInPlace() && !linkTest()) {
                        getConfig().addError(LINK_TEST_FAILED);
                        rtn = Boolean.FALSE;

                    }
                } else {
                    getConfig().addWarning(LINK_TEST_SKIPPED_WITH_IS);
                }
                break;
            case SCHEMA_ONLY:
                if (getConfig().isCopyAvroSchemaUrls()) {
                    log.info("CopyAVROSchemaUrls is TRUE, so the cluster must be linked to do this.  Testing...");
                    if (!linkTest()) {
                        getConfig().addError(LINK_TEST_FAILED);
                        rtn = Boolean.FALSE;
                    }
                }
                break;
            case DUMP:
                if (getConfig().getDumpSource() == Environment.RIGHT) {
                    getConfig().addWarning(DUMP_ENV_FLIP);
                }
            case COMMON:
                break;
            case CONVERT_LINKED:
                // Check that the RIGHT cluster is NOT a legacy cluster.  No testing done in this scenario.
                if (getConfig().getCluster(Environment.RIGHT).isLegacyHive()) {
                    getConfig().addError(LEGACY_HIVE_RIGHT_CLUSTER);
                    rtn = Boolean.FALSE;
                }
                break;
        }

        // Check the use of downgrades and replace.
        // Removing.  If you use -ma, you'll also be processing non-ACID tables.
        // Logic further down will check for this.
//        if (getConfig().getMigrateACID().isDowngrade()) {
//            if (!getConfig().getMigrateACID().isOn()) {
//                getConfig().addError(DOWNGRADE_ONLY_FOR_ACID);
//                rtn = Boolean.FALSE;
//            }
//        }

        if (getConfig().isReplace()) {
            if (getConfig().getDataStrategy() != DataStrategyEnum.SQL) {
                getConfig().addError(REPLACE_ONLY_WITH_SQL);
                rtn = Boolean.FALSE;
            }
            if (getConfig().getMigrateACID().isOn()) {
                if (!getConfig().getMigrateACID().isDowngrade()) {
                    getConfig().addError(REPLACE_ONLY_WITH_DA);
                    rtn = Boolean.FALSE;
                }
            }
        }

        if (getConfig().isReadOnly()) {
            switch (getConfig().getDataStrategy()) {
                case SCHEMA_ONLY:
                case LINKED:
                case COMMON:
                case SQL:
                    break;
                default:
                    getConfig().addError(RO_VALID_STRATEGIES);
                    rtn = Boolean.FALSE;
            }
        }

        if (getConfig().getCluster(Environment.RIGHT) != null) {
            if (getConfig().getDataStrategy() != DataStrategyEnum.SCHEMA_ONLY &&
                    getConfig().getCluster(Environment.RIGHT).isCreateIfNotExists()) {
                getConfig().addWarning(CINE_WITH_DATASTRATEGY);
            }
        }

        if (getConfig().getTranslator().getGlobalLocationMap() != null) {
            // Validate that none of the 'from' maps overlap.  IE: can't have /data and /data/mydir as from locations.
            //    For items that match /data/mydir maybe confusing as to which one to adjust.
            //   OR we move this to a TreeMap and supply a custom comparator the sorts by length, then natural.  This
            //      will push longer paths to be evaluated first and once a match is found, skip further checks.

        }

        // TODO: Check the connections.
        // If the environments are mix of legacy and non-legacy, check the connections for kerberos or zk.

        // Set maxConnections to Concurrency.
        // Don't validate connections or url's if we're working with test data.
        if (!getConfig().isLoadingTestData()) {
            HiveServer2Config leftHS2 = getConfig().getCluster(Environment.LEFT).getHiveServer2();
            if (!leftHS2.isValidUri()) {
                rtn = Boolean.FALSE;
                getConfig().addError(LEFT_HS2_URI_INVALID);
            }

            if (leftHS2.isKerberosConnection() && leftHS2.getJarFile() != null) {
                rtn = Boolean.FALSE;
                getConfig().addError(LEFT_KERB_JAR_LOCATION);
            }

            HiveServer2Config rightHS2 = getConfig().getCluster(Environment.RIGHT).getHiveServer2();

            if (rightHS2 != null) {
                // TODO: Add validation for -rid (right-is-disconnected) option.
                // - Only applies to SCHEMA_ONLY, SQL, EXPORT_IMPORT, and HYBRID data strategies.
                // -
                //
                if (getConfig().getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION
                        && !rightHS2.isValidUri()) {
                    if (!getConfig().getDataStrategy().equals(DataStrategyEnum.DUMP)) {
                        rtn = Boolean.FALSE;
                        getConfig().addError(RIGHT_HS2_URI_INVALID);
                    }
                } else {

                    if (rightHS2.isKerberosConnection()
                            && rightHS2.getJarFile() != null) {
                        rtn = Boolean.FALSE;
                        getConfig().addError(RIGHT_KERB_JAR_LOCATION);
                    }

                    if (leftHS2.isKerberosConnection()
                            && rightHS2.isKerberosConnection()
                            && (getConfig().getCluster(Environment.LEFT).isLegacyHive() != getConfig().getCluster(Environment.RIGHT).isLegacyHive())) {
                        rtn = Boolean.FALSE;
                        getConfig().addError(KERB_ACROSS_VERSIONS);
                    }
                }
            } else {
                if (!(getConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                        || getConfig().getDataStrategy() == DataStrategyEnum.DUMP)) {
                    if (!getConfig().getMigrateACID().isDowngradeInPlace()) {
                        rtn = Boolean.FALSE;
                        getConfig().addError(RIGHT_HS2_DEFINITION_MISSING);
                    }
                }
            }
        }

        if (rtn) {
            // Last check for errors.
            if (getConfig().getProgression().getErrors().getReturnCode() != 0) {
                rtn = Boolean.FALSE;
            }
        }
        return rtn;
    }

}
