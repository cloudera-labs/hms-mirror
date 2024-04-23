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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hms.mirror.Cluster;
import com.cloudera.utils.hms.mirror.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.HiveServer2Config;
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
public class HmsMirrorCfgService {

    private HmsMirrorConfig hmsMirrorConfig = null;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public Boolean canDeriveDistcpPlan() {
        Boolean rtn = Boolean.FALSE;
        if (getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp()) {
            // We need to get partition location to support partitioned tables and distcp
            if (!hmsMirrorConfig.isEvaluatePartitionLocation()) {
                if (hmsMirrorConfig.getDataStrategy() == STORAGE_MIGRATION) {
                    // This is an ERROR condition since we can NOT build the correct ALTER TABLE statements without this
                    // information.
                    rtn = Boolean.FALSE;
                    getHmsMirrorConfig().addError(DISTCP_REQUIRES_EPL);
                } else {
                    getHmsMirrorConfig().addWarning(DISTCP_REQUIRES_EPL);
                }
            }
            rtn = Boolean.TRUE;
        } else {
            getHmsMirrorConfig().addWarning(DISTCP_OUTPUT_NOT_REQUESTED);
        }
        if (rtn && getHmsMirrorConfig().isResetToDefaultLocation() &&
                getHmsMirrorConfig().getTransfer().getWarehouse().getExternalDirectory() == null) {
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    public Boolean checkConnections() {
        boolean rtn = Boolean.FALSE;
        Set<Environment> envs = new HashSet<>();
        if (!(getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.DUMP ||
                getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION ||
                getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.ICEBERG_CONVERSION)) {
            envs.add(Environment.LEFT);
            envs.add(Environment.RIGHT);
        } else {
            envs.add(Environment.LEFT);
        }

        for (Environment env : envs) {
            Cluster cluster = getHmsMirrorConfig().getCluster(env);
            if (cluster != null
                    && cluster.getHiveServer2() != null
                    && cluster.getHiveServer2().isValidUri()
                    && !cluster.getHiveServer2().isDisconnected()) {
                Connection conn = null;
                try {
                    conn = cluster.getConnection();
                    // May not be set for DUMP strategy (RIGHT cluster)
                    log.debug("{}:: Checking Hive Connection", env);
                    if (conn != null) {
//                        Statement stmt = null;
//                        ResultSet resultSet = null;
//                        try {
//                            stmt = conn.createStatement();
//                            resultSet = stmt.executeQuery("SHOW DATABASES");
//                            resultSet = stmt.executeQuery("SELECT 'HIVE CONNECTION TEST PASSED' AS STATUS");
                        log.debug("{}:: Hive Connection Successful", env);
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
                        log.error("{}: Hive Connection check failed.  Connection is null.", env);
                        rtn = Boolean.FALSE;
                    }
                } catch (SQLException se) {
                    rtn = Boolean.FALSE;
                    log.error("{}: Hive Connection check failed.", env, se);
                } finally {
                    if (conn != null) {
                        try {
                            log.info("{}: Closing Connection", env);
                            conn.close();
                        } catch (Throwable throwables) {
                            log.error("{}: Error closing connection.", env, throwables);
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
        lclDb = (getHmsMirrorConfig().getDbPrefix() != null ? getHmsMirrorConfig().getDbPrefix() + lclDb : lclDb);
        // Rename overrides prefix, otherwise use lclDb as its been set.
        rtn = (getHmsMirrorConfig().getDbRename() != null ? getHmsMirrorConfig().getDbRename() : lclDb);
        return rtn;
    }

    public Boolean getSkipStatsCollection() {
        // Reset skipStatsCollection to true if we're doing a dump or schema only. (and a few other conditions)
        if (!getHmsMirrorConfig().getOptimization().isSkipStatsCollection()) {
            try {
                switch (getHmsMirrorConfig().getDataStrategy()) {
                    case DUMP:
                    case SCHEMA_ONLY:
                    case EXPORT_IMPORT:
                        getHmsMirrorConfig().getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        break;
                    case STORAGE_MIGRATION:
                        if (getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp()) {
                            getHmsMirrorConfig().getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        }
                        break;
                }
            } catch (NullPointerException npe) {
                // Ignore: Caused during 'setup' since the context and config don't exist.
            }
        }
        return getHmsMirrorConfig().getOptimization().isSkipStatsCollection();
    }

    @JsonIgnore
    public Boolean isConnectionKerberized() {
        boolean rtn = Boolean.FALSE;
        Set<Environment> envs = getHmsMirrorConfig().getClusters().keySet();
        for (Environment env : envs) {
            Cluster cluster = getHmsMirrorConfig().getClusters().get(env);
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
        if (getHmsMirrorConfig().getCluster(Environment.LEFT).isLegacyHive() != getHmsMirrorConfig().getCluster(Environment.RIGHT).isLegacyHive()) {
            if (getHmsMirrorConfig().getCluster(Environment.LEFT).isLegacyHive()) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    protected Boolean linkTest() throws DisabledException {
        Boolean rtn = Boolean.FALSE;
        if (getHmsMirrorConfig().isSkipLinkCheck() || getHmsMirrorConfig().isLoadingTestData()) {
            log.warn("Skipping Link Check.");
            rtn = Boolean.TRUE;
        } else {
            CliEnvironment cli = getHmsMirrorConfig().getCliEnvironment();

            log.info("Performing Cluster Link Test to validate cluster 'hcfsNamespace' availability.");
            // TODO: develop a test to copy data between clusters.
            String leftHCFSNamespace = getHmsMirrorConfig().getCluster(Environment.LEFT).getHcfsNamespace();
            String rightHCFSNamespace = getHmsMirrorConfig().getCluster(Environment.RIGHT).getHcfsNamespace();

            // List User Directories on LEFT
            String leftlsTestLine = "ls " + leftHCFSNamespace + "/user";
            String rightlsTestLine = "ls " + rightHCFSNamespace + "/user";
            log.info("LEFT ls testline: {}", leftlsTestLine);
            log.info("RIGHT ls testline: {}", rightlsTestLine);

            CommandReturn lcr = cli.processInput(leftlsTestLine);
            if (lcr.isError()) {
                throw new RuntimeException("Link to RIGHT cluster FAILED.\n " + lcr.getError() +
                        "\nCheck configuration and hcfsNamespace value.  " +
                        "Check the documentation about Linking clusters: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
            }
            CommandReturn rcr = cli.processInput(rightlsTestLine);
            if (rcr.isError()) {
                throw new RuntimeException("Link to LEFT cluster FAILED.\n " + rcr.getError() +
                        "\nCheck configuration and hcfsNamespace value.  " +
                        "Check the documentation about Linking clusters: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
            }
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean loadPartitionMetadata() {
        if (getHmsMirrorConfig().isEvaluatePartitionLocation() ||
                (getHmsMirrorConfig().getDataStrategy() == STORAGE_MIGRATION &&
                        getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp())) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

//    @Autowired
//    public void setHmsMirrorConfig(HmsMirrorConfig hmsMirrorConfig) {
//        this.hmsMirrorConfig = hmsMirrorConfig;
//    }


    public HmsMirrorCfgService(HmsMirrorConfig hmsMirrorConfig) {
        this.hmsMirrorConfig = hmsMirrorConfig;
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
                    log.debug("Adding conf resource: '{}'", f.getAbsolutePath());
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
            throw t;
        }
    }

    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;

        // Set distcp options.
        canDeriveDistcpPlan();

        switch (getHmsMirrorConfig().getDataStrategy()) {
            case DUMP:
            case STORAGE_MIGRATION:
            case ICEBERG_CONVERSION:
                break;
            default:
                if (getHmsMirrorConfig().getCluster(Environment.RIGHT).isHdpHive3()) {
                    getHmsMirrorConfig().getTranslator().setForceExternalLocation(Boolean.TRUE);
                    getHmsMirrorConfig().addWarning(HDP3_HIVE);

                }
                // Check for INPLACE DOWNGRADE, in which case no RIGHT needs to be defined or check.
                if (!getHmsMirrorConfig().getMigrateACID().isDowngradeInPlace()) {
                    if (getHmsMirrorConfig().getCluster(Environment.RIGHT).isLegacyHive() &&
                            !getHmsMirrorConfig().getCluster(Environment.LEFT).isLegacyHive() &&
                            !getHmsMirrorConfig().isDumpTestData()) {
                        getHmsMirrorConfig().addError(NON_LEGACY_TO_LEGACY);
                        rtn = Boolean.FALSE;
                    }
                }
        }

        if (getHmsMirrorConfig().getCluster(Environment.LEFT).isHdpHive3() &&
                getHmsMirrorConfig().getCluster(Environment.LEFT).isLegacyHive()) {
            getHmsMirrorConfig().addError(LEGACY_AND_HIVE3);
            rtn = Boolean.FALSE;
        }

        if (getHmsMirrorConfig().getCluster(Environment.RIGHT).isHdpHive3() &&
                getHmsMirrorConfig().getCluster(Environment.RIGHT).isLegacyHive()) {
            getHmsMirrorConfig().addError(LEGACY_AND_HIVE3);
            rtn = Boolean.FALSE;
        }

        if (getHmsMirrorConfig().getCluster(Environment.LEFT).isHdpHive3() &&
                getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
            getHmsMirrorConfig().getTranslator().setForceExternalLocation(Boolean.TRUE);
            if (getHmsMirrorConfig().getMigrateACID().isOn() &&
                    !getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp()) {
                getHmsMirrorConfig().addError(HIVE3_ON_HDP_ACID_TRANSFERS);
                rtn = Boolean.FALSE;
            }
        }

        if (getHmsMirrorConfig().isResetToDefaultLocation()) {
            if (!(getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY ||
                    getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION ||
                    getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SQL ||
                    getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT ||
                    getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.HYBRID)) {
                getHmsMirrorConfig().addError(RESET_TO_DEFAULT_LOCATION);
                rtn = Boolean.FALSE;
            }
            if (getHmsMirrorConfig().getTransfer().getWarehouse().getManagedDirectory() == null ||
                    getHmsMirrorConfig().getTransfer().getWarehouse().getExternalDirectory() == null) {
                getHmsMirrorConfig().addError(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
                rtn = Boolean.FALSE;
            }
            if (getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp()) {
                getHmsMirrorConfig().addWarning(RDL_DC_WARNING_TABLE_ALIGNMENT);
            }
            if (getHmsMirrorConfig().getTranslator().isForceExternalLocation()) {
                getHmsMirrorConfig().addWarning(RDL_FEL_OVERRIDES);
            }
        }

        if (getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.LINKED) {
            if (getHmsMirrorConfig().getMigrateACID().isOn()) {
                log.error("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
                // TODO: Add to errors.
                throw new RuntimeException("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
            }
        }

        // When RIGHT is defined
        switch (getHmsMirrorConfig().getDataStrategy()) {
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
            case LINKED:
            case SCHEMA_ONLY:
            case CONVERT_LINKED:
                // When the storage on LEFT and RIGHT match, we need to specify both rdl (resetDefaultLocation)
                //   and use -dbp (db prefix) to identify a new db name (hence a location).
                if (getHmsMirrorConfig().getCluster(Environment.RIGHT) != null &&
                        (getHmsMirrorConfig().getCluster(Environment.LEFT).getHcfsNamespace()
                                .equalsIgnoreCase(getHmsMirrorConfig().getCluster(Environment.RIGHT).getHcfsNamespace()))) {
                    if (!getHmsMirrorConfig().isResetToDefaultLocation()) {
                        getHmsMirrorConfig().addError(SAME_CLUSTER_COPY_WITHOUT_RDL);
                        rtn = Boolean.FALSE;
                    }
                    if (getHmsMirrorConfig().getDbPrefix() == null &&
                            getHmsMirrorConfig().getDbRename() == null) {
                        getHmsMirrorConfig().addError(SAME_CLUSTER_COPY_WITHOUT_DBPR);
                        rtn = Boolean.FALSE;
                    }
                }
        }

        if (getHmsMirrorConfig().isEvaluatePartitionLocation() && !getHmsMirrorConfig().isLoadingTestData()) {
            switch (getHmsMirrorConfig().getDataStrategy()) {
                case SCHEMA_ONLY:
                case DUMP:
                    // Check the metastore_direct config on the LEFT.
                    if (getHmsMirrorConfig().getCluster(Environment.LEFT).getMetastoreDirect() == null) {
                        getHmsMirrorConfig().addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
                        rtn = Boolean.FALSE;
                    }
                    getHmsMirrorConfig().addWarning(EVALUATE_PARTITION_LOCATION);
                    break;
                case STORAGE_MIGRATION:
                    if (getHmsMirrorConfig().getCluster(Environment.LEFT).getMetastoreDirect() == null) {
                        getHmsMirrorConfig().addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
                        rtn = Boolean.FALSE;
                    }
                    if (!getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp()) {
                        getHmsMirrorConfig().addError(EVALUATE_PARTITION_LOCATION_STORAGE_MIGRATION, "LEFT");
                        rtn = Boolean.FALSE;
                    }
                    break;
                default:
                    getHmsMirrorConfig().addError(EVALUATE_PARTITION_LOCATION_USE);
                    rtn = Boolean.FALSE;
            }
        }

        // Only allow db rename with a single database.
        if (getHmsMirrorConfig().getDbRename() != null &&
                getHmsMirrorConfig().getDatabases().length > 1) {
            getHmsMirrorConfig().addError(DB_RENAME_ONLY_WITH_SINGLE_DB_OPTION);
            rtn = Boolean.FALSE;
        }

        if (getHmsMirrorConfig().isLoadingTestData()) {
            if (getHmsMirrorConfig().getFilter().isTableFiltering()) {
                getHmsMirrorConfig().addWarning(IGNORING_TBL_FILTERS_W_TEST_DATA);
            }
        }

        if (getHmsMirrorConfig().isFlip() &&
                getHmsMirrorConfig().getCluster(Environment.LEFT) == null) {
            getHmsMirrorConfig().addError(FLIP_WITHOUT_RIGHT);
            rtn = Boolean.FALSE;
        }

        if (getHmsMirrorConfig().getTransfer().getConcurrency() > 4 &&
                !getHmsMirrorConfig().isLoadingTestData()) {
            // We need to pass on a few scale parameters to the hs2 configs so the connection pools can handle the scale requested.
            if (getHmsMirrorConfig().getCluster(Environment.LEFT) != null) {
                Cluster cluster = getHmsMirrorConfig().getCluster(Environment.LEFT);
                cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(getHmsMirrorConfig().getTransfer().getConcurrency() / 2));
                cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(getHmsMirrorConfig().getTransfer().getConcurrency() / 2));
                if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(getHmsMirrorConfig().getTransfer().getConcurrency()));
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "10000");
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(getHmsMirrorConfig().getTransfer().getConcurrency()));
                }
            }
            if (getHmsMirrorConfig().getCluster(Environment.RIGHT) != null) {
                Cluster cluster = getHmsMirrorConfig().getCluster(Environment.RIGHT);
                if (cluster.getHiveServer2() != null) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(getHmsMirrorConfig().getTransfer().getConcurrency() / 2));
                    cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(getHmsMirrorConfig().getTransfer().getConcurrency() / 2));
                    if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(getHmsMirrorConfig().getTransfer().getConcurrency()));
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "10000");
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(getHmsMirrorConfig().getTransfer().getConcurrency()));
                    }
                }
            }
        }

        if (getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp()) {
            if (getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT
                    || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.COMMON
                    || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.DUMP
                    || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.LINKED
                    || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.CONVERT_LINKED
                    || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.HYBRID) {
                getHmsMirrorConfig().addError(DISTCP_VALID_STRATEGY);
                rtn = Boolean.FALSE;
            }
            if (getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                    && getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp()) {
                getHmsMirrorConfig().addWarning(STORAGE_MIGRATION_DISTCP_EXECUTE);
            }

            if (getHmsMirrorConfig().getFilter().isTableFiltering()) {
                getHmsMirrorConfig().addWarning(DISTCP_W_TABLE_FILTERS);
            } else {
                getHmsMirrorConfig().addWarning(DISTCP_WO_TABLE_FILTERS);
            }
            if (getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SQL
                    && getHmsMirrorConfig().getMigrateACID().isOn()
                    && getHmsMirrorConfig().getMigrateACID().isDowngrade()
                    && (getHmsMirrorConfig().getTransfer().getWarehouse().getExternalDirectory() == null)) {
                getHmsMirrorConfig().addError(SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE);
                rtn = Boolean.FALSE;
            }
            if (getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SQL) {
                // For SQL, we can only migrate ACID tables with `distcp` if we're downgrading of them.
                if (getHmsMirrorConfig().getMigrateACID().isOn() ||
                        getHmsMirrorConfig().getMigrateACID().isOnly()) {
                    if (!getHmsMirrorConfig().getMigrateACID().isDowngrade()) {
                        getHmsMirrorConfig().addError(SQL_DISTCP_ONLY_W_DA_ACID);
                        rtn = Boolean.FALSE;
                    }
                }
                if (getHmsMirrorConfig().getTransfer().getCommonStorage() != null) {
                    getHmsMirrorConfig().addError(SQL_DISTCP_ACID_W_STORAGE_OPTS);
                    rtn = Boolean.FALSE;
                }
            }
        }

        // Because the ACID downgrade requires some SQL transformation, we can't do this via SCHEMA_ONLY.
        if (getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY &&
                getHmsMirrorConfig().getMigrateACID().isOn() &&
                getHmsMirrorConfig().getMigrateACID().isDowngrade()) {
            getHmsMirrorConfig().addError(ACID_DOWNGRADE_SCHEMA_ONLY);
            rtn = Boolean.FALSE;
        }

        if (getHmsMirrorConfig().getMigrateACID().isDowngradeInPlace()) {
            if (getHmsMirrorConfig().getDataStrategy() != DataStrategyEnum.SQL) {
                getHmsMirrorConfig().addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn = Boolean.FALSE;
            }
        }

        if (getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY) {
            if (!getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp()) {
                if (getHmsMirrorConfig().isResetToDefaultLocation()) {
                    // requires distcp.
                    getHmsMirrorConfig().addError(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL);
                    rtn = Boolean.FALSE;
                }
                if (getHmsMirrorConfig().getTransfer().getIntermediateStorage() != null) {
                    // requires distcp.
                    getHmsMirrorConfig().addError(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_IS);
                    rtn = Boolean.FALSE;
                }
            }
        }

        if (getHmsMirrorConfig().isResetToDefaultLocation()
                && (getHmsMirrorConfig().getTransfer().getWarehouse().getExternalDirectory() == null)) {
            getHmsMirrorConfig().addWarning(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
        }

        if (getHmsMirrorConfig().isSync()
                && (getHmsMirrorConfig().getFilter().getTblRegEx() != null
                || getHmsMirrorConfig().getFilter().getTblExcludeRegEx() != null)) {
            getHmsMirrorConfig().addWarning(SYNC_TBL_FILTER);
        }
        if (getHmsMirrorConfig().isSync()
                && !(getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.LINKED ||
                getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SQL ||
                getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT ||
                getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.HYBRID)) {
            getHmsMirrorConfig().addError(VALID_SYNC_STRATEGIES);
            rtn = Boolean.FALSE;
        }
        if (getHmsMirrorConfig().getMigrateACID().isOn()
                && !(getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.DUMP
                || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT
                || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.HYBRID
                || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SQL
                || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
            getHmsMirrorConfig().addError(VALID_ACID_STRATEGIES);
            rtn = Boolean.FALSE;
        }

        // DUMP does require Execute.
        if (getHmsMirrorConfig().isExecute()
                && getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.DUMP) {
            getHmsMirrorConfig().setExecute(Boolean.FALSE);
        }

        if (getHmsMirrorConfig().getMigrateACID().isOn()
                && getHmsMirrorConfig().getMigrateACID().isInplace()) {
            if (!(getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.SQL)) {
                getHmsMirrorConfig().addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn = Boolean.FALSE;
            }
            if (getHmsMirrorConfig().getTransfer().getCommonStorage() != null) {
                getHmsMirrorConfig().addError(COMMON_STORAGE_WITH_DA_IP);
                rtn = Boolean.FALSE;
            }
            if (getHmsMirrorConfig().getTransfer().getIntermediateStorage() != null) {
                getHmsMirrorConfig().addError(INTERMEDIATE_STORAGE_WITH_DA_IP);
                rtn = Boolean.FALSE;
            }
            if (getHmsMirrorConfig().getTransfer().getStorageMigration().isDistcp()) {
                getHmsMirrorConfig().addError(DISTCP_W_DA_IP_ACID);
                rtn = Boolean.FALSE;
            }
            if (getHmsMirrorConfig().getCluster(Environment.LEFT).isLegacyHive()) {
                getHmsMirrorConfig().addError(DA_IP_NON_LEGACY);
                rtn = Boolean.FALSE;
            }
        }

        if (getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
            // The commonStorage and Storage Migration Namespace are the same thing.
            if (getHmsMirrorConfig().getTransfer().getCommonStorage() == null) {
                // Use the same namespace, we're assuming that was the intent.
                getHmsMirrorConfig().getTransfer().setCommonStorage(getHmsMirrorConfig().getCluster(Environment.LEFT).getHcfsNamespace());
                // Force reset to default location.
//                this.setResetToDefaultLocation(Boolean.TRUE);
                getHmsMirrorConfig().addWarning(STORAGE_MIGRATION_NAMESPACE_LEFT, getHmsMirrorConfig().getCluster(Environment.LEFT).getHcfsNamespace());
                if (!getHmsMirrorConfig().isResetToDefaultLocation()
                        && getHmsMirrorConfig().getTranslator().getOrderedGlobalLocationMap().isEmpty()) {
                    getHmsMirrorConfig().addError(STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM);
                    rtn = Boolean.FALSE;
                }
            }
            if (getHmsMirrorConfig().getTransfer().getWarehouse() == null ||
                    (getHmsMirrorConfig().getTransfer().getWarehouse().getManagedDirectory() == null ||
                            getHmsMirrorConfig().getTransfer().getWarehouse().getExternalDirectory() == null)) {
                getHmsMirrorConfig().addError(STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS);
                rtn = Boolean.FALSE;
            }
        }

        // Because some just don't get you can't do this...
        if (getHmsMirrorConfig().getTransfer().getWarehouse().getManagedDirectory() != null &&
                getHmsMirrorConfig().getTransfer().getWarehouse().getExternalDirectory() != null) {
            // Make sure these aren't set to the same location.
            if (getHmsMirrorConfig().getTransfer().getWarehouse().getManagedDirectory().equals(getHmsMirrorConfig().getTransfer().getWarehouse().getExternalDirectory())) {
                getHmsMirrorConfig().addError(WAREHOUSE_DIRS_SAME_DIR, getHmsMirrorConfig().getTransfer().getWarehouse().getExternalDirectory()
                        , getHmsMirrorConfig().getTransfer().getWarehouse().getManagedDirectory());
                rtn = Boolean.FALSE;
            }
        }

        if (getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.ACID) {
            getHmsMirrorConfig().addError(ACID_NOT_TOP_LEVEL_STRATEGY);
            rtn = Boolean.FALSE;
        }

        // Test to ensure the clusters are LINKED to support underlying functions.
        switch (getHmsMirrorConfig().getDataStrategy()) {
            case LINKED:
                if (getHmsMirrorConfig().getTransfer().getCommonStorage() != null) {
                    getHmsMirrorConfig().addError(COMMON_STORAGE_WITH_LINKED);
                    rtn = Boolean.FALSE;
                }
                if (getHmsMirrorConfig().getTransfer().getIntermediateStorage() != null) {
                    getHmsMirrorConfig().addError(INTERMEDIATE_STORAGE_WITH_LINKED);
                    rtn = Boolean.FALSE;
                }
            case HYBRID:
            case EXPORT_IMPORT:
            case SQL:
                // Only do link test when NOT using intermediate storage.
                if (getHmsMirrorConfig().getCluster(Environment.RIGHT).getHiveServer2() != null
                        && !getHmsMirrorConfig().getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()
                        && getHmsMirrorConfig().getTransfer().getIntermediateStorage() == null
                        && getHmsMirrorConfig().getTransfer().getCommonStorage() == null) {

                    try {
                        if (!getHmsMirrorConfig().getMigrateACID().isDowngradeInPlace() && !linkTest()) {
                            getHmsMirrorConfig().addError(LINK_TEST_FAILED);
                            rtn = Boolean.FALSE;
                        }
                    } catch (DisabledException e) {
                        log.error("Link Test Skipped because the CLI Interface is disabled.");
                        getHmsMirrorConfig().addError(LINK_TEST_FAILED);
                        rtn = Boolean.FALSE;
                    }
                } else {
                    getHmsMirrorConfig().addWarning(LINK_TEST_SKIPPED_WITH_IS);
                }
                break;
            case SCHEMA_ONLY:
                if (getHmsMirrorConfig().isCopyAvroSchemaUrls()) {
                    log.info("CopyAVROSchemaUrls is TRUE, so the cluster must be linked to do this.  Testing...");
                    try {
                        if (!linkTest()) {
                            getHmsMirrorConfig().addError(LINK_TEST_FAILED);
                            rtn = Boolean.FALSE;
                        }
                    } catch (DisabledException e) {
                        log.error("Link Test Skipped because the CLI Interface is disabled.");
                        getHmsMirrorConfig().addError(LINK_TEST_FAILED);
                        rtn = Boolean.FALSE;
                    }
                }
                break;
            case DUMP:
                if (getHmsMirrorConfig().getDumpSource() == Environment.RIGHT) {
                    getHmsMirrorConfig().addWarning(DUMP_ENV_FLIP);
                }
            case COMMON:
                break;
            case CONVERT_LINKED:
                // Check that the RIGHT cluster is NOT a legacy cluster.  No testing done in this scenario.
                if (getHmsMirrorConfig().getCluster(Environment.RIGHT).isLegacyHive()) {
                    getHmsMirrorConfig().addError(LEGACY_HIVE_RIGHT_CLUSTER);
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

        if (getHmsMirrorConfig().isReplace()) {
            if (getHmsMirrorConfig().getDataStrategy() != DataStrategyEnum.SQL) {
                getHmsMirrorConfig().addError(REPLACE_ONLY_WITH_SQL);
                rtn = Boolean.FALSE;
            }
            if (getHmsMirrorConfig().getMigrateACID().isOn()) {
                if (!getHmsMirrorConfig().getMigrateACID().isDowngrade()) {
                    getHmsMirrorConfig().addError(REPLACE_ONLY_WITH_DA);
                    rtn = Boolean.FALSE;
                }
            }
        }

        if (getHmsMirrorConfig().isReadOnly()) {
            switch (getHmsMirrorConfig().getDataStrategy()) {
                case SCHEMA_ONLY:
                case LINKED:
                case COMMON:
                case SQL:
                    break;
                default:
                    getHmsMirrorConfig().addError(RO_VALID_STRATEGIES);
                    rtn = Boolean.FALSE;
            }
        }

        if (getHmsMirrorConfig().getCluster(Environment.RIGHT) != null) {
            if (getHmsMirrorConfig().getDataStrategy() != DataStrategyEnum.SCHEMA_ONLY &&
                    getHmsMirrorConfig().getCluster(Environment.RIGHT).isCreateIfNotExists()) {
                getHmsMirrorConfig().addWarning(CINE_WITH_DATASTRATEGY);
            }
        }

        if (getHmsMirrorConfig().getTranslator().getOrderedGlobalLocationMap() != null) {
            // Validate that none of the 'from' maps overlap.  IE: can't have /data and /data/mydir as from locations.
            //    For items that match /data/mydir maybe confusing as to which one to adjust.
            //   OR we move this to a TreeMap and supply a custom comparator the sorts by length, then natural.  This
            //      will push longer paths to be evaluated first and once a match is found, skip further checks.

        }

        // TODO: Check the connections.
        // If the environments are mix of legacy and non-legacy, check the connections for kerberos or zk.

        // Set maxConnections to Concurrency.
        // Don't validate connections or url's if we're working with test data.
        if (!getHmsMirrorConfig().isLoadingTestData()) {
            HiveServer2Config leftHS2 = getHmsMirrorConfig().getCluster(Environment.LEFT).getHiveServer2();
            if (!leftHS2.isValidUri()) {
                rtn = Boolean.FALSE;
                getHmsMirrorConfig().addError(LEFT_HS2_URI_INVALID);
            }

            if (leftHS2.isKerberosConnection() && leftHS2.getJarFile() != null) {
                rtn = Boolean.FALSE;
                getHmsMirrorConfig().addError(LEFT_KERB_JAR_LOCATION);
            }

            HiveServer2Config rightHS2 = getHmsMirrorConfig().getCluster(Environment.RIGHT).getHiveServer2();

            if (rightHS2 != null) {
                // TODO: Add validation for -rid (right-is-disconnected) option.
                // - Only applies to SCHEMA_ONLY, SQL, EXPORT_IMPORT, and HYBRID data strategies.
                // -
                //
                if (getHmsMirrorConfig().getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION
                        && !rightHS2.isValidUri()) {
                    if (!getHmsMirrorConfig().getDataStrategy().equals(DataStrategyEnum.DUMP)) {
                        rtn = Boolean.FALSE;
                        getHmsMirrorConfig().addError(RIGHT_HS2_URI_INVALID);
                    }
                } else {

                    if (rightHS2.isKerberosConnection()
                            && rightHS2.getJarFile() != null) {
                        rtn = Boolean.FALSE;
                        getHmsMirrorConfig().addError(RIGHT_KERB_JAR_LOCATION);
                    }

                    if (leftHS2.isKerberosConnection()
                            && rightHS2.isKerberosConnection()
                            && (getHmsMirrorConfig().getCluster(Environment.LEFT).isLegacyHive() != getHmsMirrorConfig().getCluster(Environment.RIGHT).isLegacyHive())) {
                        rtn = Boolean.FALSE;
                        getHmsMirrorConfig().addError(KERB_ACROSS_VERSIONS);
                    }
                }
            } else {
                if (!(getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                        || getHmsMirrorConfig().getDataStrategy() == DataStrategyEnum.DUMP)) {
                    if (!getHmsMirrorConfig().getMigrateACID().isDowngradeInPlace()) {
                        rtn = Boolean.FALSE;
                        getHmsMirrorConfig().addError(RIGHT_HS2_DEFINITION_MISSING);
                    }
                }
            }
        }

        if (rtn) {
            // Last check for errors.
            if (getHmsMirrorConfig().getProgression().getErrors().getReturnCode() != 0) {
                rtn = Boolean.FALSE;
            }
        }
        return rtn;
    }

}
