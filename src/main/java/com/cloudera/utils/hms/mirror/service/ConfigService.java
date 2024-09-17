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
import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.STORAGE_MIGRATION;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
@Getter
public class ConfigService {

    private ObjectMapper yamlMapper;

    private org.springframework.core.env.Environment springEnv;

    private DomainService domainService;
//    private TranslatorService translatorService;

    @Autowired
    public void setYamlMapper(ObjectMapper yamlMapper) {
        this.yamlMapper = yamlMapper;
    }

    @Autowired
    public void setDomainService(DomainService domainService) {
        this.domainService = domainService;
    }

    @Autowired
    public void setSpringEnv(org.springframework.core.env.Environment springEnv) {
        this.springEnv = springEnv;
    }

    public boolean doWareHousePlansExist(ExecuteSession session) {
        HmsMirrorConfig config = session.getConfig();
        boolean rtn = Boolean.FALSE;
        WarehouseMapBuilder warehouseMapBuilder = config.getTranslator().getWarehouseMapBuilder();
        if (!warehouseMapBuilder.getWarehousePlans().isEmpty()) {
            rtn = Boolean.TRUE;
        }
        if (!rtn) {
            Warehouse warehouse = config.getTransfer().getWarehouse();
            if (nonNull(warehouse) && !(isBlank(warehouse.getExternalDirectory())
                    || isBlank(warehouse.getManagedDirectory()))) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    public boolean isMetastoreDirectConfigured(ExecuteSession session, Environment environment) {
        HmsMirrorConfig config = session.getConfig();
        boolean rtn = Boolean.FALSE;
        if (nonNull(config.getCluster(environment).getMetastoreDirect())) {
            DBStore dbStore = config.getCluster(environment).getMetastoreDirect();
            if (!isBlank(dbStore.getUri())) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    public boolean canDeriveDistcpPlan(ExecuteSession session) {
        boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = session.getConfig();

        if (config.getTransfer().getStorageMigration().isDistcp()) {
            rtn = Boolean.TRUE;
            if (!isBlank(config.getDbRename()) || !isBlank(config.getDbPrefix())) {
                rtn = Boolean.FALSE;
                session.addError(DISTCP_W_RENAME_NOT_SUPPORTED);
            }
        }
        return rtn;
    }

    public String configToString(HmsMirrorConfig config) {
        String rtn = null;
        try {
            rtn = yamlMapper.writeValueAsString(config);
            // Blank out passwords
            rtn = rtn.replaceAll("user:\\s\".*\"", "user: \"*****\"");
            rtn = rtn.replaceAll("password:\\s\".*\"", "password: \"*****\"");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return rtn;
    }

    /*
    Using our alignment table, make the necessary adjustment and record changes to the session for information.

    Return should be true 'if there are no breaking errors'.
     */
    public boolean alignConfigurationSettings(ExecuteSession session, HmsMirrorConfig config) {
        boolean rtn = Boolean.TRUE;
        switch (config.getDataStrategy()) {
            case DUMP:
                // Translation Type need to be RELATIVE.
                if (config.getTransfer().getStorageMigration().getTranslationType() != TranslationTypeEnum.RELATIVE) {
                    session.addConfigAdjustmentMessage(config.getDataStrategy(),
                            "TranslationType",
                            config.getTransfer().getStorageMigration().getTranslationType().toString(),
                            TranslationTypeEnum.RELATIVE.toString(), "Only the RELATIVE Translation Type is supported for DUMP.");
                    config.getTransfer().getStorageMigration().setTranslationType(TranslationTypeEnum.RELATIVE);
                }
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.NA);
                // DUMP doesn't require Execute.
                if (config.isExecute()) {
                    config.setExecute(Boolean.FALSE);
                }

                break;
            case SCHEMA_ONLY:
                switch (config.getTransfer().getStorageMigration().getTranslationType()) {
                    case RELATIVE:
                        // Ensure the proper Data Movement Strategy is set. (which is MANUAL)
                        if (config.getTransfer().getStorageMigration().getDataMovementStrategy() == DataMovementStrategyEnum.SQL) {
                            session.addConfigAdjustmentMessage(config.getDataStrategy(),
                                    "DataMovementStrategy",
                                    config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                                    DataMovementStrategyEnum.MANUAL.toString(), "Only the MANUAL Data Movement Strategy is supported for SCHEMA_ONLY with the translationType of RELATIVE.");
                            config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                        }
                        break;
                    case ALIGNED:
                        // Ensure the proper Data Movement Strategy is set. (which is SQL)
                        if (config.getTransfer().getStorageMigration().getDataMovementStrategy() != DataMovementStrategyEnum.DISTCP) {
                            session.addConfigAdjustmentMessage(config.getDataStrategy(),
                                    "DataMovementStrategy",
                                    config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                                    DataMovementStrategyEnum.DISTCP.toString(), "Only the DISTCP Data Movement Strategy is supported for SCHEMA_ONLY with the translationType of ALIGNED.");
                            config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
                        }
                        break;
                    default:
                        break;
                }

                // Sync process is read-only and no-purge to ensure data is not deleted.
                // Because we'll assume that the data is being copied through other processes.
                if (config.isSync()) {
                    config.setReadOnly(Boolean.TRUE);
                    config.setNoPurge(Boolean.TRUE);
                }

                break;
            case SQL:
            case HYBRID:
                // Correct to the only supported translation type (ALIGNED).
//                if (config.getTransfer().getStorageMigration().getTranslationType() != TranslationTypeEnum.ALIGNED) {
//                    session.addConfigAdjustmentMessage(config.getDataStrategy(),
//                            "TranslationType",
//                            config.getTransfer().getStorageMigration().getTranslationType().toString(),
//                            TranslationTypeEnum.ALIGNED.toString(), "Only the ALIGNED Translation Type is supported for SQL and HYBRID.");
//                    config.getTransfer().getStorageMigration().setTranslationType(TranslationTypeEnum.ALIGNED);
//                }
                // Ensure the proper Data Movement Strategy is set. (which is SQL)
                switch (config.getTransfer().getStorageMigration().getDataMovementStrategy()) {
                    case SQL:
                        break;
                    default:
                        session.addConfigAdjustmentMessage(config.getDataStrategy(),
                                "DataMovementStrategy",
                                config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                                DataMovementStrategyEnum.SQL.toString(), "Only the SQL Data Movement Strategy is supported for SQL and HYBRID.");
                        config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                        break;
                }
                break;
            case EXPORT_IMPORT:
//                if (config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.RELATIVE) {
//                    session.addConfigAdjustmentMessage(config.getDataStrategy(),
//                            "TranslationType",
//                            config.getTransfer().getStorageMigration().getTranslationType().toString(),
//                            TranslationTypeEnum.ALIGNED.toString(), "Only the ALIGNED Translation Type is supported for EXPORT_IMPORT.");
//                    config.getTransfer().getStorageMigration().setTranslationType(TranslationTypeEnum.ALIGNED);
//                }
                if (config.getTransfer().getStorageMigration().getDataMovementStrategy() != DataMovementStrategyEnum.NA) {
                    session.addConfigAdjustmentMessage(config.getDataStrategy(),
                            "DataMovementStrategy",
                            config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                            DataMovementStrategyEnum.NA.toString(), "Only the NA Data Movement Strategy is supported for EXPORT_IMPORT.");
                    config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.NA);
                }
                break;
            case STORAGE_MIGRATION:
                if (config.getTransfer().getStorageMigration().getTranslationType() != TranslationTypeEnum.ALIGNED) {
                    session.addConfigAdjustmentMessage(config.getDataStrategy(),
                            "TranslationType",
                            config.getTransfer().getStorageMigration().getTranslationType().toString(),
                            TranslationTypeEnum.ALIGNED.toString(), "Only the ALIGNED Translation Type is supported for STORAGE_MIGRATION.");
                    config.getTransfer().getStorageMigration().setTranslationType(TranslationTypeEnum.ALIGNED);
                }

                // For the Aligned Translation Type, we need to ensure the Data Movement Strategy is set to SQL or DISTCP.
                if (config.getTransfer().getStorageMigration().getDataMovementStrategy() == DataMovementStrategyEnum.NA) {
                    session.addConfigAdjustmentMessage(config.getDataStrategy(),
                            "DataMovementStrategy",
                            config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                            DataMovementStrategyEnum.DISTCP.toString(), "Only the SQL/DISTCP Data Movement Strategy is supported for STORAGE_MIGRATION.");
                    config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
                }

                break;
            case COMMON:
                break;
            case LINKED:
                // Sync process is read-only and no-purge to ensure data is not deleted.
                // Because we'll assume that the data is being copied through other processes.
                if (config.isSync()) {
                    config.setReadOnly(Boolean.TRUE);
                    config.setNoPurge(Boolean.TRUE);
                }
                break;
            default:
                break;
        }
        if (config.loadMetadataDetails()) {
            switch (config.getDatabaseFilterType()) {
                case WAREHOUSE_PLANS:
//                    if (config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().isEmpty() &&
//                            !config.getDatabases().isEmpty()) {
//                        session.addConfigAdjustmentMessage(config.getDataStrategy(),
//                                "Databases",
//                                config.getDatabases().toString(), "",
//                                "Only Warehouse Plans supported with ALIGNED/DISTCP combination.");
//                        rtn = Boolean.FALSE;
//                    }
                    // Need to ensure we have Warehouse Plans and Databases are in sync.
                    config.getDatabases().clear();
                    if (!config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().isEmpty()) {
                        for (Map.Entry<String, Warehouse> warehousePlan : config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().entrySet()) {
                            config.getDatabases().add(warehousePlan.getKey());
                        }
                    }
                    break;
                case MANUAL:
                case REGEX:
                case UNDETERMINED:
                    break;
            }
        }

        // Check for the unsupport Migration Scenarios
        // ==============================================================================================================
        // We don't support NonLegacy to Legacy Migrations (downgrades)
        if (config.getCluster(Environment.LEFT).isHdpHive3() &&
                config.getCluster(Environment.LEFT).isLegacyHive()) {
            session.addConfigAdjustmentMessage(config.getDataStrategy(),
                    "LegacyHive(LEFT)",
                    Boolean.toString(config.getCluster(Environment.LEFT).isLegacyHive()),
                    Boolean.FALSE.toString(), "Legacy Hive is not supported for HDP Hive 3.");
            config.getCluster(Environment.LEFT).setLegacyHive(Boolean.FALSE);
        }

        if (nonNull(config.getCluster(Environment.RIGHT))
                && config.getCluster(Environment.RIGHT).isHdpHive3() &&
                config.getCluster(Environment.RIGHT).isLegacyHive()) {
            session.addConfigAdjustmentMessage(config.getDataStrategy(),
                    "LegacyHive(RIGHT)",
                    Boolean.toString(config.getCluster(Environment.RIGHT).isLegacyHive()),
                    Boolean.FALSE.toString(), "Legacy Hive is not supported for HDP Hive 3.");
            config.getCluster(Environment.RIGHT).setLegacyHive(Boolean.FALSE);
        }
        // ==============================================================================================================

        return rtn;
    }

    public Boolean getSkipStatsCollection(HmsMirrorConfig config) {
//        HmsMirrorConfig config = executeSessionService.getActiveSession().getConfig();

        // Reset skipStatsCollection to true if we're doing a dump or schema only. (and a few other conditions)
        if (!config.getOptimization().isSkipStatsCollection()) {
            try {
                switch (config.getDataStrategy()) {
                    case DUMP:
                    case SCHEMA_ONLY:
                    case EXPORT_IMPORT:
                        config.getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        break;
                    case STORAGE_MIGRATION:
                        // TODO: This might be helpful to get so we can be more clear with the distcp process. EG: Mapper count.
                        if (config.getTransfer().getStorageMigration().isDistcp()) {
                            config.getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        }
                        break;
                }
            } catch (NullPointerException npe) {
                // Ignore: Caused during 'setup' since the context and config don't exist.
            }
        }
        return config.getOptimization().isSkipStatsCollection();
    }

//    @JsonIgnore
//    public Boolean isConnectionKerberized() {
//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
//        return hmsMirrorConfig.isConnectionKerberized();
//    }

    public Boolean legacyMigration(HmsMirrorConfig config) {
        Boolean rtn = Boolean.FALSE;
//        HmsMirrorConfig config = executeSessionService.getActiveSession().getConfig();

        if (config.getCluster(Environment.LEFT).isLegacyHive() != config.getCluster(Environment.RIGHT).isLegacyHive()) {
            if (config.getCluster(Environment.LEFT).isLegacyHive()) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    protected Boolean linkTest(ExecuteSession session, CliEnvironment cli) throws DisabledException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = session.getConfig();

        if (isNull(cli)) {
            return Boolean.TRUE;
        }

        if (config.isSkipLinkCheck() || config.isLoadingTestData()) {
            log.warn("Skipping Link Check.");
            rtn = Boolean.TRUE;
        } else {
//            CliEnvironment cli = executeSessionService.getCliEnvironment();

            log.info("Performing Cluster Link Test to validate cluster 'hcfsNamespace' availability.");
            // TODO: develop a test to copy data between clusters.
            String leftHCFSNamespace = config.getCluster(Environment.LEFT).getHcfsNamespace();
            String rightHCFSNamespace = config.getCluster(Environment.RIGHT).getHcfsNamespace();

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

//    public Boolean loadPartitionMetadata() {
//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
//        return hmsMirrorConfig.loadPartitionMetadata();
//    }

    /*
    Load a config for the default config directory.
    Check that it is valid, if not, revert to the previous config.

    TODO: Need to return an error that can be shown via the REST API.
     */
    public HmsMirrorConfig loadConfig(String configFileName) {
        HmsMirrorConfig rtn = domainService.deserializeConfig(configFileName);
        return rtn;
    }

    public boolean saveConfig(HmsMirrorConfig config, String configFileName, Boolean overwrite) throws IOException {
        return HmsMirrorConfig.save(config, configFileName, overwrite);
    }

    public void overlayConfig(HmsMirrorConfig config, HmsMirrorConfig overlay) {
//        config.overlay(overlay);
        List<Environment> envs = Arrays.asList(Environment.LEFT, Environment.RIGHT);
        for (Environment env : envs) {
            if (nonNull(config.getCluster(env))) {
                boolean hasHS2 = nonNull(config.getCluster(env).getHiveServer2());
                boolean hasMS = nonNull(config.getCluster(env).getMetastoreDirect());
                if (nonNull(overlay.getCluster(env))) {
                    // Method 1: Replace the entire cluster.
                    config.getClusters().put(env, overlay.getCluster(env));
                    if (hasHS2) {
                        if (isNull(config.getCluster(env).getHiveServer2())) {
                            config.getCluster(env).setHiveServer2(new HiveServer2Config());
                        }
                    }
                    if (hasMS) {
                        if (isNull(config.getCluster(env).getMetastoreDirect())) {
                            config.getCluster(env).setMetastoreDirect(new DBStore());
                        }
                    }
                }
            }
        }

        // Set Encrypted Status of Passwords.
        config.setEncryptedPasswords(overlay.isEncryptedPasswords());

        setDefaultsForDataStrategy(config);
    }

    /*
    Apply a set of rules based on a hierarchy of settings and make adjustments to other configuration elements.
     */
    public void setDefaultsForDataStrategy(HmsMirrorConfig config) {
        // Set Attribute for the config.
        switch (config.getDataStrategy()) {
            case DUMP:
//                config.setEvaluatePartitionLocation(Boolean.FALSE);
                break;
            case STORAGE_MIGRATION:
//                config.setEvaluatePartitionLocation(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
                break;
            case SCHEMA_ONLY:
//                config.setEvaluatePartitionLocation(Boolean.FALSE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                break;
            case SQL:
//                config.setEvaluatePartitionLocation(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                break;
            case EXPORT_IMPORT:
//                config.setEvaluatePartitionLocation(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.EXPORT_IMPORT);
                break;
            case HYBRID:
//                config.setEvaluatePartitionLocation(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.HYBRID);
                break;
            case COMMON:
//                config.setEvaluatePartitionLocation(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                break;
            case LINKED:
//                config.setEvaluatePartitionLocation(Boolean.FALSE);
                // Set to ensure 'dr' doesn't delete LINKED tables.
                config.setNoPurge(Boolean.TRUE);
                config.setReadOnly(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                config.getMigrateACID().setOn(Boolean.FALSE);
                break;
            default:
                break;
        }

    }

    public HmsMirrorConfig createForDataStrategy(DataStrategyEnum dataStrategy) {
        HmsMirrorConfig rtn = new HmsMirrorConfig();
        rtn.setDataStrategy(dataStrategy);

        switch (dataStrategy) {
            case DUMP:
                rtn.getMigrateACID().setOn(Boolean.TRUE);
                rtn.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                Cluster leftDump = new Cluster();
                leftDump.setEnvironment(Environment.LEFT);
                leftDump.setHiveServer2(new HiveServer2Config());
                leftDump.setMetastoreDirect(new DBStore());
                leftDump.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.LEFT, leftDump);
                break;
            case STORAGE_MIGRATION:
                rtn.getMigrateACID().setOn(Boolean.TRUE);
                Cluster leftSM = new Cluster();
                leftSM.setEnvironment(Environment.LEFT);
                leftSM.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.LEFT, leftSM);
                leftSM.setMetastoreDirect(new DBStore());
                leftSM.getMetastoreDirect().setType(DBStore.DB_TYPE.MYSQL);
                rtn.getTransfer().setTargetNamespace("ofs://NEED_TO_SET_THIS");
                leftSM.setHiveServer2(new HiveServer2Config());
                break;
            case SCHEMA_ONLY:
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
            case LINKED:
                Cluster leftT = new Cluster();
                leftT.setEnvironment(Environment.LEFT);
                leftT.setLegacyHive(Boolean.TRUE);
                rtn.getClusters().put(Environment.LEFT, leftT);
                leftT.setMetastoreDirect(new DBStore());
                leftT.getMetastoreDirect().setType(DBStore.DB_TYPE.MYSQL);
                leftT.setHiveServer2(new HiveServer2Config());
                Cluster rightT = new Cluster();
                rightT.setEnvironment(Environment.RIGHT);
                rightT.setHiveServer2(new HiveServer2Config());
                rightT.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.RIGHT, rightT);
                break;
            case COMMON:
                Cluster leftC = new Cluster();
                leftC.setEnvironment(Environment.LEFT);
                leftC.setLegacyHive(Boolean.TRUE);
                rtn.getClusters().put(Environment.LEFT, leftC);
                leftC.setMetastoreDirect(new DBStore());
                leftC.getMetastoreDirect().setType(DBStore.DB_TYPE.MYSQL);
                rtn.getTransfer().setTargetNamespace("hdfs|s3a|ofs://NEED_TO_SET_THIS");
                leftC.setHiveServer2(new HiveServer2Config());
                Cluster rightC = new Cluster();
                rightC.setEnvironment(Environment.RIGHT);
                rightC.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.RIGHT, rightC);
                break;
//            case ICEBERG_CONVERSION:
//                break;
            default:
                break;
        }

        setDefaultsForDataStrategy(rtn);
        return rtn;
    }

//    @Autowired
//    public void setTranslatorService(TranslatorService translatorService) {
//        this.translatorService = translatorService;
//    }

//    public void setupGSS() {
//        try {
//            String CURRENT_USER_PROP = "current.user";
//
//            String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
//            String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};
//
//            // Get a value that over rides the default, if nothing then use default.
//            String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");
//
//            // Set a default
//            if (hadoopConfDirProp == null)
//                hadoopConfDirProp = "/etc/hadoop/conf";
//
//            Configuration hadoopConfig = new Configuration(true);
//
//            File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
//            for (String file : HADOOP_CONF_FILES) {
//                File f = new File(hadoopConfDir, file);
//                if (f.exists()) {
//                    log.debug("Adding conf resource: '{}'", f.getAbsolutePath());
//                    try {
//                        // I found this new Path call failed on the Squadron Clusters.
//                        // Not sure why.  Anyhow, the above seems to work the same.
//                        hadoopConfig.addResource(new Path(f.getAbsolutePath()));
//                    } catch (Throwable t) {
//                        // This worked for the Squadron Cluster.
//                        // I think it has something to do with the Docker images.
//                        hadoopConfig.addResource("file:" + f.getAbsolutePath());
//                    }
//                }
//            }
//
//            // hadoop.security.authentication
//            if (hadoopConfig.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
//                try {
//                    UserGroupInformation.setConfiguration(hadoopConfig);
//                } catch (Throwable t) {
//                    // Revert to non JNI. This happens in Squadron (Docker Imaged Hosts)
//                    log.error("Failed GSS Init.  Attempting different Group Mapping");
//                    hadoopConfig.set("hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");
//                    UserGroupInformation.setConfiguration(hadoopConfig);
//                }
//            }
//        } catch (Throwable t) {
//            log.error("Issue initializing Kerberos", t);
//            throw t;
//        }
//    }

    public Boolean validateForConnections(ExecuteSession session) {
        Boolean rtn = Boolean.TRUE;

        HmsMirrorConfig config = session.getConfig();

        RunStatus runStatus = session.getRunStatus();

        if (config.isEncryptedPasswords()) {
            runStatus.addWarning(PASSWORDS_ENCRYPTED);
            if (isNull(config.getPasswordKey()) || config.getPasswordKey().isEmpty()) {
                runStatus.addError(PKEY_PASSWORD_CFG);
                rtn = Boolean.FALSE;
            }
        }

        List<Environment> envList = Arrays.asList(Environment.LEFT, Environment.RIGHT);
        Set<Environment> envSet = new TreeSet<>(envList);
        // Validate that the cluster properties are set for the data strategy.
        switch (config.getDataStrategy()) {
            // Need both clusters defined and HS2 configs set.
            case SQL:
                // Inplace Downgrade is a single cluster effort
                if (config.getMigrateACID().isDowngradeInPlace()) {
                    envSet.remove(Environment.RIGHT);
                    // Drop the Right cluster to prevent confusion.
                    config.getClusters().remove(Environment.RIGHT);
                }
            case SCHEMA_ONLY:
            case EXPORT_IMPORT:
            case HYBRID:
            case COMMON:
                for (Environment env : envSet) {
                    if (isNull(config.getCluster(env))) {
                        runStatus.addError(CLUSTER_NOT_DEFINED_OR_CONFIGURED, env);
                        rtn = Boolean.FALSE;
                    } else {
                        if (isNull(config.getCluster(env).getHiveServer2())) {
                            if (!config.isLoadingTestData()) {
                                runStatus.addError(HS2_NOT_DEFINED_OR_CONFIGURED, env);
                                rtn = Boolean.FALSE;
                            }
                        } else {
                            // Check the config values.
                            /*
                            Minimum Values:
                            - uri
                            - driverClassName
                            - username (if not kerberized)

                             */
                            HiveServer2Config hs2 = config.getCluster(env).getHiveServer2();
                            if (!config.isLoadingTestData()) {
                                if (isBlank(hs2.getUri())) {
                                    runStatus.addError(MISSING_PROPERTY, "uri", "HiveServer2", env);
                                    rtn = Boolean.FALSE;
                                }
                                if (isBlank(hs2.getDriverClassName())) {
                                    runStatus.addError(MISSING_PROPERTY, "driverClassName", "HiveServer2", env);
                                    rtn = Boolean.FALSE;
                                }
                                if (!hs2.isKerberosConnection() && isBlank(hs2.getConnectionProperties().getProperty("user"))) {
                                    runStatus.addError(MISSING_PROPERTY, "user", "HiveServer2", env);
                                    rtn = Boolean.FALSE;
                                }
                            }
                        }
                    }
                    // If evaluate partition locations is set, we need metastore_direct set on LEFT.
//                    if (env == Environment.LEFT) {
//                        if (config.isEvaluatePartitionLocation()) {
//                            if (isNull(config.getCluster(env).getMetastoreDirect())) {
//                                if (!config.isLoadingTestData()) {
//                                    runStatus.addError(EVALUATE_PARTITION_LOCATION_CONFIG, env);
//                                    rtn = Boolean.FALSE;
//                                }
//                            } else {
//                                // Check the config values;
//                                    /* Minimum Values:
//                                    - type
//                                    - uri
//                                    - username
//                                    - password
//                                     */
//                                if (!config.isLoadingTestData()) {
//                                    DBStore dbStore = config.getCluster(env).getMetastoreDirect();
//                                    if (isNull(dbStore.getType())) {
//                                        runStatus.addError(MISSING_PROPERTY, "type", "Metastore Direct", env);
//                                        rtn = Boolean.FALSE;
//                                    }
//                                    if (isBlank(dbStore.getUri())) {
//                                        runStatus.addError(MISSING_PROPERTY, "uri", "Metastore Direct", env);
//                                        rtn = Boolean.FALSE;
//                                    }
//                                    if (isBlank(dbStore.getConnectionProperties().getProperty("user"))) {
//                                        runStatus.addError(MISSING_PROPERTY, "user", "Metastore Direct", env);
//                                        rtn = Boolean.FALSE;
//                                    }
//                                    if (isBlank(dbStore.getConnectionProperties().getProperty("password"))) {
//                                        runStatus.addError(MISSING_PROPERTY, "password", "Metastore Direct", env);
//                                        rtn = Boolean.FALSE;
//                                    }
//                                }
//                            }
//                        }
//                    }
                }
                break;
            // Need Left cluster defined with HS2 config.
            case STORAGE_MIGRATION:
                // Check for Metastore Direct on LEFT.
            case DUMP:
                // Drop the Right cluster to prevent confusion.
                config.getClusters().remove(Environment.RIGHT);
                break;

        }

//        if (config.isEvaluatePartitionLocation() && !config.isLoadingTestData()) {
//            switch (config.getDataStrategy()) {
//                case SCHEMA_ONLY:
//                case DUMP:
//                case STORAGE_MIGRATION:
//                    // Check the metastore_direct config on the LEFT.
//                    if (isNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
//                        runStatus.addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
//                        rtn = Boolean.FALSE;
//                    }
//                    runStatus.addWarning(ALIGN_LOCATIONS_WITH_DB);
//                    break;
//                default:
//                    runStatus.addError(EVALUATE_PARTITION_LOCATION_USE);
//                    rtn = Boolean.FALSE;
//            }
//        }

        // Set maxConnections to Concurrency.
        // Don't validate connections or url's if we're working with test data.
        if (!config.isLoadingTestData()) {
            HiveServer2Config leftHS2 = config.getCluster(Environment.LEFT).getHiveServer2();
            if (!leftHS2.isValidUri()) {
                rtn = Boolean.FALSE;
                runStatus.addError(LEFT_HS2_URI_INVALID);
            }

            if (isBlank(leftHS2.getJarFile())) {
                rtn = Boolean.FALSE;
                runStatus.addError(LEFT_HS2_DRIVER_JARS);
            }

            if (nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                DBStore leftMS = config.getCluster(Environment.LEFT).getMetastoreDirect();

                if (isBlank(leftMS.getUri())) {
//                    rtn = Boolean.FALSE;
                    runStatus.addWarning(LEFT_METASTORE_URI_NOT_DEFINED);
                }
            }

            if (nonNull(config.getCluster(Environment.RIGHT))) {
                // TODO: Add validation for -rid (right-is-disconnected) option.
                // - Only applies to SCHEMA_ONLY, SQL, EXPORT_IMPORT, and HYBRID data strategies.
                // -
                //
                if (nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
                    HiveServer2Config rightHS2 = config.getCluster(Environment.RIGHT).getHiveServer2();

                    if (isBlank(rightHS2.getJarFile())) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(RIGHT_HS2_DRIVER_JARS);
                    }

                    if (config.getDataStrategy() != STORAGE_MIGRATION
                            && !rightHS2.isValidUri()) {
                        if (!config.getDataStrategy().equals(DataStrategyEnum.DUMP)) {
                            rtn = Boolean.FALSE;
                            runStatus.addError(RIGHT_HS2_URI_INVALID);
                        }
                    } else {


                        if (leftHS2.isKerberosConnection()
                                && rightHS2.isKerberosConnection()
                                && (config.getCluster(Environment.LEFT).isLegacyHive() != config.getCluster(Environment.RIGHT).isLegacyHive())) {
                            rtn = Boolean.FALSE;
                            runStatus.addError(KERB_ACROSS_VERSIONS);
                        }
                    }
                }
            } else {
                if (!(config.getDataStrategy() == STORAGE_MIGRATION
                        || config.getDataStrategy() == DataStrategyEnum.DUMP)) {
                    if (!config.getMigrateACID().isDowngradeInPlace()) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(RIGHT_HS2_DEFINITION_MISSING);
                    }
                }
            }
        }

        return rtn;
    }

    public Boolean validate(ExecuteSession session, CliEnvironment cli) {
        AtomicReference<Boolean> rtn = new AtomicReference<>(Boolean.TRUE);

        HmsMirrorConfig config = session.getConfig();

        RunStatus runStatus = session.getRunStatus();

        if (isNull(runStatus)) {
            // Needed to hold errors and warnings.
            runStatus = new RunStatus();
            session.setRunStatus(runStatus);
        } else {
            // Reset RunStatus
            runStatus.clearErrors();
            runStatus.clearWarnings();
        }
        // Set distcp options.
        canDeriveDistcpPlan(session);

        // Both of these can't be set together.
        if (!isBlank(config.getDbRename()) || !isBlank(config.getDbPrefix())) {
            rtn.set(Boolean.FALSE);
            session.addError(DISTCP_W_RENAME_NOT_SUPPORTED);
        }

        // Before Validation continues, let's make some adjustments to the configuration to
        // ensure we're in a valid state.
        rtn.set(alignConfigurationSettings(session, config));

        switch (config.getDataStrategy()) {
            case DUMP:
                break;
            case STORAGE_MIGRATION:
                // This strategy not available for Legacy Hive.
                if (config.getCluster(Environment.LEFT).isLegacyHive()) {
                    runStatus.addError(STORAGE_MIGRATION_NOT_AVAILABLE_FOR_LEGACY, Environment.LEFT);
                    rtn.set(Boolean.FALSE);
                }

                // When using the Aligned movement strategy, we MUST have access to the Metastore Direct to pull
                // All the location details for the tables.
                if (config.getCluster(Environment.LEFT).getMetastoreDirect() == null) {
                    session.addError(METASTORE_DIRECT_NOT_DEFINED_OR_CONFIGURED, Environment.LEFT.toString());
                    rtn.set(Boolean.FALSE);
                }

                // When STORAGE_MIGRATION and HDP3, we need to force external location AND
                //   we can't use the ACID options.
                if (config.getCluster(Environment.LEFT).isHdpHive3()) {
                    config.getTranslator().setForceExternalLocation(Boolean.TRUE);
                    if (config.getMigrateACID().isOn() &&
                            !config.getTransfer().getStorageMigration().isDistcp()) {
                        session.addError(HIVE3_ON_HDP_ACID_TRANSFERS);
                        rtn.set(Boolean.FALSE);
                    }
                }

                break;
            case ICEBERG_CONVERSION:
                break;
            case SCHEMA_ONLY:
                // Because the ACID downgrade requires some SQL transformation, we can't do this via SCHEMA_ONLY.
                if (config.getMigrateACID().isOn() &&
                        config.getMigrateACID().isDowngrade()) {
                    runStatus.addError(ACID_DOWNGRADE_SCHEMA_ONLY);
                    rtn.set(Boolean.FALSE);
                }
                if (config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.RELATIVE
                        && config.getTransfer().getStorageMigration().getDataMovementStrategy() == DataMovementStrategyEnum.DISTCP) {
                    runStatus.addWarning(DISTCP_W_RELATIVE);
                }
                if (config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.RELATIVE
                        && config.getTransfer().getStorageMigration().getDataMovementStrategy() == DataMovementStrategyEnum.MANUAL) {
                    runStatus.addWarning(RELATIVE_MANUAL);
                }
            default:

                if (nonNull(config.getCluster(Environment.RIGHT)) && config.getCluster(Environment.RIGHT).isHdpHive3()) {
                    config.getTranslator().setForceExternalLocation(Boolean.TRUE);
                    runStatus.addWarning(HDP3_HIVE);

                }
                // Check for INPLACE DOWNGRADE, in which case no RIGHT needs to be defined or check.
                if (!config.getMigrateACID().isDowngradeInPlace()) {
                    if (config.getCluster(Environment.RIGHT).isLegacyHive() &&
                            !config.getCluster(Environment.LEFT).isLegacyHive() &&
                            !config.isDumpTestData()) {
                        runStatus.addError(NON_LEGACY_TO_LEGACY);
                        rtn.set(Boolean.FALSE);
                    }
                }
        }

        // Ozone Volume Name Check.
        // If the target namespace is set to ofs:// and we have warehouse plans defined for movement, ensure the
        //   volume name is at least 3 characters long.
        if (!isBlank(config.getTransfer().getTargetNamespace()) &&
                config.getTransfer().getTargetNamespace().startsWith("ofs://")) {
            if (!config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().isEmpty()) {
                RunStatus finalRunStatus = runStatus;
                config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().forEach((k, v) -> {
                    String externalDirectory = v.getExternalDirectory();
                    String managedDirectory = v.getManagedDirectory();
                    if (nonNull(externalDirectory) && Objects.requireNonNull(NamespaceUtils.getFirstDirectory(externalDirectory)).length() < 3) {
                        finalRunStatus.addError(OZONE_VOLUME_NAME_TOO_SHORT);
                        rtn.set(Boolean.FALSE);
                    }
                    if (nonNull(managedDirectory) && Objects.requireNonNull(NamespaceUtils.getFirstDirectory(managedDirectory)).length() < 3) {
                        finalRunStatus.addError(OZONE_VOLUME_NAME_TOO_SHORT);
                        rtn.set(Boolean.FALSE);
                    }
                });
            }
        }


        if (nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
            DBStore leftMS = config.getCluster(Environment.LEFT).getMetastoreDirect();
            if (isBlank(leftMS.getUri())) {
                runStatus.addWarning(LEFT_METASTORE_URI_NOT_DEFINED);
            }
        }

        // Check for valid acid downgrade scenario.
        // Can't downgrade without SQL.
        if (config.getMigrateACID().isDowngradeInPlace()) {
            if (config.getDataStrategy() != DataStrategyEnum.SQL) {
                runStatus.addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn.set(Boolean.FALSE);
            }
        }

        // Messages about what controls the way the databases are filtered.
        switch (config.getDataStrategy()) {
            case STORAGE_MIGRATION:
                runStatus.addWarning(DATASTRATEGY_FILTER_CONTROLLED_BY, config.getDataStrategy().toString(), "Warehouse Plans");
                break;
            case DUMP:
            case SCHEMA_ONLY:
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
            case COMMON:
            case LINKED:
            default:
                switch (config.getDatabaseFilterType()) {
                    case MANUAL:
                        runStatus.addWarning(DATABASE_FILTER_CONTROLLED_BY, "Manual Database Input");
                        break;
                    case WAREHOUSE_PLANS:
                        runStatus.addWarning(DATABASE_FILTER_CONTROLLED_BY, "Warehouse Plans");
                        break;
                    case REGEX:
                        runStatus.addWarning(DATABASE_FILTER_CONTROLLED_BY, "RegEx Filter");
                        break;
                    case UNDETERMINED:
                        break;
                }
                break;
        }


        if (config.loadMetadataDetails()) {
            runStatus.addWarning(ALIGNED_DISTCP_EXECUTE);
            // We can't move forward in this condition without some warehouse plans.
            if (!doWareHousePlansExist(session)) {
                runStatus.addError(ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS);
                rtn.set(Boolean.FALSE);
            }
            if (config.getFilter().isTableFiltering()) {
                runStatus.addWarning(DISTCP_W_TABLE_FILTERS);
            } else {
                runStatus.addWarning(DISTCP_WO_TABLE_FILTERS);
            }
            // Need to work through the ACID Inplace Downgrade.
            if (config.getDataStrategy() == DataStrategyEnum.SQL
                    && config.getMigrateACID().isOn()
                    && config.getMigrateACID().isDowngrade()
                    && (!doWareHousePlansExist(session))) {
                runStatus.addError(SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE);
                rtn.set(Boolean.FALSE);
            }
            if (config.getDataStrategy() == DataStrategyEnum.SQL) {
                // For SQL, we can only migrate ACID tables with `distcp` if we're downgrading of them.
                if (config.getMigrateACID().isOn() ||
                        config.getMigrateACID().isOnly()) {
                    if (!config.getMigrateACID().isDowngrade()) {
                        runStatus.addError(SQL_DISTCP_ONLY_W_DA_ACID);
                        rtn.set(Boolean.FALSE);
                    }
                }
//                if (!isBlank(config.getTransfer().getTargetNamespace())) {
//                    runStatus.addError(SQL_DISTCP_ACID_W_STORAGE_OPTS);
//                    rtn.set(Boolean.FALSE);
//                }
            }


        }

        // TODO: Work to do on Force External Location.
        if (config.getTranslator().isForceExternalLocation()) {
            runStatus.addWarning(RDL_FEL_OVERRIDES);
        }

        // Can't LINK ACID tables. They'll need to turn ACID off.
        if (config.getDataStrategy() == DataStrategyEnum.LINKED) {
            if (config.getMigrateACID().isOn()) {
                session.addError(LINKED_NO_ACID_SUPPORT);
                rtn.set(Boolean.FALSE);
            }
        }

        // Only allow db rename with a single database.
        if (!isBlank(config.getDbRename()) &&
                config.getDatabases().size() > 1) {
            runStatus.addError(DB_RENAME_ONLY_WITH_SINGLE_DB_OPTION);
            rtn.set(Boolean.FALSE);
        }

        if (config.isLoadingTestData()) {
            if (config.getFilter().isTableFiltering()) {
                runStatus.addWarning(IGNORING_TBL_FILTERS_W_TEST_DATA);
            }
        }

        if (config.isSync()
                && (config.getFilter().getTblRegEx() != null
                || config.getFilter().getTblExcludeRegEx() != null)) {
            runStatus.addWarning(SYNC_TBL_FILTER);
        }

        if (config.isSync()
                && !(config.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || config.getDataStrategy() == DataStrategyEnum.LINKED ||
                config.getDataStrategy() == DataStrategyEnum.SQL ||
                config.getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT ||
                config.getDataStrategy() == DataStrategyEnum.HYBRID)) {
            runStatus.addError(VALID_SYNC_STRATEGIES);
            rtn.set(Boolean.FALSE);
        }

        if (config.getMigrateACID().isOn()
                && !(config.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || config.getDataStrategy() == DataStrategyEnum.DUMP
                || config.getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT
                || config.getDataStrategy() == DataStrategyEnum.HYBRID
                || config.getDataStrategy() == DataStrategyEnum.SQL
                || config.getDataStrategy() == STORAGE_MIGRATION)) {
            runStatus.addError(VALID_ACID_STRATEGIES);
            rtn.set(Boolean.FALSE);
        }

        if (config.getMigrateACID().isOn()
                && config.getMigrateACID().isInplace()) {
            if (!(config.getDataStrategy() == DataStrategyEnum.SQL)) {
                runStatus.addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn.set(Boolean.FALSE);
            }
            if (!isBlank(config.getTransfer().getTargetNamespace())) {
                runStatus.addError(COMMON_STORAGE_WITH_DA_IP);
                rtn.set(Boolean.FALSE);
            }
            if (!isBlank(config.getTransfer().getIntermediateStorage())) {
                runStatus.addError(INTERMEDIATE_STORAGE_WITH_DA_IP);
                rtn.set(Boolean.FALSE);
            }
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                runStatus.addError(DISTCP_W_DA_IP_ACID);
                rtn.set(Boolean.FALSE);
            }
            if (config.getCluster(Environment.LEFT).isLegacyHive()) {
                runStatus.addError(DA_IP_NON_LEGACY);
                rtn.set(Boolean.FALSE);
            }
        }


        // Check the ensure the Target Namespace is available.
        try {
            // Under this condition, there isn't a RIGHT cluster defined. So skip the check.
            String targetNamespace = null;
            switch (config.getDataStrategy()) {
                case DUMP:
                    // No check needed.
                    break;
                case SQL:
                    if (config.getMigrateACID().isDowngradeInPlace()) {
                        break;
                    } else {
                        targetNamespace = config.getTargetNamespace();
                    }
                default:
                    targetNamespace = config.getTargetNamespace();
                    break;
            }
        } catch (RequiredConfigurationException e) {
            runStatus.addError(TARGET_NAMESPACE_NOT_DEFINED);
            rtn.set(Boolean.FALSE);
        }

        // Because some just don't get that you can't do this...
        if (nonNull(config.getTransfer().getWarehouse())) {
            if ((!isBlank(config.getTransfer().getWarehouse().getManagedDirectory())) &&
                    (!isBlank(config.getTransfer().getWarehouse().getExternalDirectory()))) {
                // Make sure these aren't set to the same location.
                if (config.getTransfer().getWarehouse().getManagedDirectory().equals(config.getTransfer().getWarehouse().getExternalDirectory())) {
                    runStatus.addError(WAREHOUSE_DIRS_SAME_DIR, config.getTransfer().getWarehouse().getExternalDirectory()
                            , config.getTransfer().getWarehouse().getManagedDirectory());
                    rtn.set(Boolean.FALSE);
                }
            }
        }

        if (config.getDataStrategy() == DataStrategyEnum.ACID) {
            runStatus.addError(ACID_NOT_TOP_LEVEL_STRATEGY);
            rtn.set(Boolean.FALSE);
        }

        // Test to ensure the clusters are LINKED to support underlying functions.
        switch (config.getDataStrategy()) {
            case LINKED:
                if (config.getTransfer().getTargetNamespace() != null) {
                    runStatus.addError(COMMON_STORAGE_WITH_LINKED);
                    rtn.set(Boolean.FALSE);
                }
                if (!isBlank(config.getTransfer().getIntermediateStorage())) {
                    runStatus.addError(INTERMEDIATE_STORAGE_WITH_LINKED);
                    rtn.set(Boolean.FALSE);
                }
            case HYBRID:
            case EXPORT_IMPORT:
            case SQL:
                // Only do link test when NOT using intermediate storage.
                // Downgrade inplace is a single cluster effort.
                if (!config.getMigrateACID().isDowngradeInPlace()) {
                    if (config.getCluster(Environment.RIGHT).getHiveServer2() != null
                            && !config.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()
                            && isBlank(config.getTransfer().getIntermediateStorage())
                    ) {

                        try {
                            if (!config.getMigrateACID().isDowngradeInPlace() && !linkTest(session, cli)) {
                                runStatus.addError(LINK_TEST_FAILED);
                                rtn.set(Boolean.FALSE);
                            }
                        } catch (DisabledException e) {
                            log.error("Link Test Skipped because the CLI Interface is disabled.");
                            runStatus.addError(LINK_TEST_FAILED);
                            rtn.set(Boolean.FALSE);
                        }
                    } else {
                        runStatus.addWarning(LINK_TEST_SKIPPED_WITH_OPTIONS);
                    }
                }
                break;
            case SCHEMA_ONLY:
                if (config.isCopyAvroSchemaUrls()) {
                    log.info("CopyAVROSchemaUrls is TRUE, so the cluster must be linked to do this.  Testing...");
                    try {
                        if (!linkTest(session, cli)) {
                            runStatus.addError(LINK_TEST_FAILED);
                            rtn.set(Boolean.FALSE);
                        }
                    } catch (DisabledException e) {
                        log.error("Link Test Skipped because the CLI Interface is disabled.");
                        runStatus.addError(LINK_TEST_FAILED);
                        rtn.set(Boolean.FALSE);
                    }
                }
                break;
            case DUMP:
                if (config.getDumpSource() == Environment.RIGHT) {
                    runStatus.addWarning(DUMP_ENV_FLIP);
                }
            case COMMON:
                break;
            case CONVERT_LINKED:
                // Check that the RIGHT cluster is NOT a legacy cluster.  No testing done in this scenario.
                if (config.getCluster(Environment.RIGHT).isLegacyHive()) {
                    runStatus.addError(LEGACY_HIVE_RIGHT_CLUSTER);
                    rtn.set(Boolean.FALSE);
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

        if (config.isReplace()) {
            if (config.getDataStrategy() != DataStrategyEnum.SQL) {
                runStatus.addError(REPLACE_ONLY_WITH_SQL);
                rtn.set(Boolean.FALSE);
            }
            if (config.getMigrateACID().isOn()) {
                if (!config.getMigrateACID().isDowngrade()) {
                    runStatus.addError(REPLACE_ONLY_WITH_DA);
                    rtn.set(Boolean.FALSE);
                }
            }
        }

        if (config.isReadOnly()) {
            switch (config.getDataStrategy()) {
                case SCHEMA_ONLY:
                case LINKED:
                case COMMON:
                case SQL:
                    break;
                default:
                    runStatus.addError(RO_VALID_STRATEGIES);
                    rtn.set(Boolean.FALSE);
            }
        }

        if (config.getCluster(Environment.RIGHT) != null) {
            if (config.getDataStrategy() != DataStrategyEnum.SCHEMA_ONLY &&
                    config.getCluster(Environment.RIGHT).isCreateIfNotExists()) {
                runStatus.addWarning(CINE_WITH_DATASTRATEGY);
            }
        }

        if (config.getTranslator().getOrderedGlobalLocationMap() != null) {
            // Validate that none of the 'from' maps overlap.  IE: can't have /data and /data/mydir as from locations.
            //    For items that match /data/mydir maybe confusing as to which one to adjust.
            //   OR we move this to a TreeMap and supply a custom comparator the sorts by length, then natural.  This
            //      will push longer paths to be evaluated first and once a match is found, skip further checks.

        }

        // TODO: Check the connections.
        // If the environments are mix of legacy and non-legacy, check the connections for kerberos or zk.


        if (rtn.get()) {
            // Last check for errors.
            if (runStatus.hasErrors()) {
                rtn.set(Boolean.FALSE);
            }
        }
//        runStatus.setConfigValidated(rtn);
        return rtn.get();
    }

    public HmsMirrorConfig flipConfig(HmsMirrorConfig config) {
        if (config != null) {
            Cluster leftClone = config.getCluster(Environment.LEFT).clone();
            leftClone.setEnvironment(Environment.RIGHT);
            Cluster rightClone = config.getCluster(Environment.RIGHT).clone();
            rightClone.setEnvironment(Environment.LEFT);
            config.getClusters().put(Environment.RIGHT, leftClone);
            config.getClusters().put(Environment.LEFT, rightClone);
        }
        return config;
    }

}
