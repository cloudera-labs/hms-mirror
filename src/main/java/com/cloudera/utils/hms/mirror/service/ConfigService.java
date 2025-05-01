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
import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.SQL;
import static com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.STORAGE_MIGRATION;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Service class responsible for managing HMS Mirror configuration operations.
 * This class handles configuration validation, loading, saving, and various configuration-related
 * operations for the HMS Mirror tool. It ensures that configurations are valid for different
 * data strategies and cluster setups.
 *
 * Key responsibilities include:
 * - Configuration validation for different data strategies
 * - Cluster connection validation
 * - Configuration adjustments based on data strategies
 * - Warehouse and namespace validation
 * - Link testing between clusters
 * - Configuration file operations (load/save)
 */
@Service
@Slf4j
@Getter
public class ConfigService {

    private final ObjectMapper yamlMapper;
    private final org.springframework.core.env.Environment springEnv;
    private final DomainService domainService;
//    private TranslatorService translatorService;

    /**
     * Constructor for ConfigService.
     *
     * @param yamlMapper ObjectMapper for YAML processing
     * @param springEnv Spring environment
     * @param domainService Service for domain operations
     */
    public ConfigService(ObjectMapper yamlMapper, 
                        org.springframework.core.env.Environment springEnv, 
                        DomainService domainService) {
        this.yamlMapper = yamlMapper;
        this.springEnv = springEnv;
        this.domainService = domainService;
        log.debug("ConfigService initialized");
    }

    /**
     * Checks if warehouse plans exist in the configuration.
     * This method verifies if either warehouse map builder plans are defined or
     * if warehouse directories (external and managed) are configured.
     *
     * @param session The execute session containing the configuration
     * @return true if warehouse plans exist, false otherwise
     */
    public boolean doWareHousePlansExist(ExecuteSession session) {
        HmsMirrorConfig config = session.getConfig();
        
        WarehouseMapBuilder warehouseMapBuilder = config.getTranslator().getWarehouseMapBuilder();
        if (!warehouseMapBuilder.getWarehousePlans().isEmpty()) {
            return true;
        }
        
        Warehouse warehouse = config.getTransfer().getWarehouse();
        return nonNull(warehouse) && !(isBlank(warehouse.getExternalDirectory())
                || isBlank(warehouse.getManagedDirectory()));
    }

    /**
     * Checks if metastore direct configuration is available for a specific environment.
     *
     * @param session The execute session containing the configuration
     * @param environment The environment (LEFT/RIGHT) to check
     * @return true if metastore direct is configured, false otherwise
     */
    public boolean isMetastoreDirectConfigured(ExecuteSession session, Environment environment) {
        HmsMirrorConfig config = session.getConfig();
        
        if (nonNull(config.getCluster(environment).getMetastoreDirect())) {
            DBStore dbStore = config.getCluster(environment).getMetastoreDirect();
            return !isBlank(dbStore.getUri());
        }
        return false;
    }

    /**
     * Determines if a DistCp plan can be derived from the current configuration.
     *
     * @param session The execute session containing the configuration
     * @return true if DistCp plan can be derived, false otherwise
     */
    public boolean canDeriveDistcpPlan(ExecuteSession session) {
        HmsMirrorConfig config = session.getConfig();
        return config.getTransfer().getStorageMigration().isDistcp();
    }

    /**
     * Converts the configuration to a string representation, masking sensitive information.
     * This method converts the configuration to YAML format and masks passwords for security.
     *
     * @param config The HMS Mirror configuration to convert
     * @return String representation of the configuration with masked sensitive data
     */
    public String configToString(HmsMirrorConfig config) {
        String rtn = null;
        try {
            rtn = yamlMapper.writeValueAsString(config);
            // Blank out passwords
            rtn = rtn.replaceAll("user:\\s\".*\"", "user: \"*****\"");
            rtn = rtn.replaceAll("password:\\s\".*\"", "password: \"*****\"");
        } catch (JsonProcessingException e) {
            log.error("Parsing issue", e);
        }
        return rtn;
    }

    /**
     * Aligns configuration settings based on the data strategy and other parameters.
     * This method adjusts various configuration settings to ensure they are compatible
     * with the selected data strategy and other configuration options.
     *
     * @param session The execute session containing the configuration
     * @param config The HMS Mirror configuration to align
     * @return true if alignment was successful without breaking errors, false otherwise
     */
    public boolean alignConfigurationSettings(ExecuteSession session, HmsMirrorConfig config) {
        boolean rtn = Boolean.TRUE;
        // Iceberg Conversions is a beta feature.
        if (config.getIcebergConversion().isEnable()) {
            // Check that the DataStategy is either STORAGE_MIGRATION or SQL before allowing the Iceberg Conversion can be set.
            if (!EnumSet.of(STORAGE_MIGRATION, SQL).contains(config.getDataStrategy())) {
                session.addConfigAdjustmentMessage(config.getDataStrategy(),
                        "icebergConversion:enabled",
                        Boolean.toString(config.getIcebergConversion().isEnable()),
                        Boolean.toString(false), "Only available w/ Data Strategies STORAGE_MIGRATION and SQL.");
                config.getIcebergConversion().setEnable(false);
            }
        }

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
                if (config.getIcebergConversion().isEnable() &&
                        config.getTransfer().getStorageMigration().getDataMovementStrategy() != DataMovementStrategyEnum.SQL) {
                    session.addConfigAdjustmentMessage(config.getDataStrategy(),
                            "DataMovementStrategy",
                            config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                            DataMovementStrategyEnum.SQL.toString(), "Only the SQL Data Movement Strategy is supported for STORAGE_MIGRATION w/ ICEBERG_CONVERSION enabled.");
                    config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                }

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
        // If migrateView is on and the data strategy is NOT either DUMP or SCHEMA_ONLY,
        //  change to data strategy to SCHEMA_ONLY.
        if (config.getMigrateVIEW().isOn() &&
                !(config.getDataStrategy() == DataStrategyEnum.DUMP ||
                config.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY)) {
            session.addConfigAdjustmentMessage(config.getDataStrategy(),
                    "DataStrategy",
                    config.getDataStrategy().toString(),
                    DataStrategyEnum.SCHEMA_ONLY.toString(),
                    "MigrateVIEW is only valid for DUMP and SCHEMA_ONLY Data Strategies.");
            config.setDataStrategy(DataStrategyEnum.SCHEMA_ONLY);
        }

        if (config.loadMetadataDetails()) {
            switch (config.getDatabaseFilterType()) {
                case WAREHOUSE_PLANS:
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

    /**
     * Gets the skip stats collection setting based on the configuration and data strategy.
     * This method determines whether statistics collection should be skipped based on
     * various configuration parameters and the selected data strategy.
     *
     * @param config The HMS Mirror configuration
     * @return Boolean indicating whether to skip stats collection
     */
    public Boolean getSkipStatsCollection(HmsMirrorConfig config) {
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

    /**
     * Determines if the current configuration represents a legacy migration.
     * A legacy migration is when one cluster is using legacy Hive and the other isn't.
     *
     * @param config The HMS Mirror configuration
     * @return true if this is a legacy migration, false otherwise
     */
    public Boolean legacyMigration(HmsMirrorConfig config) {
        Boolean rtn = Boolean.FALSE;

        if (config.getCluster(Environment.LEFT).isLegacyHive() != config.getCluster(Environment.RIGHT).isLegacyHive()) {
            if (config.getCluster(Environment.LEFT).isLegacyHive()) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    /**
     * Tests the link between clusters to validate namespace availability.
     * This method checks if the configured HCFS namespaces are accessible.
     *
     * @param session The execute session
     * @param cli The CLI environment
     * @return true if link test passes, false otherwise
     * @throws DisabledException if the CLI interface is disabled
     */
    protected Boolean linkTest(ExecuteSession session, CliEnvironment cli) throws DisabledException {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig config = session.getConfig();

        if (isNull(cli)) {
            return Boolean.TRUE;
        }

        if (config.isSkipLinkCheck() || config.isLoadingTestData()) {
            log.warn("Skipping Link Check.");
            rtn = Boolean.TRUE;
        } else {

            log.info("Performing Cluster Link Test to validate cluster 'hcfsNamespace' availability.");

            // We need to gather and test the configured hcfs namespaces defined in the config.
            // LEFT, RIGHT, and TargetNamespace.
            // If a namespace is NOT defined, record an issue and move on.  Only 'fail' when a defined namespace fails.
            List<String> namespaces = new ArrayList<>();

            if (nonNull(config.getCluster(Environment.LEFT)) && !isBlank(config.getCluster(Environment.LEFT).getHcfsNamespace())) {
                namespaces.add(config.getCluster(Environment.LEFT).getHcfsNamespace());
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && !isBlank(config.getCluster(Environment.RIGHT).getHcfsNamespace())) {
                namespaces.add(config.getCluster(Environment.RIGHT).getHcfsNamespace());
            }
            if (!isBlank(config.getTransfer().getTargetNamespace())) {
                namespaces.add(config.getTransfer().getTargetNamespace());
            }

            for (String namespace : namespaces) {
                try {
                    if (NamespaceUtils.isNamespaceAvailable(cli, namespace)) {
                        log.info("Namespace: {} is available.", namespace);
                    } else {
                        log.warn("Namespace: {} is not available.", namespace);
                        rtn = Boolean.FALSE;
                    }
                } catch (DisabledException de) {
                    log.warn("Namespace: {} is not available because the hcfs client has been disabled.", namespace);
                }
            }

        }
        return rtn;
    }

    /**
     * Loads a configuration from the default config directory.
     *
     * @param configFileName The name of the configuration file to load
     * @return The loaded HMS Mirror configuration
     */
    public HmsMirrorConfig loadConfig(String configFileName) {
        HmsMirrorConfig rtn = domainService.deserializeConfig(configFileName);
        return rtn;
    }

    /**
     * Saves the configuration to a file.
     *
     * @param config The configuration to save
     * @param configFileName The name of the file to save to
     * @param overwrite Whether to overwrite an existing file
     * @return true if save was successful, false otherwise
     * @throws IOException if there is an error writing the file
     */
    public boolean saveConfig(HmsMirrorConfig config, String configFileName, Boolean overwrite) throws IOException {
        return HmsMirrorConfig.save(config, configFileName, overwrite);
    }

    /**
     * Overlays one configuration onto another, merging their settings.
     *
     * @param config The base configuration
     * @param overlay The configuration to overlay on top
     */
    public void overlayConfig(HmsMirrorConfig config, HmsMirrorConfig overlay) {
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

    /**
     * Sets default configuration values based on the data strategy.
     *
     * @param config The configuration to set defaults for
     */
    public void setDefaultsForDataStrategy(HmsMirrorConfig config) {
        // Set Attribute for the config.
        switch (config.getDataStrategy()) {
            case DUMP:
                break;
            case STORAGE_MIGRATION:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
                break;
            case SCHEMA_ONLY:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                break;
            case SQL:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                break;
            case EXPORT_IMPORT:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.EXPORT_IMPORT);
                break;
            case HYBRID:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.HYBRID);
                break;
            case COMMON:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                break;
            case LINKED:
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

    /**
     * Creates a new configuration for a specific data strategy with appropriate defaults.
     *
     * @param dataStrategy The data strategy to create configuration for
     * @return A new HMS Mirror configuration
     */
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
            default:
                break;
        }

        setDefaultsForDataStrategy(rtn);
        return rtn;
    }

    /**
     * Validates the configuration for cluster connections.
     * This method performs comprehensive validation of connection-related configurations including:
     * - Password encryption and key validation
     * - Cluster configuration validation based on data strategy
     * - HiveServer2 and Metastore connection configuration validation
     * - URI validation for both LEFT and RIGHT clusters
     * - Driver JAR file validation
     * - Kerberos configuration validation across versions
     *
     * @param session The execute session containing the configuration to validate
     * @return Boolean TRUE if all connection configurations are valid, FALSE otherwise.
     *         The method will add appropriate error messages to the session's RunStatus for any validation failures.
     */
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
                if (config.getMigrateACID().isInplace()) {
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
                    if (!config.getMigrateACID().isInplace()) {
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

        // ==============================================================================================================

        // Validate the jar files listed in the configs for each cluster.
        // Visible Environment Variables:
        // Don't validate when using test data.
        if (!config.isLoadingTestData()) {
            Environment[] envs = Environment.getVisible();
            for (Environment env : envs) {
                Cluster cluster = config.getCluster(env);
                if (nonNull(cluster)) {
                    if (nonNull(cluster.getHiveServer2())) {
                        if (isBlank(cluster.getHiveServer2().getJarFile())) {
                            runStatus.addError(HS2_DRIVER_JARS_MISSING, env);
                            rtn.set(Boolean.FALSE);
                        } else {
                            String[] jarFiles = cluster.getHiveServer2().getJarFile().split("\\:");
                            // Go through each jar file and validate it exists.
                            for (String jarFile : jarFiles) {
                                if (!new File(jarFile).exists()) {
                                    runStatus.addError(HS2_DRIVER_JAR_NOT_FOUND, jarFile, env);
                                    rtn.set(Boolean.FALSE);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Set distcp options.
        canDeriveDistcpPlan(session);

        // When doing STORARE_MIGRATION using DISTCP, we can alter the table location of an ACID table and 'distcp' the
        // data to the new location without any other changes because the filesystem directory names will still match the
        // metastores internal transactional values.
        // BUT, if you've asked to save an ARCHIVE of the table and use 'distcp', we need to 'create' the new table, which
        // will not maintain the ACID properties. This is an invalid state.
        if (config.getDataStrategy() == STORAGE_MIGRATION) {
            if (config.getTransfer().getStorageMigration().isDistcp() &&
                    config.getMigrateACID().isOn() &&
                    config.getTransfer().getStorageMigration().isCreateArchive()) {
                session.addError(STORAGE_MIGRATION_ACID_ARCHIVE_DISTCP);
                rtn.set(Boolean.FALSE);
            }
        }

        // Both of these can't be set together.
        if (!isBlank(config.getDbRename()) && !isBlank(config.getDbPrefix())) {
            rtn.set(Boolean.FALSE);
            session.addError(CONFLICTING_PROPERTIES, "dbRename", "dbPrefix");
        }

        if ((isNull(config.getDatabases()) || config.getDatabases().isEmpty()) && (isBlank(config.getFilter().getDbRegEx()))) {
            session.addError(MISC_ERROR, "No databases specified OR found if you used dbRegEx");
            rtn.set(Boolean.FALSE);
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
                if (!config.getMigrateACID().isInplace()) {
                    if (config.getCluster(Environment.RIGHT).isLegacyHive() &&
                            !config.getCluster(Environment.LEFT).isLegacyHive() &&
                            !config.isDumpTestData()) {
                        runStatus.addError(NON_LEGACY_TO_LEGACY);
                        rtn.set(Boolean.FALSE);
                    }
                } else {
                    // Drop the Right cluster to prevent confusion.
                    config.getClusters().remove(Environment.RIGHT);
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
        if (config.getMigrateACID().isInplace()) {
            if (config.getDataStrategy() != SQL) {
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


        if (config.loadMetadataDetails() && config.getTransfer().getStorageMigration().isDistcp()) {
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
            if (config.getDataStrategy() == SQL
                    && config.getMigrateACID().isOn()
                    && config.getMigrateACID().isDowngrade()
                    && (!doWareHousePlansExist(session))) {
                runStatus.addError(SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE);
                rtn.set(Boolean.FALSE);
            }
            if (config.getDataStrategy() == SQL) {
                // For SQL, we can only migrate ACID tables with `distcp` if we're downgrading of them.
                if (config.getMigrateACID().isOn() ||
                        config.getMigrateACID().isOnly()) {
                    if (!config.getMigrateACID().isDowngrade() && config.getTransfer().getStorageMigration().isDistcp()) {
                        runStatus.addError(SQL_DISTCP_ONLY_W_DA_ACID);
                        rtn.set(Boolean.FALSE);
                    }
                }
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
                config.getDataStrategy() == SQL ||
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
                || config.getDataStrategy() == SQL
                || config.getDataStrategy() == STORAGE_MIGRATION)) {
            runStatus.addError(VALID_ACID_STRATEGIES);
            rtn.set(Boolean.FALSE);
        }

        if (config.getMigrateACID().isOn()
                && config.getMigrateACID().isInplace()) {
            if (!(config.getDataStrategy() == SQL)) {
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
                case EXPORT_IMPORT:
                case SQL:
                    if (config.getMigrateACID().isInplace()) {
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
                if (!config.getMigrateACID().isInplace()) {
                    if (config.getCluster(Environment.RIGHT).getHiveServer2() != null
                            && !config.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()
                            && isBlank(config.getTransfer().getIntermediateStorage())
                    ) {

                        try {
                            if (!config.getMigrateACID().isInplace() && !linkTest(session, cli)) {
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

        if (config.isReplace()) {
            if (config.getDataStrategy() != SQL) {
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
        return rtn.get();
    }

    /**
     * Flips the configuration between LEFT and RIGHT clusters.
     * This method creates a new configuration with the LEFT and RIGHT cluster configurations swapped.
     *
     * @param config The configuration to flip
     * @return The flipped configuration
     */
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
