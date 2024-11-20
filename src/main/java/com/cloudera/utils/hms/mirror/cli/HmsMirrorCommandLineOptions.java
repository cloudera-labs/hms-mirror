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

package com.cloudera.utils.hms.mirror.cli;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.reporting.ReportingConf;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.util.*;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Configuration
@Order(5)
@Slf4j
@Getter
@Setter
public class HmsMirrorCommandLineOptions {
    public static String SPRING_CONFIG_PREFIX = "hms-mirror.config";

    private ConfigService configService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public static void main(String[] args) {
        HmsMirrorCommandLineOptions pcli = new HmsMirrorCommandLineOptions();
        String[] convertedArgs = pcli.toSpringBootOption(Boolean.TRUE, args);
        String newCmdLn = String.join(" ", convertedArgs);
        System.out.println(newCmdLn);
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.debug-dir")
    CommandLineRunner runDebugSession(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.acid-partition-count}") String value) {
        return args -> {
            log.info("acid-partition-count: {}", value);
            hmsMirrorConfig.getMigrateACID().setPartitionLimit(Integer.parseInt(value));
        };
    }


    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.acid-partition-count")
    CommandLineRunner configAcidPartitionCount(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.acid-partition-count}") String value) {
        return args -> {
            log.info("acid-partition-count: {}", value);
            hmsMirrorConfig.getMigrateACID().setPartitionLimit(Integer.parseInt(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.auto-tune",
            havingValue = "true")
    CommandLineRunner configAutoTuneTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("auto-tune: {}", Boolean.TRUE);
            hmsMirrorConfig.getOptimization().setAutoTune(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.auto-tune",
            havingValue = "false")
    CommandLineRunner configAutoTuneFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("auto-tune: {}", Boolean.FALSE);
            hmsMirrorConfig.getOptimization().setAutoTune(Boolean.FALSE);
        };
    }


    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.avro-schema-migration",
            havingValue = "true")
    CommandLineRunner configAvroSchemaMigrationTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("avro-schema-migration: {}", Boolean.TRUE);
            hmsMirrorConfig.setCopyAvroSchemaUrls(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.avro-schema-migration",
            havingValue = "false")
    CommandLineRunner configAvroSchemaMigrationFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("avro-schema-migration: {}", Boolean.FALSE);
            hmsMirrorConfig.setCopyAvroSchemaUrls(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.beta",
            havingValue = "true")
    CommandLineRunner configBetaTrue(HmsMirrorConfig config) {
        return args -> {
            log.info("beta: {}", Boolean.TRUE);
            config.setBeta(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.beta",
            havingValue = "false")
    CommandLineRunner configBetaFalse(HmsMirrorConfig config) {
        return args -> {
            log.info("beta: {}", Boolean.TRUE);
            config.setBeta(Boolean.FALSE);
        };
    }


    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.create-if-not-exist",
            havingValue = "true")
    CommandLineRunner configCineTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("create-if-not-exist: {}", Boolean.TRUE);
            if (nonNull(hmsMirrorConfig.getCluster(Environment.LEFT))) {
                hmsMirrorConfig.getCluster(Environment.LEFT).setCreateIfNotExists(Boolean.TRUE);
            }
            if (nonNull(hmsMirrorConfig.getCluster(Environment.RIGHT))) {
                hmsMirrorConfig.getCluster(Environment.RIGHT).setCreateIfNotExists(Boolean.TRUE);
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.create-if-not-exist",
            havingValue = "false")
    CommandLineRunner configCineFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("create-if-not-exist: {}", Boolean.FALSE);
            if (nonNull(hmsMirrorConfig.getCluster(Environment.LEFT))) {
                hmsMirrorConfig.getCluster(Environment.LEFT).setCreateIfNotExists(Boolean.FALSE);
            }
            if (nonNull(hmsMirrorConfig.getCluster(Environment.RIGHT))) {
                hmsMirrorConfig.getCluster(Environment.RIGHT).setCreateIfNotExists(Boolean.FALSE);
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.legacy-command-line-options")
    CommandLineRunner configCommandLineOptions(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.legacy-command-line-options}") String value) {
        return args -> {
            log.info("legacy-command-line-options: {}", value);
            hmsMirrorConfig.setCommandLineOptions(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.target-namespace")
    CommandLineRunner configTargetNamespace(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.target-namespace}") String value) {
        return args -> {
            log.info("target-namespace: {}", value);
            hmsMirrorConfig.getTransfer().setTargetNamespace(value);
            // This usually means an on-prem to cloud migration, which should be a PUSH data flow for distcp.
//            hmsMirrorConfig.getTransfer().getStorageMigration().setDataFlow(DistcpFlowEnum.PUSH);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.compress-text-output",
            havingValue = "true")
    CommandLineRunner configCompressTextOutputTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("compress-text-output: {}", Boolean.TRUE);
            hmsMirrorConfig.getOptimization().setCompressTextOutput(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.compress-text-output",
            havingValue = "false")
    CommandLineRunner configCompressTextOutputFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("compress-text-output: {}", Boolean.FALSE);
            hmsMirrorConfig.getOptimization().setCompressTextOutput(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.data-strategy")
    CommandLineRunner configDataStrategy(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.data-strategy}") String value) {
        return args -> {
            log.info("data-strategy: {}", value);
            hmsMirrorConfig.setDataStrategy(DataStrategyEnum.valueOf(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.database")
    CommandLineRunner configDatabase(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.database}") String dbs) {
        return args -> {
            log.info("database: {}", dbs);
            List<String> databases = Arrays.asList(dbs.split(","));
            Set<String> dbSet = new TreeSet<>(databases);
            hmsMirrorConfig.setDatabases(dbSet);
//            log.info("Concurrency: " + config.getTransfer().getConcurrency());
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.database-only",
            havingValue = "true")
    CommandLineRunner configDatabaseOnlyTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("database-only: {}", Boolean.TRUE);
            hmsMirrorConfig.setDatabaseOnly(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.database-only",
            havingValue = "false")
    CommandLineRunner configDatabaseOnlyFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("database-only: {}", Boolean.FALSE);
            hmsMirrorConfig.setDatabaseOnly(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.consolidate-tables-for-distcp",
            havingValue = "true")
    CommandLineRunner configConsolidateTablesForDistcpTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("consolidate-tables-for-distcp: {}", Boolean.TRUE);
            hmsMirrorConfig.getTransfer().getStorageMigration().setConsolidateTablesForDistcp(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.consolidate-tables-for-distcp",
            havingValue = "false")
    CommandLineRunner configConsolidateTablesForDistcpFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("consolidate-tables-for-distcp: {}", Boolean.FALSE);
            hmsMirrorConfig.getTransfer().getStorageMigration().setConsolidateTablesForDistcp(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.db-prefix")
    CommandLineRunner configDatabasePrefix(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.db-prefix}") String value) {
        return args -> {
            log.info("db-prefix: {}", value);
            hmsMirrorConfig.setDbPrefix(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.database-regex")
    CommandLineRunner configDatabaseRegEx(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.database-regex}") String value) {
        return args -> {
            log.info("database-regex: {}", value);
            hmsMirrorConfig.getFilter().setDbRegEx(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.db-rename")
    CommandLineRunner configDatabaseRename(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.db-rename}") String value) {
        return args -> {
            log.info("db-rename: {}", value);
            hmsMirrorConfig.setDbRename(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.distcp")
    CommandLineRunner configDistcp(HmsMirrorConfig hmsMirrorConfig,
                                   @Value("${hms-mirror.config.distcp}") String value) {
        return args -> {
            log.info("distcp: {}", value);
            if (Boolean.parseBoolean(value)) {
                hmsMirrorConfig.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
            } else {
                String flowStr = value;
                if (!isBlank(flowStr)) {
                    try {
                        DistcpFlowEnum flow = DistcpFlowEnum.valueOf(flowStr.toUpperCase(Locale.ROOT));
                        hmsMirrorConfig.getTransfer().getStorageMigration().setDataFlow(flow);
                    } catch (IllegalArgumentException iae) {
                        log.error("Optional argument for `distcp` is invalid. Valid values: {}", Arrays.toString(DistcpFlowEnum.values()));
                        throw new RuntimeException("Optional argument for `distcp` is invalid. Valid values: " +
                                Arrays.toString(DistcpFlowEnum.values()), iae);
                    }
                }
                hmsMirrorConfig.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
            }
        };
    }

    // data-movement-strategy
    @Bean
    @Order(2) // To override the distcp setting if different.
    @ConditionalOnProperty(
            name = "hms-mirror.config.data-movement-strategy")
    CommandLineRunner configDataMovementStrategy(HmsMirrorConfig config,
                                   @Value("${hms-mirror.config.data-movement-strategy}") String value) {
        return args -> {
            log.info("data-movement-strategy: {}", value);
            try {
                DataMovementStrategyEnum strategy = DataMovementStrategyEnum.valueOf(value);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(strategy);
            } catch (IllegalArgumentException iae) {
                log.error("Can't set data-movement-strategy with value: {}", value);
            }
        };
    }


    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.downgrade-acid",
            havingValue = "true")
    CommandLineRunner configDowngradeAcidTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("downgrade-acid: {}", Boolean.TRUE);
            hmsMirrorConfig.getMigrateACID().setDowngrade(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.downgrade-acid",
            havingValue = "false")
    CommandLineRunner configDowngradeAcidFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("downgrade-acid: {}", Boolean.FALSE);
            hmsMirrorConfig.getMigrateACID().setDowngrade(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.dump-source")
    CommandLineRunner configDumpSource(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.dump-source}") String value) {
        return args -> {
            log.info("dump-source: {}", value);
            if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.DUMP) {
                hmsMirrorConfig.setExecute(Boolean.FALSE); // No Actions.
                hmsMirrorConfig.setSync(Boolean.FALSE);

                try {
                    Environment source = Environment.valueOf(value.toUpperCase());
                    hmsMirrorConfig.setDumpSource(source);
                } catch (RuntimeException re) {
                    log.error("The `-ds` option should be either: (LEFT|RIGHT). {} is NOT a valid option.", value);
                    throw new RuntimeException("The `-ds` option should be either: (LEFT|RIGHT). " + value +
                            " is NOT a valid option.");
                }
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.dump-test-data")
    CommandLineRunner configDumpTestData(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.dump-test-data}") String value) {
        return args -> {
            log.info("dump-test-data: {}", value);
            hmsMirrorConfig.setDumpTestData(Boolean.parseBoolean(value));
        };
    }

        @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.align-locations")
    CommandLineRunner configAlignLocations(HmsMirrorConfig hmsMirrorConfig,
                                           @Value("${hms-mirror.config.align-locations}") boolean value) {
        return args -> {
            log.info("align-locations: {}", value);
            hmsMirrorConfig.getTransfer().getStorageMigration().setTranslationType(TranslationTypeEnum.ALIGNED);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.execute",
            havingValue = "true")
    CommandLineRunner configExecuteTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("execute: {}", Boolean.TRUE);
            hmsMirrorConfig.setExecute(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.execute",
            havingValue = "false")
    CommandLineRunner configExecuteFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("execute: {}", Boolean.FALSE);
            hmsMirrorConfig.setExecute(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.export-partition-count")
    CommandLineRunner configExportPartitionCount(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.export-partition-count}") String value) {
        return args -> {
            log.info("export-partition-count: {}", value);
            hmsMirrorConfig.getHybrid().setExportImportPartitionLimit(Integer.parseInt(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.external-warehouse-directory")
    CommandLineRunner configExternalWarehouseDirectory(HmsMirrorConfig config, @Value("${hms-mirror.config.external-warehouse-directory}") String value) {
        return args -> {
            log.info("external-warehouse-directory: {}", value);

            if (isNull(config.getTransfer().getWarehouse())) {
                Warehouse warehouse = new Warehouse();
                warehouse.setSource(WarehouseSource.GLOBAL);
                config.getTransfer().setWarehouse(warehouse);
            }
            String ewdStr = value;
            // Remove/prevent duplicate namespace config.
            if (nonNull(config.getTransfer().getTargetNamespace())) {
                if (ewdStr.startsWith(config.getTransfer().getTargetNamespace())) {
                    ewdStr = ewdStr.substring(config.getTransfer().getTargetNamespace().length());
                    log.warn("External Warehouse Location Modified (stripped duplicate namespace): {}", ewdStr);
                }
            }
            config.getTransfer().getWarehouse().setExternalDirectory(ewdStr);

        };
    }

    @Bean
    @Order(6)
    @ConditionalOnProperty(
            name = "hms-mirror.config.flip",
            havingValue = "true")
    CommandLineRunner configFlipTrue(HmsMirrorConfig config) {
        return args -> {
            log.info("flip: {}", Boolean.TRUE);
            configService.flipConfig(config);
//            hmsMirrorConfig.setFlip(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.flip",
            havingValue = "false")
    CommandLineRunner configFlipFalse(HmsMirrorConfig config) {
        return args -> {
            log.info("flip: {}", Boolean.FALSE);
//            hmsMirrorConfig.setFlip(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.force-external-location",
            havingValue = "true")
    CommandLineRunner configForceExternalLocationTrue(HmsMirrorConfig config) {
        return args -> {
            log.info("force-external-location: {}", Boolean.TRUE);
            config.getTranslator().setForceExternalLocation(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.force-external-location",
            havingValue = "false")
    CommandLineRunner configForceExternalLocationFalse(HmsMirrorConfig config) {
        return args -> {
            log.info("force-external-location: {}", Boolean.FALSE);
            config.getTranslator().setForceExternalLocation(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.global-location-map")
    CommandLineRunner configGlobalLocationMap(HmsMirrorConfig config, @Value("${hms-mirror.config.global-location-map}") String value) {
        return args -> {
            log.info("global-location-map: {}", value);
            String[] globalLocMap = value.split(",");
            for (String entry: globalLocMap) {
                config.addGlobalLocationMap(entry);
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.help")
    CommandLineRunner configHelp(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.help}") String value) {
        return args -> {
            log.info("help: {}", value);
//            config.
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.iceberg-table-property-overrides")
    CommandLineRunner configIcebergTablePropertyOverrides(HmsMirrorConfig config, @Value("${hms-mirror.config.iceberg-table-property-overrides}") String value) {
        return args -> {
            log.info("iceberg-table-property-overrides: {}", value);
            String[] overrides = value.split(",");
            if (nonNull(overrides))
                config.getIcebergConversion().setPropertyOverridesStr(overrides);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.iceberg-version")
    CommandLineRunner configIcebergVersion(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.iceberg-version}") String value) {
        return args -> {
            log.info("iceberg-version: {}", value);
            hmsMirrorConfig.getIcebergConversion().setVersion(Integer.parseInt(value));
        };
    }

    @Bean
    // So this happens AFTER migrate acid and migrate acid only are checked.
    @Order(5)
    @ConditionalOnProperty(
            name = "hms-mirror.config.in-place",
            havingValue = "true")
    CommandLineRunner configInPlace(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("in-place downgrade acid tables: {}", Boolean.TRUE);
            if (hmsMirrorConfig.getMigrateACID().isOn()) {
                // Downgrade ACID tables inplace
                // Only work on LEFT cluster definition.
                hmsMirrorConfig.getMigrateACID().setDowngrade(Boolean.TRUE);
                hmsMirrorConfig.getMigrateACID().setInplace(Boolean.TRUE);
                // For 'in-place' downgrade, only applies to ACID tables.
                // Implies `-mao`.
                log.info("Only ACID Tables will be looked at since 'ip' was specified.");
                hmsMirrorConfig.getMigrateACID().setOnly(Boolean.TRUE);
                // Remove RIGHT cluster and enforce mao
                log.info("RIGHT Cluster definition will be disconnected if exists since this is a LEFT cluster ONLY operation");
                if (nonNull(hmsMirrorConfig.getCluster(Environment.RIGHT)) && nonNull(hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2()))
                    hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2().setDisconnected(Boolean.TRUE);
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.intermediate-storage")
    CommandLineRunner configIntermediateStorage(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.intermediate-storage}") String value) {
        return args -> {
            log.info("intermediate-storage: {}", value);
            hmsMirrorConfig.getTransfer().setIntermediateStorage(value);
            // This usually means an on-prem to cloud migration, which should be a PUSH data flow for distcp from the
            // LEFT and PULL from the RIGHT.
            hmsMirrorConfig.getTransfer().getStorageMigration().setDataFlow(DistcpFlowEnum.PUSH_PULL);
        };
    }

    /*
    This is taken care of thru the spring variable 'hms-mirror.config.load-test-data' and the
    construction of the Conversion object.

    Setting this value in the config will allow us to check for this in the config and make the
    appropriate workflow adjustments.

    This is actually not getting set quickly enough.  So we're also setting this when a test-file is used
    in ConversionInitialization.
     */
    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.conversion.test-filename")
    CommandLineRunner configLoadTestData(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.conversion.test-filename}") String value) {
        return args -> {
            log.info("test-filename: {}", value);
            hmsMirrorConfig.setLoadTestDataFile(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.migrate-acid")
    CommandLineRunner configMigrateAcid(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.migrate-acid}") String value) {
        return args -> {
            log.info("migrate-acid: {}", value);
            if (Boolean.parseBoolean(value)) {
                hmsMirrorConfig.getMigrateACID().setOn(Boolean.TRUE);
                hmsMirrorConfig.getMigrateACID().setOnly(Boolean.FALSE);
            } else {
                hmsMirrorConfig.getMigrateACID().setOn(Boolean.TRUE);
                hmsMirrorConfig.getMigrateACID().setOnly(Boolean.FALSE);
                String bucketLimit = value;
                if (!isBlank(bucketLimit)) {
                    hmsMirrorConfig.getMigrateACID().setArtificialBucketThreshold(Integer.valueOf(bucketLimit));
                }
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.migrate-acid-only")
    CommandLineRunner configMigrateAcidOnly(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.migrate-acid-only}") String value) {
        return args -> {
            log.info("migrate-acid-only: {}", value);
            if (Boolean.parseBoolean(value)) {
                hmsMirrorConfig.getMigrateACID().setOn(Boolean.TRUE);
                hmsMirrorConfig.getMigrateACID().setOnly(Boolean.TRUE);
            } else {
                hmsMirrorConfig.getMigrateACID().setOn(Boolean.TRUE);
                hmsMirrorConfig.getMigrateACID().setOnly(Boolean.TRUE);
                String bucketLimit = value;
                if (!isBlank(bucketLimit)) {
                    hmsMirrorConfig.getMigrateACID().setArtificialBucketThreshold(Integer.valueOf(bucketLimit));
                }
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.migrate-non-native",
            havingValue = "true")
    CommandLineRunner configMigrateNonNativeTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("migrate-non-native: {}", Boolean.TRUE);
            hmsMirrorConfig.setMigrateNonNative(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.migrate-non-native",
            havingValue = "false")
    CommandLineRunner configMigrateNonNativeFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("migrate-non-native: {}", Boolean.FALSE);
            hmsMirrorConfig.setMigrateNonNative(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.migrate-non-native-only",
            havingValue = "true")
    CommandLineRunner configMigrateNonNativeOnlyTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("migrate-non-native-only: {}", Boolean.TRUE);
            hmsMirrorConfig.setMigrateNonNative(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.migrate-non-native-only",
            havingValue = "false")
    CommandLineRunner configMigrateNonNativeOnlyFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("migrate-non-native-only: {}", Boolean.FALSE);
            hmsMirrorConfig.setMigrateNonNative(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.no-purge",
            havingValue = "true")
    CommandLineRunner configNoPurgeTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("no-purge: {}", Boolean.TRUE);
            hmsMirrorConfig.setNoPurge(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.no-purge",
            havingValue = "false")
    CommandLineRunner configNoPurgeFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("no-purge: {}", Boolean.FALSE);
            hmsMirrorConfig.setNoPurge(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.password-key")
    CommandLineRunner configPasswordKey(HmsMirrorConfig config, @Value("${hms-mirror.config.password-key}") String value) {
        return args -> {
            log.info("password-key: {}", value);
            config.setPasswordKey(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.property-overrides")
    CommandLineRunner configPropertyOverrides(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.property-overrides}") String value) {
        return args -> {
            log.info("property-overrides: {}", value);
            String[] overrides = value.split(",");
            if (nonNull(overrides))
                hmsMirrorConfig.getOptimization().getOverrides().setPropertyOverridesStr(overrides, SideType.BOTH);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.property-overrides-left")
    CommandLineRunner configPropertyOverridesLeft(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.property-overrides-left}") String value) {
        return args -> {
            log.info("property-overrides-left: {}", value);
            String[] overrides = value.split(",");
            if (nonNull(overrides))
                hmsMirrorConfig.getOptimization().getOverrides().setPropertyOverridesStr(overrides, SideType.LEFT);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.property-overrides-right")
    CommandLineRunner configPropertyOverridesRight(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.property-overrides-right}") String value) {
        return args -> {
            log.info("property-overrides-right: {}", value);
            String[] overrides = value.split(",");
            if (nonNull(overrides))
                hmsMirrorConfig.getOptimization().getOverrides().setPropertyOverridesStr(overrides, SideType.RIGHT);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.quiet",
            havingValue = "true")
    CommandLineRunner configQuietTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("quiet: {}", Boolean.TRUE);
            hmsMirrorConfig.setQuiet(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.quiet",
            havingValue = "false")
    CommandLineRunner configQuietFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("quiet: {}", Boolean.FALSE);
            hmsMirrorConfig.setQuiet(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.read-only",
            havingValue = "true")
    CommandLineRunner configReadOnlyTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("read-only: {}", Boolean.TRUE);
            hmsMirrorConfig.setReadOnly(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.read-only",
            havingValue = "false")
    CommandLineRunner configReadOnlyFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("read-only: {}", Boolean.FALSE);
            hmsMirrorConfig.setReadOnly(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.replay-directory")
    CommandLineRunner configReplayDirectory(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.replay-directory}") String value) {
        return args -> {
            log.info("replay-directory: {}", value);
            // TODO: Implement Replay
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.reset-right",
            havingValue = "true")
    CommandLineRunner configResetRightTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("reset-right: {}", Boolean.TRUE);
            // TODO: Implement.  Does this still make sense?
            hmsMirrorConfig.setResetRight(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.reset-right",
            havingValue = "false")
    CommandLineRunner configResetRightFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("reset-right: {}", Boolean.FALSE);
            // TODO: Implement.  Does this still make sense?
            hmsMirrorConfig.setResetRight(Boolean.FALSE);
        };
    }

//    @Bean
//    @Order(2)
//    @ConditionalOnProperty(
//            name = "hms-mirror.config.reset-to-default-location",
//            havingValue = "true")
//    CommandLineRunner configResetToDefaultLocationTrue(HmsMirrorConfig hmsMirrorConfig) {
//        return args -> {
//            log.info("reset-to-default-location: {}", Boolean.TRUE);
//            hmsMirrorConfig.setResetToDefaultLocation(Boolean.TRUE);
//        };
//    }

//    @Bean
//    @Order(2)
//    @ConditionalOnProperty(
//            name = "hms-mirror.config.reset-to-default-location",
//            havingValue = "false")
//    CommandLineRunner configResetToDefaultLocationFalse(HmsMirrorConfig hmsMirrorConfig) {
//        return args -> {
//            log.info("reset-to-default-location: {}", Boolean.FALSE);
//            hmsMirrorConfig.setResetToDefaultLocation(Boolean.FALSE);
//        };
//    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.right-is-disconnected",
            havingValue = "true")
    CommandLineRunner configRightIsDisconnectedTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("right-is-disconnected: {}", Boolean.TRUE);
            if (null != hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2())
                hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2().setDisconnected(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.right-is-disconnected",
            havingValue = "false")
    CommandLineRunner configRightIsDisconnectedFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("right-is-disconnected: {}", Boolean.FALSE);
            if (null != hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2())
                hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2().setDisconnected(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.setup",
            havingValue = "true")
    CommandLineRunner configSetup(@Value("${hms-mirror.config.setup}") String value) {
        return args -> {
            log.info("setup: {}", value);
            if (Boolean.parseBoolean(value)) {
//            throw new NotImplementedException("Setup is not implemented yet.");
                String configFile = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror/cfg/default.yaml";
                File defaultCfg = new File(configFile);
                if (defaultCfg.exists()) {
                    Scanner scanner = new Scanner(System.in);
                    System.out.print("Default Config exists.  Proceed with overwrite:(Y/N) ");
                    String response = scanner.next();
                    if (response.equalsIgnoreCase("y")) {
                        HmsMirrorConfig.setup(configFile);
                        System.exit(0);
                    }
                } else {
                    HmsMirrorConfig.setup(configFile);
                    System.exit(0);
                }
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-features",
            havingValue = "true")
    CommandLineRunner configSkipFeaturesTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("skip-features: {}", Boolean.TRUE);
            hmsMirrorConfig.setSkipFeatures(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-features",
            havingValue = "false")
    CommandLineRunner configSkipFeaturesFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("skip-features: {}", Boolean.FALSE);
            hmsMirrorConfig.setSkipFeatures(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-legacy-translation",
            havingValue = "true")
    CommandLineRunner configSkipLegacyTranslationTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("skip-legacy-translation: {}", Boolean.TRUE);
            hmsMirrorConfig.setSkipLegacyTranslation(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-legacy-translation",
            havingValue = "false")
    CommandLineRunner configSkipLegacyTranslationFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("skip-legacy-translation: {}", Boolean.FALSE);
            hmsMirrorConfig.setSkipLegacyTranslation(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-link-check",
            havingValue = "true")
    CommandLineRunner configSkipLinkCheckTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("skip-link-check: {}", Boolean.TRUE);
            hmsMirrorConfig.setSkipLinkCheck(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-link-check",
            havingValue = "false")
    CommandLineRunner configSkipLinkCheckFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("skip-link-check: {}", Boolean.FALSE);
            hmsMirrorConfig.setSkipLinkCheck(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-optimizations",
            havingValue = "true")
    CommandLineRunner configSkipOptimizationsTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("skip-optimizations: {}", Boolean.TRUE);
            hmsMirrorConfig.getOptimization().setSkip(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-optimizations",
            havingValue = "false")
    CommandLineRunner configSkipOptimizationsFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("skip-optimizations: {}", Boolean.FALSE);
            hmsMirrorConfig.getOptimization().setSkip(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-stats-collection",
            havingValue = "true")
    CommandLineRunner configSkipStatsCollectionTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("skip-stats-collection: {}", Boolean.TRUE);
            hmsMirrorConfig.getOptimization().setSkipStatsCollection(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-stats-collection",
            havingValue = "false")
    CommandLineRunner configSkipStatsCollectionFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("skip-stats-collection: {}", Boolean.FALSE);
            hmsMirrorConfig.getOptimization().setSkipStatsCollection(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.sort-dynamic-partition-inserts",
            havingValue = "true")
    CommandLineRunner configSortDynamicPartitionInsertsTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("sort-dynamic-partition-inserts: {}", Boolean.TRUE);
            hmsMirrorConfig.getOptimization().setSortDynamicPartitionInserts(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.sort-dynamic-partition-inserts",
            havingValue = "false")
    CommandLineRunner configSortDynamicPartitionInsertsFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("sort-dynamic-partition-inserts: {}", Boolean.FALSE);
            hmsMirrorConfig.getOptimization().setSortDynamicPartitionInserts(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.sql-partition-count")
    CommandLineRunner configSqlPartitionCount(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.sql-partition-count}") String value) {
        return args -> {
            log.info("sql-partition-count: {}", value);
            hmsMirrorConfig.getHybrid().setSqlPartitionLimit(Integer.parseInt(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.storage-migration-namespace")
    CommandLineRunner configStorageMigrationNamespace(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.storage-migration-namespace}") String value) {
        return args -> {
            log.info("storage-migration-namespace: {}", value);
            hmsMirrorConfig.getTransfer().setTargetNamespace(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.storage-migration-strict",
            havingValue = "true")
    CommandLineRunner configStorageMigrationStrictTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("storage-migration-strict: {}", Boolean.TRUE);
            hmsMirrorConfig.getTransfer().getStorageMigration().setStrict(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.storage-migration-strict",
            havingValue = "false")
    CommandLineRunner configStorageMigrationStrictFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.warn("storage-migration-strict: {} is not currently supported to ensure valid migration plans.", Boolean.FALSE);
//            hmsMirrorConfig.getTransfer().getStorageMigration().setStrict(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.sync",
            havingValue = "true")
    CommandLineRunner configSyncTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("sync: {}", Boolean.TRUE);
            if (hmsMirrorConfig.getDataStrategy() != DataStrategyEnum.DUMP) {
                hmsMirrorConfig.setSync(Boolean.TRUE);
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.sync",
            havingValue = "false")
    CommandLineRunner configSyncFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("sync: {}", Boolean.FALSE);
            hmsMirrorConfig.setSync(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.table-exclude-filter")
    CommandLineRunner configTableExcludeFilter(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.table-exclude-filter}") String value) {
        return args -> {
            log.info("table-exclude-filter: {}", value);
            hmsMirrorConfig.getFilter().setTblExcludeRegEx(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.table-filter")
    CommandLineRunner configTableFilter(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.table-filter}") String value) {
        return args -> {
            log.info("table-filter: {}", value);
            hmsMirrorConfig.getFilter().setTblRegEx(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.table-filter-partition-count-limit")
    CommandLineRunner configTableFilterPartitionCountLimit(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.table-filter-partition-count-limit}") String value) {
        return args -> {
            log.info("table-filter-partition-count-limit: {}", value);
            hmsMirrorConfig.getFilter().setTblPartitionLimit(Integer.parseInt(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.table-filter-size-limit")
    CommandLineRunner configTableFilterSizeLimit(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.table-filter-size-limit}") String value) {
        return args -> {
            log.info("table-filter-size-limit: {}", value);
            hmsMirrorConfig.getFilter().setTblSizeLimit(Long.parseLong(value));
        };
    }

    @Bean
    @Order(2)
    @ConditionalOnProperty(
            name = "hms-mirror.config.transfer-ownership-database",
            havingValue = "true")
    CommandLineRunner configTransferOwnershipDbTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("transfer-ownership-database: {}", Boolean.TRUE);
            hmsMirrorConfig.getOwnershipTransfer().setDatabase(Boolean.TRUE);
        };
    }

    @Bean
    @Order(2)
    @ConditionalOnProperty(
            name = "hms-mirror.config.transfer-ownership-table",
            havingValue = "true")
    CommandLineRunner configTransferOwnershipTblTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("transfer-ownership-table: {}", Boolean.TRUE);
            hmsMirrorConfig.getOwnershipTransfer().setTable(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.transfer-ownership",
            havingValue = "true")
    CommandLineRunner configTransferOwnershipTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("transfer-ownership: {}", Boolean.TRUE);
            hmsMirrorConfig.getOwnershipTransfer().setDatabase(Boolean.TRUE);
            hmsMirrorConfig.getOwnershipTransfer().setTable(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.translation-type")
    CommandLineRunner configTranslationType(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.translation-type}") String value) {
        return args -> {
            log.info("translation-type: {}", value);
            hmsMirrorConfig.getTransfer().getStorageMigration().setTranslationType(TranslationTypeEnum.valueOf(value.toUpperCase()));
        };
    }


    /*
    Set this first (false or not set).  Individual settings will override.
     */
    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.transfer-ownership",
            havingValue = "false")
    CommandLineRunner configTransferOwnershipFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("transfer-ownership: {}", Boolean.FALSE);
            hmsMirrorConfig.getOwnershipTransfer().setDatabase(Boolean.FALSE);
            hmsMirrorConfig.getOwnershipTransfer().setTable(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.views-only",
            havingValue = "true")
    CommandLineRunner configViewsOnlyTrue(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("views-only: {}", Boolean.TRUE);
            hmsMirrorConfig.getMigrateVIEW().setOn(Boolean.TRUE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.views-only",
            havingValue = "false")
    CommandLineRunner configViewsOnlyFalse(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            log.info("views-only: {}", Boolean.FALSE);
            hmsMirrorConfig.getMigrateVIEW().setOn(Boolean.FALSE);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.warehouse-directory")
    CommandLineRunner configWarehouseDirectory(HmsMirrorConfig config, @Value("${hms-mirror.config.warehouse-directory}") String value) {
        return args -> {
            log.info("warehouse-directory: {}", value);
            if (isNull(config.getTransfer().getWarehouse())) {
                Warehouse warehouse = new Warehouse();
                warehouse.setSource(WarehouseSource.GLOBAL);
                config.getTransfer().setWarehouse(warehouse);
            }
            String wdStr = value;
            // Remove/prevent duplicate namespace config.
            if (nonNull(config.getTransfer().getTargetNamespace())) {
                if (wdStr.startsWith(config.getTransfer().getTargetNamespace())) {
                    wdStr = wdStr.substring(config.getTransfer().getTargetNamespace().length());
                    log.warn("Managed Warehouse Location Modified (stripped duplicate namespace): {}", wdStr);
                }
            }
            config.getTransfer().getWarehouse().setManagedDirectory(wdStr);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.warehouse-plans")
    CommandLineRunner configWarehousePlans(HmsMirrorConfig config, @Value("${hms-mirror.config.warehouse-plans}") String value) {
        return args -> {
            log.info("warehouse-plan: {}", value);
            String[] warehouseplan = value.split(",");

            if (nonNull(warehouseplan) && warehouseplan.length > 0) {
                // for each plan entry, split on '=' for db=ext_dir:mngd_dir
                for (String plan : warehouseplan) {
                    String[] planParts = plan.split("=");
                    String db = planParts[0];
                    if (planParts.length == 2) {
                        String[] locations = planParts[1].split(":");
                        String ext_dir = locations[0];
                        String mngd_dir = locations[1];
                        config.getTranslator().getWarehouseMapBuilder().addWarehousePlan(db, ext_dir, mngd_dir);//.put(planParts[0], planParts[1]);
                    } else {
                        log.warn("Invalid Warehouse Plan Entry: {}", plan);
                    }
                }
            }
        };
    }


    public CommandLine getCommandLine(String[] args) {
        Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException pe) {
            System.out.println("Missing Arguments: " + pe.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            String cmdline = ReportingConf.substituteVariablesFromManifest("hms-mirror <options> \nversion:${HMS-Mirror-Version}");
            formatter.printHelp(100, cmdline, "Hive Metastore Migration Utility", options,
                    "\nVisit https://github.com/cloudera-labs/hms-mirror/blob/main/README.md for detailed docs.");
            throw new RuntimeException(pe);
        }

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String cmdline = ReportingConf.substituteVariablesFromManifest("hms-mirror <options> \nversion:${HMS-Mirror-Version}");
            formatter.printHelp(100, cmdline, "Hive Metastore Migration Utility", options,
                    "\nVisit https://github.com/cloudera-labs/hms-mirror/blob/main/README.md for detailed docs");
            System.exit(0);
        }

        return cmd;
    }

    private Options getOptions() {
        // create Options object
        Options options = new Options();

        Option quietOutput = new Option("q", "quiet", false,
                "Reduce screen reporting output.  Good for background processes with output redirects to a file");
        quietOutput.setOptionalArg(Boolean.FALSE);
        quietOutput.setRequired(Boolean.FALSE);
        options.addOption(quietOutput);

        Option resetTarget = new Option("rr", "reset-right", false,
                "Use this for testing to remove the database on the RIGHT using CASCADE.");
        resetTarget.setRequired(Boolean.FALSE);
        options.addOption(resetTarget);

        Option resetToDefaultLocation = new Option("rdl", "reset-to-default-location", false,
                "Strip 'LOCATION' from all target cluster definitions.  This will allow the system defaults " +
                        "to take over and define the location of the new datasets.");
        resetToDefaultLocation.setRequired(Boolean.FALSE);
        options.addOption(resetToDefaultLocation);

        Option skipLegacyTranslation = new Option("slt", "skip-legacy-translation", false,
                "Skip Schema Upgrades and Serde Translations");
        skipLegacyTranslation.setRequired(Boolean.FALSE);
        options.addOption(skipLegacyTranslation);

        Option flipOption = new Option("f", "flip", false,
                "Flip the definitions for LEFT and RIGHT.  Allows the same config to be used in reverse.");
        flipOption.setOptionalArg(Boolean.FALSE);
        flipOption.setRequired(Boolean.FALSE);
        options.addOption(flipOption);

        Option smDistCpOption = new Option("dc", "distcp", false,
                "Build the 'distcp' workplans.  Optional argument (PULL, PUSH) to define which cluster is running " +
                        "the distcp commands.  Default is PULL.");
        smDistCpOption.setArgs(1);
        smDistCpOption.setOptionalArg(Boolean.TRUE);
        smDistCpOption.setArgName("flow-direction default:PULL");
        smDistCpOption.setRequired(Boolean.FALSE);
        options.addOption(smDistCpOption);

        Option metadataStage = new Option("d", "data-strategy", true,
                "Specify how the data will follow the schema. " + Arrays.deepToString(DataStrategyEnum.visibleValues()));
        metadataStage.setOptionalArg(Boolean.TRUE);
        metadataStage.setArgName("strategy");
        metadataStage.setRequired(Boolean.FALSE);
        options.addOption(metadataStage);

        Option dataMovementStategy = new Option("dms", "data-movement-strategy", true,
                "Specify how the data will follow the schema. " + Arrays.deepToString(DataMovementStrategyEnum.visibleValues()));
        dataMovementStategy.setOptionalArg(Boolean.TRUE);
        dataMovementStategy.setArgName("data-movement-strategy");
        dataMovementStategy.setRequired(Boolean.FALSE);
        options.addOption(dataMovementStategy);

        Option dumpSource = new Option("ds", "dump-source", true,
                "Specify which 'cluster' is the source for the DUMP strategy (LEFT|RIGHT). ");
        dumpSource.setOptionalArg(Boolean.TRUE);
        dumpSource.setArgName("source");
        dumpSource.setRequired(Boolean.FALSE);
        options.addOption(dumpSource);

        Option propertyOverrides = new Option("po", "property-overrides", true,
                "Comma separated key=value pairs of Hive properties you wish to set/override.");
        propertyOverrides.setArgName("key=value");
        propertyOverrides.setRequired(Boolean.FALSE);
        propertyOverrides.setValueSeparator(',');
        propertyOverrides.setArgs(100);
        options.addOption(propertyOverrides);

        Option propertyLeftOverrides = new Option("pol", "property-overrides-left", true,
                "Comma separated key=value pairs of Hive properties you wish to set/override for LEFT cluster.");
        propertyLeftOverrides.setArgName("key=value");
        propertyLeftOverrides.setRequired(Boolean.FALSE);
        propertyLeftOverrides.setValueSeparator(',');
        propertyLeftOverrides.setArgs(100);
        options.addOption(propertyLeftOverrides);

        Option propertyRightOverrides = new Option("por", "property-overrides-right", true,
                "Comma separated key=value pairs of Hive properties you wish to set/override for RIGHT cluster.");
        propertyRightOverrides.setArgName("key=value");
        propertyRightOverrides.setRequired(Boolean.FALSE);
        propertyRightOverrides.setValueSeparator(',');
        propertyRightOverrides.setArgs(100);
        options.addOption(propertyRightOverrides);

        OptionGroup optimizationsGroup = new OptionGroup();
        optimizationsGroup.setRequired(Boolean.FALSE);

        Option skipStatsCollectionOption = new Option("ssc", "skip-stats-collection", false,
                "Skip collecting basic FS stats for a table.  This WILL affect the optimizer and our ability to " +
                        "determine the best strategy for moving data.");
        skipStatsCollectionOption.setRequired(Boolean.FALSE);
        optimizationsGroup.addOption(skipStatsCollectionOption);

        Option skipOptimizationsOption = new Option("so", "skip-optimizations", false,
                "Skip any optimizations during data movement, like dynamic sorting or distribute by");
        skipOptimizationsOption.setRequired(Boolean.FALSE);
        optimizationsGroup.addOption(skipOptimizationsOption);

        Option sdpiOption = new Option("sdpi", "sort-dynamic-partition-inserts", false,
                "Used to set `hive.optimize.sort.dynamic.partition` in TEZ for optimal partition inserts.  " +
                        "When not specified, will use prescriptive sorting by adding 'DISTRIBUTE BY' to transfer SQL. " +
                        "default: false");
        sdpiOption.setRequired(Boolean.FALSE);
        optimizationsGroup.addOption(sdpiOption);

        Option autoTuneOption = new Option("at", "auto-tune", false,
                "Auto-tune Session Settings for SELECT's and DISTRIBUTION for Partition INSERT's.");
        autoTuneOption.setRequired(Boolean.FALSE);
        optimizationsGroup.addOption(autoTuneOption);

        options.addOptionGroup(optimizationsGroup);

        Option compressTextOutputOption = new Option("cto", "compress-text-output", false,
                "Data movement (SQL/STORAGE_MIGRATION) of TEXT based file formats will be compressed in the new " +
                        "table.");
        compressTextOutputOption.setRequired(Boolean.FALSE);
        options.addOption(compressTextOutputOption);

        Option icebergVersionOption = new Option("iv", "iceberg-version", true,
                "Specify the Iceberg Version to use.  Specify 1 or 2.  Default is 2.");
        icebergVersionOption.setOptionalArg(Boolean.TRUE);
        icebergVersionOption.setArgName("version");
        icebergVersionOption.setRequired(Boolean.FALSE);
        options.addOption(icebergVersionOption);

        Option icebergTablePropertyOverrides = new Option("itpo", "iceberg-table-property-overrides", true,
                "Comma separated key=value pairs of Iceberg Table Properties to set/override.");
        icebergTablePropertyOverrides.setArgName("key=value");
        icebergTablePropertyOverrides.setRequired(Boolean.FALSE);
        icebergTablePropertyOverrides.setValueSeparator(',');
        icebergTablePropertyOverrides.setArgs(100);
        options.addOption(icebergTablePropertyOverrides);

        Option createIfNotExistsOption = new Option("cine", "create-if-not-exist", false,
                "CREATE table/partition statements will be adjusted to include 'IF NOT EXISTS'.  This will ensure " +
                        "all remaining sql statements will be run.  This can be used to sync partition definitions for existing tables.");
        createIfNotExistsOption.setRequired(Boolean.FALSE);
        options.addOption(createIfNotExistsOption);

        OptionGroup testDataOptionGroup = new OptionGroup();

        Option dumpTestDataOption = new Option("dtd", "dump-test-data", false,
                "Used to dump a data set that can be feed into the process for testing.");
        dumpTestDataOption.setRequired(Boolean.FALSE);
        testDataOptionGroup.addOption(dumpTestDataOption);

        Option loadTestDataOption = new Option("ltd", "load-test-data", true,
                "Use the data saved by the `-dtd` option to test the process.");
        loadTestDataOption.setOptionalArg(Boolean.TRUE);
        loadTestDataOption.setArgName("file");
        // Adding to dbGroup because it overrides those values.
//        testDataOptionGroup.addOption(loadTestDataOption);

        options.addOptionGroup(testDataOptionGroup);

        Option forceExternalLocationOption = new Option("fel", "force-external-location", false,
                "Under some conditions, the LOCATION element for EXTERNAL tables is removed (ie: -rdl).  " +
                        "In which case we rely on the settings of the database definition to control the " +
                        "EXTERNAL table data location.  But for some older Hive versions, the LOCATION element in " +
                        "the database is NOT honored.  Even when the database LOCATION is set, the EXTERNAL table LOCATION " +
                        "defaults to the system wide warehouse settings.  This flag will ensure the LOCATION element " +
                        "remains in the CREATE definition of the table to force it's location.");
        forceExternalLocationOption.setRequired(Boolean.FALSE);
        options.addOption(forceExternalLocationOption);

        Option glblLocationMapOption = new Option("glm", "global-location-map", true,
                "Comma separated key=value pairs of Locations to Map. IE: /myorig/data/finance=/data/ec/finance. " +
                        "This reviews 'EXTERNAL' table locations for the path '/myorig/data/finance' and replaces it " +
                        "with '/data/ec/finance'.  Option can be used alone or with -rdl. Only applies to 'EXTERNAL' tables " +
                        "and if the tables location doesn't contain one of the supplied maps, it will be translated according " +
                        "to -rdl rules if -rdl is specified.  If -rdl is not specified, the conversion for that table is skipped. ");
        glblLocationMapOption.setArgName("key=value");
        glblLocationMapOption.setRequired(Boolean.FALSE);
        glblLocationMapOption.setValueSeparator(',');
        glblLocationMapOption.setArgs(1000);
        options.addOption(glblLocationMapOption);

        OptionGroup storageOptionsGroup = new OptionGroup();
        storageOptionsGroup.setRequired(Boolean.FALSE);

        Option intermediateStorageOption = new Option("is", "intermediate-storage", true,
                "Intermediate Storage used with Data Strategy HYBRID, SQL, EXPORT_IMPORT.  This will change " +
                        "the way these methods are implemented by using the specified storage location as an " +
                        "intermediate transfer point between two clusters.  In this case, the cluster do NOT need to " +
                        "be 'linked'.  Each cluster DOES need to have access to the location and authorization to " +
                        "interact with the location.  This may mean additional configuration requirements for " +
                        "'hdfs' to ensure this seamless access.");
        intermediateStorageOption.setOptionalArg(Boolean.TRUE);
        intermediateStorageOption.setArgName("storage-path");
        intermediateStorageOption.setRequired(Boolean.FALSE);
        storageOptionsGroup.addOption(intermediateStorageOption);

        Option commonStorageOption = new Option("cs", "common-storage", true,
                "Common Storage used with Data Strategy HYBRID, SQL, EXPORT_IMPORT.  This will change " +
                        "the way these methods are implemented by using the specified storage location as an " +
                        "'common' storage point between two clusters.  In this case, the cluster do NOT need to " +
                        "be 'linked'.  Each cluster DOES need to have access to the location and authorization to " +
                        "interact with the location.  This may mean additional configuration requirements for " +
                        "'hdfs' to ensure this seamless access.");
        commonStorageOption.setOptionalArg(Boolean.TRUE);
        commonStorageOption.setArgName("storage-path");
        commonStorageOption.setRequired(Boolean.FALSE);
        storageOptionsGroup.addOption(commonStorageOption);

        options.addOptionGroup(storageOptionsGroup);

        // External Warehouse Dir
        Option externalWarehouseDirOption = new Option("ewd", "external-warehouse-directory", true,
                "The external warehouse directory path.  Should not include the namespace OR the database directory. " +
                        "This will be used to set the LOCATION database option.");
        externalWarehouseDirOption.setOptionalArg(Boolean.TRUE);
        externalWarehouseDirOption.setArgName("path");
        externalWarehouseDirOption.setRequired(Boolean.FALSE);
        options.addOption(externalWarehouseDirOption);

        // Warehouse Dir
        Option warehouseDirOption = new Option("wd", "warehouse-directory", true,
                "The warehouse directory path.  Should not include the namespace OR the database directory. " +
                        "This will be used to set the MANAGEDLOCATION database option.");
        warehouseDirOption.setOptionalArg(Boolean.TRUE);
        warehouseDirOption.setArgName("path");
        warehouseDirOption.setRequired(Boolean.FALSE);
        options.addOption(warehouseDirOption);

        // Warehouse Plans
        Option warehousePlansOption = new Option("wps", "warehouse-plans", true,
                "The warehouse plans by database. Defines a plan for a database with 'external' and 'managed' directories.");
        warehousePlansOption.setOptionalArg(Boolean.TRUE);
        warehousePlansOption.setArgName("db=ext-dir:mngd-dir[,db=ext-dir:mngd-dir]...");
        warehousePlansOption.setRequired(Boolean.FALSE);
        options.addOption(warehousePlansOption);

        // Migration Options - Only one of these can be selected at a time, but isn't required.
        OptionGroup migrationOptionsGroup = new OptionGroup();
        migrationOptionsGroup.setRequired(Boolean.FALSE);

        Option dboOption = new Option("dbo", "database-only", false,
                "Migrate the Database definitions as they exist from LEFT to RIGHT");
        dboOption.setRequired(Boolean.FALSE);
        migrationOptionsGroup.addOption(dboOption);

        Option maoOption = new Option("mao", "migrate-acid-only", false,
                "Migrate ACID tables ONLY (if strategy allows). Optional: ArtificialBucketThreshold count that will remove " +
                        "the bucket definition if it's below this.  Use this as a way to remove artificial bucket definitions that " +
                        "were added 'artificially' in legacy Hive. (default: 2)");
        maoOption.setArgs(1);
        maoOption.setOptionalArg(Boolean.TRUE);
        maoOption.setArgName("bucket-threshold (2)");
        maoOption.setRequired(Boolean.FALSE);
        migrationOptionsGroup.addOption(maoOption);

        Option mnnoOption = new Option("mnno", "migrate-non-native-only", false,
                "Migrate Non-Native tables (if strategy allows). These include table definitions that rely on " +
                        "external connections to systems like: HBase, Kafka, JDBC");
        mnnoOption.setRequired(Boolean.FALSE);
        migrationOptionsGroup.addOption(mnnoOption);

        Option viewOption = new Option("v", "views-only", false,
                "Process VIEWs ONLY");
        viewOption.setRequired(false);
        migrationOptionsGroup.addOption(viewOption);

        options.addOptionGroup(migrationOptionsGroup);

        Option maOption = new Option("ma", "migrate-acid", false,
                "Migrate ACID tables (if strategy allows). Optional: ArtificialBucketThreshold count that will remove " +
                        "the bucket definition if it's below this.  Use this as a way to remove artificial bucket definitions that " +
                        "were added 'artificially' in legacy Hive. (default: 2)");
        maOption.setArgs(1);
        maOption.setOptionalArg(Boolean.TRUE);
        maOption.setArgName("bucket-threshold (2)");
        maOption.setRequired(Boolean.FALSE);
        options.addOption(maOption);

        Option daOption = new Option("da", "downgrade-acid", false,
                "Downgrade ACID tables to EXTERNAL tables with purge.");
        daOption.setRequired(Boolean.FALSE);
        options.addOption(daOption);

        Option evaluatePartLocationOption = new Option("epl", "evaluate-partition-location", false,
                "For SCHEMA_ONLY and DUMP data-strategies, review the partition locations and build " +
                        "partition metadata calls to create them is they can't be located via 'MSCK'.");
        evaluatePartLocationOption.setRequired(Boolean.FALSE);
        options.addOption(evaluatePartLocationOption);

        Option ridOption = new Option("rid", "right-is-disconnected", false,
                "Don't attempt to connect to the 'right' cluster and run in this mode");
        ridOption.setRequired(Boolean.FALSE);
        options.addOption(ridOption);

        Option ipOption = new Option("ip", "in-place", false,
                "Downgrade ACID tables to EXTERNAL tables with purge.");
        ipOption.setRequired(Boolean.FALSE);
        options.addOption(ipOption);

        Option skipLinkTestOption = new Option("slc", "skip-link-check", false,
                "Skip Link Check. Use when going between or to Cloud Storage to avoid having to configure " +
                        "hms-mirror with storage credentials and libraries. This does NOT preclude your Hive Server 2 and " +
                        "compute environment from such requirements.");
        skipLinkTestOption.setRequired(Boolean.FALSE);
        options.addOption(skipLinkTestOption);

        // Non Native Migrations
        Option mnnOption = new Option("mnn", "migrate-non-native", false,
                "Migrate Non-Native tables (if strategy allows). These include table definitions that rely on " +
                        "external connections to systems like: HBase, Kafka, JDBC");
//        mnnOption.setArgs(1);
//        mnnOption.setOptionalArg(Boolean.TRUE);
        mnnOption.setRequired(Boolean.FALSE);
        options.addOption(mnnOption);

        // TODO: Implement this feature...  If requested.  Needs testings, not complete after other downgrade work.
//        Option replaceOption = new Option("r", "replace", false,
//                "When downgrading an ACID table as its transferred to the 'RIGHT' cluster, this option " +
//                        "will replace the current ACID table on the LEFT cluster with a 'downgraded' table (EXTERNAL). " +
//                        "The option only works with options '-da' and '-cs'.");
//        replaceOption.setRequired(Boolean.FALSE);
//        options.addOption(replaceOption);

        Option syncOption = new Option("s", "sync", false,
                "For SCHEMA_ONLY, COMMON, and LINKED data strategies.  Drop and Recreate Schema's when different.  " +
                        "Best to use with RO to ensure table/partition drops don't delete data. When used WITHOUT `-tf` it will " +
                        "compare all the tables in a database and sync (bi-directional).  Meaning it will DROP tables on the RIGHT " +
                        "that aren't in the LEFT and ADD tables to the RIGHT that are missing.  When used with `-ro`, table schemas can be updated " +
                        "by dropping and recreating.  When used with `-tf`, only the tables that match the filter (on both " +
                        "sides) will be considered.\n When used with HYBRID, SQL, and EXPORT_IMPORT data strategies and ACID tables " +
                        "are involved, the tables will be dropped and recreated.  The data in this case WILL be dropped and replaced.");
        syncOption.setRequired(Boolean.FALSE);
        options.addOption(syncOption);

        Option roOption = new Option("ro", "read-only", false,
                "For SCHEMA_ONLY, COMMON, and LINKED data strategies set RIGHT table to NOT purge on DROP. " +
                        "Intended for use with replication distcp strategies and has restrictions about existing DB's " +
                        "on RIGHT and PATH elements.  To simply NOT set the purge flag for applicable tables, use -np.");
        roOption.setRequired(Boolean.FALSE);
        options.addOption(roOption);

        Option npOption = new Option("np", "no-purge", false,
                "For SCHEMA_ONLY, COMMON, and LINKED data strategies set RIGHT table to NOT purge on DROP");
        npOption.setRequired(Boolean.FALSE);
        options.addOption(npOption);

        Option acceptOption = new Option("accept", "accept", false,
                "Accept ALL confirmations and silence prompts");
        acceptOption.setRequired(Boolean.FALSE);
        options.addOption(acceptOption);

        // TODO: Add addition Storage Migration Strategies (current default and only option is SQL)
//        Option translateConfigOption = new Option("t", "translate-config", true,
//                "Translator Configuration File (Experimental)");
//        translateConfigOption.setRequired(Boolean.FALSE);
//        translateConfigOption.setArgName("translate-config-file");
//        options.addOption(translateConfigOption);

        Option outputOption = new Option("o", "output-dir", true,
                "Output Directory (default: $HOME/.hms-mirror/reports/<yyyy-MM-dd_HH-mm-ss>");
        outputOption.setRequired(Boolean.FALSE);
        outputOption.setArgName("outputdir");
        options.addOption(outputOption);

        Option skipFeaturesOption = new Option("sf", "skip-features", false,
                "Skip Features evaluation.");
        skipFeaturesOption.setRequired(Boolean.FALSE);
        options.addOption(skipFeaturesOption);

        Option executeOption = new Option("e", "execute", false,
                "Execute actions request, without this flag the process is a dry-run.");
        executeOption.setRequired(Boolean.FALSE);
        options.addOption(executeOption);

        Option asmOption = new Option("asm", "avro-schema-migration", false,
                "Migrate AVRO Schema Files referenced in TBLPROPERTIES by 'avro.schema.url'.  Without migration " +
                        "it is expected that the file will exist on the other cluster and match the 'url' defined in the " +
                        "schema DDL.\nIf it's not present, schema creation will FAIL.\nSpecifying this option REQUIRES the " +
                        "LEFT and RIGHT cluster to be LINKED.\nSee docs: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
        asmOption.setRequired(Boolean.FALSE);
        options.addOption(asmOption);

        OptionGroup transferOwnershipGroup = new OptionGroup();

        Option transferOwnershipOption = new Option("to", "transfer-ownership", false,
                "If available (supported) on LEFT cluster, extract and transfer the tables owner to the " +
                        "RIGHT cluster. Note: This will make an 'exta' SQL call on the LEFT cluster to determine " +
                        "the ownership.  This won't be supported on CDH 5 and some other legacy Hive platforms. " +
                        "Beware the cost of this extra call for EVERY table, as it may slow down the process for " +
                        "a large volume of tables.");
        transferOwnershipOption.setRequired(Boolean.FALSE);
        transferOwnershipGroup.addOption(transferOwnershipOption);

        Option transferOwnershipDbOption = new Option("todb", "transfer-ownership-database", false,
                "If available (supported) on LEFT cluster, extract and transfer the DB owner to the " +
                        "RIGHT cluster. Note: This will make an 'exta' SQL call on the LEFT cluster to determine " +
                        "the ownership.  This won't be supported on CDH 5 and some other legacy Hive platforms. ");
        transferOwnershipDbOption.setRequired(Boolean.FALSE);
        transferOwnershipGroup.addOption(transferOwnershipDbOption);

        Option transferOwnershipTblOption = new Option("totbl", "transfer-ownership-table", false,
                "If available (supported) on LEFT cluster, extract and transfer the tables owner to the " +
                        "RIGHT cluster. Note: This will make an 'exta' SQL call on the LEFT cluster to determine " +
                        "the ownership.  This won't be supported on CDH 5 and some other legacy Hive platforms. " +
                        "Beware the cost of this extra call for EVERY table, as it may slow down the process for " +
                        "a large volume of tables.");
        transferOwnershipTblOption.setRequired(Boolean.FALSE);
        transferOwnershipGroup.addOption(transferOwnershipTblOption);

        options.addOptionGroup(transferOwnershipGroup);

        OptionGroup dbAdjustOptionGroup = new OptionGroup();

        Option dbPrefixOption = new Option("dbp", "db-prefix", true,
                "Optional: A prefix to add to the RIGHT cluster DB Name. Usually used for testing.");
        dbPrefixOption.setRequired(Boolean.FALSE);
        dbPrefixOption.setArgName("prefix");
        dbAdjustOptionGroup.addOption(dbPrefixOption);

        Option dbRenameOption = new Option("dbr", "db-rename", true,
                "Optional: Rename target db to ...  This option is only valid when '1' database is listed in `-db`.");
        dbRenameOption.setRequired(Boolean.FALSE);
        dbRenameOption.setArgName("rename");
        dbAdjustOptionGroup.addOption(dbRenameOption);

        options.addOptionGroup(dbAdjustOptionGroup);

        Option storageMigrationNamespaceOption = new Option("smn", "storage-migration-namespace", true,
                "Optional: Used with the 'data strategy STORAGE_MIGRATION to specify the target namespace.");
        storageMigrationNamespaceOption.setRequired(Boolean.FALSE);
        storageMigrationNamespaceOption.setArgName("namespace");
        options.addOption(storageMigrationNamespaceOption);

        Option dbOption = new Option("db", "database", true,
                "Comma separated list of Databases (upto 100).");
        dbOption.setValueSeparator(',');
        dbOption.setArgName("databases");
        dbOption.setArgs(100);

        Option dbRegExOption = new Option("dbRegEx", "database-regex", true,
                "RegEx of Database to include in process.");
        dbRegExOption.setRequired(Boolean.FALSE);
        dbRegExOption.setArgName("regex");

        Option helpOption = new Option("h", "help", false,
                "Help");
        helpOption.setRequired(Boolean.FALSE);

        Option pwOption = new Option("p", "password", true,
                "Used this in conjunction with '-pkey' to generate the encrypted password that you'll add to the configs for the JDBC connections.");
        pwOption.setRequired(Boolean.FALSE);
        pwOption.setArgName("password");

        Option decryptPWOption = new Option("dp", "decrypt-password", true,
                "Used this in conjunction with '-pkey' to decrypt the generated passcode from `-p`.");
        decryptPWOption.setRequired(Boolean.FALSE);
        decryptPWOption.setArgName("encrypted-password");

        Option replayOption = new Option("replay", "replay", true,
                "Use to replay process from the report output.");
        replayOption.setRequired(Boolean.FALSE);
        replayOption.setArgName("report-directory");

        Option setupOption = new Option("su", "setup", false,
                "Setup a default configuration file through a series of questions");
        setupOption.setRequired(Boolean.FALSE);

        Option pKeyOption = new Option("pkey", "password-key", true,
                "The key used to encrypt / decrypt the cluster jdbc passwords.  If not present, the passwords will be processed as is (clear text) from the config file.");
        pKeyOption.setRequired(false);
        pKeyOption.setArgName("password-key");
        options.addOption(pKeyOption);

        OptionGroup dbGroup = new OptionGroup();
        dbGroup.addOption(dbOption);
        dbGroup.addOption(loadTestDataOption);
        dbGroup.addOption(dbRegExOption);
        dbGroup.addOption(helpOption);
        dbGroup.addOption(setupOption);
        dbGroup.addOption(pwOption);
        dbGroup.addOption(decryptPWOption);
        dbGroup.addOption(replayOption);
        dbGroup.setRequired(Boolean.TRUE);
        options.addOptionGroup(dbGroup);

        Option sqlOutputOption = new Option("sql", "sql-output", false,
                "<deprecated>.  This option is no longer required to get SQL out in a report.  That is the default behavior.");
        sqlOutputOption.setRequired(Boolean.FALSE);
        options.addOption(sqlOutputOption);

        Option acidPartCountOption = new Option("ap", "acid-partition-count", true,
                "Set the limit of partitions that the ACID strategy will work with. '-1' means no-limit.");
        acidPartCountOption.setRequired(Boolean.FALSE);
        acidPartCountOption.setArgName("limit");
        options.addOption(acidPartCountOption);

        Option sqlPartCountOption = new Option("sp", "sql-partition-count", true,
                "Set the limit of partitions that the SQL strategy will work with. '-1' means no-limit.");
        sqlPartCountOption.setRequired(Boolean.FALSE);
        sqlPartCountOption.setArgName("limit");
        options.addOption(sqlPartCountOption);

        Option expImpPartCountOption = new Option("ep", "export-partition-count", true,
                "Set the limit of partitions that the EXPORT_IMPORT strategy will work with.");
        expImpPartCountOption.setRequired(Boolean.FALSE);
        expImpPartCountOption.setArgName("limit");
        options.addOption(expImpPartCountOption);

        OptionGroup filterGroup = new OptionGroup();
        filterGroup.setRequired(Boolean.FALSE);

        Option tableFilterOption = new Option("tf", "table-filter", true,
                "Filter tables (inclusive) with name matching RegEx. Comparison done with 'show tables' " +
                        "results.  Check case, that's important.  Hive tables are generally stored in LOWERCASE. " +
                        "Make sure you double-quote the expression on the commandline.");
        tableFilterOption.setRequired(Boolean.FALSE);
        tableFilterOption.setArgName("regex");
        filterGroup.addOption(tableFilterOption);

        Option excludeTableFilterOption = new Option("tef", "table-exclude-filter", true,
                "Filter tables (excludes) with name matching RegEx. Comparison done with 'show tables' " +
                        "results.  Check case, that's important.  Hive tables are generally stored in LOWERCASE. " +
                        "Make sure you double-quote the expression on the commandline.");
        excludeTableFilterOption.setRequired(Boolean.FALSE);
        excludeTableFilterOption.setArgName("regex");
        filterGroup.addOption(excludeTableFilterOption);

        options.addOptionGroup(filterGroup);

        Option tableSizeFilterOption = new Option("tfs", "table-filter-size-limit", true,
                "Filter tables OUT that are above the indicated size.  Expressed in MB");
        tableSizeFilterOption.setRequired(Boolean.FALSE);
        tableSizeFilterOption.setArgName("size MB");
        options.addOption(tableSizeFilterOption);

        Option tablePartitionCountFilterOption = new Option("tfp", "table-filter-partition-count-limit", true,
                "Filter partition tables OUT that are have more than specified here. Non Partitioned table aren't " +
                        "filtered.");
        tablePartitionCountFilterOption.setRequired(Boolean.FALSE);
        tablePartitionCountFilterOption.setArgName("partition-count");
        options.addOption(tablePartitionCountFilterOption);

        Option translationTypeOption = new Option("tt", "translation-type", true,
                "Translation Strategy when migrating data. (ALIGNED|RELATIVE)  Default is RELATIVE");
        translationTypeOption.setRequired(Boolean.FALSE);
        translationTypeOption.setArgName("translation-type");
        options.addOption(translationTypeOption);

        Option cfgOption = new Option("cfg", "config", true,
                "Config with details for the HMS-Mirror.  Default: $HOME/.hms-mirror/cfg/default.yaml");
        cfgOption.setRequired(false);
        cfgOption.setArgName("filename");
        options.addOption(cfgOption);

        return options;
    }

    /*
    This should run last to ensure that the config is set correctly after all the flags have been set.
     */
    @Bean
    @Order(20)
    CommandLineRunner initializeConnectionsAfterConfigSetup(ConnectionPoolService connectionPoolService) {
        return args -> {
//            connectionPoolService.init();
        };
    }

    public String[] toSpringBootOption(Boolean withoutWeb, String[] args) {
        CommandLine cmd = getCommandLine(args);
        List<String> springOptions = new ArrayList<>();
        // Turn off web ui
        if (withoutWeb) {
            springOptions.add("--spring.main.web-application-type=none");
        }
        for (Option option : cmd.getOptions()) {
            String opt = option.getLongOpt();
            String[] values = option.getValues();
            if (opt.equals("config")) {
                // Handle the config file differently
                springOptions.add("--hms-mirror.config.filename" + "=\"" + String.join(",", values) + "\"");
            } else if (opt.equals("concurrency")) {
                // Set Concurrency
                springOptions.add("--hms-mirror.concurrency.max-threads" + "=\"" + String.join(",", values) + "\"");
            } else if (opt.equals("load-test-data")) {
                // Set Concurrency
                springOptions.add("--hms-mirror.conversion.test-filename" + "=\"" + String.join(",", values) + "\"");
            }else {
                if (nonNull(values) && values.length > 0) {
                    springOptions.add("--" + SPRING_CONFIG_PREFIX + "." + opt + "=" + String.join(",", values));
                } else {
                    springOptions.add("--" + SPRING_CONFIG_PREFIX + "." + opt + "=" + "true");
                }
            }
        }
        // Collect Legacy Command Line Options and pass them to the Spring Boot Application as a single string.
        String clo = Strings.join(Arrays.asList(args), ' ');
        springOptions.add("--hms-mirror.config.legacy-command-line-options=\"" + clo + "\"");
        return springOptions.toArray(new String[0]);
    }
}
