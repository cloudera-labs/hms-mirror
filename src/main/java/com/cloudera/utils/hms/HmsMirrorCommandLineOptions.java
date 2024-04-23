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

package com.cloudera.utils.hms;

import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.cli.CliReporter;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.HmsMirrorCfgService;
import com.cloudera.utils.hms.util.Protect;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.FileSystems;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.cloudera.utils.hms.mirror.MessageCode.ENVIRONMENT_CONNECTION_ISSUE;
import static com.cloudera.utils.hms.mirror.MessageCode.ENVIRONMENT_DISCONNECTED;

@Configuration
@Order(5)
@Slf4j
@Getter
@Setter
public class HmsMirrorCommandLineOptions {
    public static String SPRING_CONFIG_PREFIX = "hms-mirror.config";

    public static void main(String[] args) {
        HmsMirrorCommandLineOptions pcli = new HmsMirrorCommandLineOptions();
        String[] convertedArgs = pcli.toSpringBootOption(args);
        String newCmdLn = String.join(" ", convertedArgs);
        System.out.println(newCmdLn);
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
            name = "hms-mirror.config.auto-tune")
    CommandLineRunner configAutoTune(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.auto-tune}") String value) {
        return args -> {
            log.info("auto-tune: {}", value);
            hmsMirrorConfig.getOptimization().setAutoTune(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.avro-schema-migration")
    CommandLineRunner configAvroSchemaMigration(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.avro-schema-migration}") String value) {
        return args -> {
            log.info("avro-schema-migration: {}", value);
            hmsMirrorConfig.setCopyAvroSchemaUrls(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.create-if-not-exist")
    CommandLineRunner configCine(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.create-if-not-exist}") String value) {
        return args -> {
            log.info("create-if-not-exist: {}", value);
            if (hmsMirrorConfig.getCluster(Environment.LEFT) != null) {
                hmsMirrorConfig.getCluster(Environment.LEFT).setCreateIfNotExists(Boolean.parseBoolean(value));
            }
            if (hmsMirrorConfig.getCluster(Environment.RIGHT) != null) {
                hmsMirrorConfig.getCluster(Environment.RIGHT).setCreateIfNotExists(Boolean.parseBoolean(value));
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
            name = "hms-mirror.config.common-storage")
    CommandLineRunner configCommonStorage(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.common-storage}") String value) {
        return args -> {
            log.info("common-storage: {}", value);
            hmsMirrorConfig.getTransfer().setCommonStorage(value);
            // This usually means an on-prem to cloud migration, which should be a PUSH data flow for distcp.
            hmsMirrorConfig.getTransfer().getStorageMigration().setDataFlow(DistcpFlow.PUSH);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.compress-text-output")
    CommandLineRunner configCompressTextOutput(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.compress-text-output}") String value) {
        return args -> {
            log.info("compress-text-output: {}", value);
            hmsMirrorConfig.getOptimization().setCompressTextOutput(Boolean.parseBoolean(value));
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
            hmsMirrorConfig.setDatabases(dbs.split(","));
//            log.info("Concurrency: " + config.getTransfer().getConcurrency());
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.database-only")
    CommandLineRunner configDatabaseOnly(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.database-only}") String value) {
        return args -> {
            log.info("database-only: {}", value);
            hmsMirrorConfig.setDatabaseOnly(Boolean.parseBoolean(value));
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
            name = "hms-mirror.config.decrypt-password")
    CommandLineRunner configDecryptPassword(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.decrypt-password}") String value) {
        return args -> {
            log.info("decrypt-password: {}", value);
            hmsMirrorConfig.setDecryptPassword(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.distcp")
    CommandLineRunner configDistcp(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.distcp}") String value) {
        return args -> {
            log.info("distcp: {}", value);
            if (Boolean.parseBoolean(value)) {
                hmsMirrorConfig.getTransfer().getStorageMigration().setDistcp(Boolean.TRUE);
            } else {
                hmsMirrorConfig.getTransfer().getStorageMigration().setDistcp(Boolean.TRUE);
                String flowStr = value;
                if (flowStr != null) {
                    try {
                        DistcpFlow flow = DistcpFlow.valueOf(flowStr.toUpperCase(Locale.ROOT));
                        hmsMirrorConfig.getTransfer().getStorageMigration().setDataFlow(flow);
                    } catch (IllegalArgumentException iae) {
                        throw new RuntimeException("Optional argument for `distcp` is invalid. Valid values: " +
                                Arrays.toString(DistcpFlow.values()), iae);
                    }
                }
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.downgrade-acid")
    CommandLineRunner configDowngradeAcid(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.downgrade-acid}") String value) {
        return args -> {
            log.info("downgrade-acid: {}", value);
            hmsMirrorConfig.getMigrateACID().setDowngrade(Boolean.parseBoolean(value));
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
            name = "hms-mirror.config.evaluate-partition-location")
    CommandLineRunner configEvaluatePartitionLocation(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.evaluate-partition-location}") String value) {
        return args -> {
            log.info("evaluate-partition-location: {}", value);
            hmsMirrorConfig.setEvaluatePartitionLocation(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.execute")
    CommandLineRunner configExecute(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.execute}") String value) {
        return args -> {
            log.info("execute: {}", value);
            hmsMirrorConfig.setExecute(Boolean.parseBoolean(value));
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
    CommandLineRunner configExternalWarehouseDirectory(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.external-warehouse-directory}") String value) {
        return args -> {
            log.info("external-warehouse-directory: {}", value);
            if (hmsMirrorConfig.getTransfer().getWarehouse() == null)
                hmsMirrorConfig.getTransfer().setWarehouse(new WarehouseConfig());
            String ewdStr = value;
            // Remove/prevent duplicate namespace config.
            if (hmsMirrorConfig.getTransfer().getCommonStorage() != null) {
                if (ewdStr.startsWith(hmsMirrorConfig.getTransfer().getCommonStorage())) {
                    ewdStr = ewdStr.substring(hmsMirrorConfig.getTransfer().getCommonStorage().length());
                    log.warn("External Warehouse Location Modified (stripped duplicate namespace): {}", ewdStr);
                }
            }
            hmsMirrorConfig.getTransfer().getWarehouse().setExternalDirectory(ewdStr);

        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.flip")
    CommandLineRunner configFlip(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.flip}") String value) {
        return args -> {
            log.info("flip: {}", value);
            hmsMirrorConfig.setFlip(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.force-external-location")
    CommandLineRunner configForceExternalLocation(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.force-external-location}") String value) {
        return args -> {
            log.info("force-external-location: {}", value);
            hmsMirrorConfig.getTranslator().setForceExternalLocation(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.global-location-map")
    CommandLineRunner configGlobalLocationMap(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.global-location-map}") String value) {
        return args -> {
            log.info("global-location-map: {}", value);
            String[] globalLocMap = value.split(",");
            if (globalLocMap != null)
                hmsMirrorConfig.setGlobalLocationMapKV(globalLocMap);
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
    CommandLineRunner configIcebergTablePropertyOverrides(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.iceberg-table-property-overrides}") String value) {
        return args -> {
            log.info("iceberg-table-property-overrides: {}", value);
            String[] overrides = value.split(",");
            if (overrides != null)
                hmsMirrorConfig.getIcebergConfig().setPropertyOverridesStr(overrides);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.iceberg-version")
    CommandLineRunner configIcebergVersion(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.iceberg-version}") String value) {
        return args -> {
            log.info("iceberg-version: {}", value);
            hmsMirrorConfig.getIcebergConfig().setVersion(Integer.parseInt(value));
        };
    }

    @Bean
    // So this happens AFTER migrate acid and migrate acid only are checked.
    @Order(5)
    @ConditionalOnProperty(
            name = "hms-mirror.config.in-place")
    CommandLineRunner configInPlace(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.in-place}") String value) {
        return args -> {
            log.info("in-place: {}", value);
            if (hmsMirrorConfig.getMigrateACID().isOn()) {
//                if (cmd.hasOption("da")) {
//                    // Downgrade ACID tables
//                    getConfig().getMigrateACID().setDowngrade(Boolean.TRUE);
//                }
//                if (cmd.hasOption("ip")) {
                // Downgrade ACID tables inplace
                // Only work on LEFT cluster definition.
//                    log.info("Inplace ACID Downgrade");
                hmsMirrorConfig.getMigrateACID().setDowngrade(Boolean.parseBoolean(value));
                hmsMirrorConfig.getMigrateACID().setInplace(Boolean.parseBoolean(value));
                // For 'in-place' downgrade, only applies to ACID tables.
                // Implies `-mao`.
                log.info("Only ACID Tables will be looked at since 'ip' was specified.");
                hmsMirrorConfig.getMigrateACID().setOnly(Boolean.TRUE);
                // Remove RIGHT cluster and enforce mao
                log.info("RIGHT Cluster definition will be disconnected if exists since this is a LEFT cluster ONLY operation");
                if (null != hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2())
                    hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2().setDisconnected(Boolean.TRUE);
//                }
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
            hmsMirrorConfig.getTransfer().getStorageMigration().setDataFlow(DistcpFlow.PUSH_PULL);
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
                if (bucketLimit != null) {
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
                if (bucketLimit != null) {
                    hmsMirrorConfig.getMigrateACID().setArtificialBucketThreshold(Integer.valueOf(bucketLimit));
                }
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.migrate-non-native")
    CommandLineRunner configMigrateNonNative(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.migrate-non-native}") String value) {
        return args -> {
            log.info("migrate-non-native: {}", value);
            hmsMirrorConfig.setMigratedNonNative(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.migrate-non-native-only")
    CommandLineRunner configMigrateNonNativeOnly(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.migrate-non-native-only}") String value) {
        return args -> {
            log.info("migrate-non-native-only: {}", value);
            hmsMirrorConfig.setMigratedNonNative(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.no-purge")
    CommandLineRunner configNoPurge(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.no-purge}") String value) {
        return args -> {
            log.info("no-purge: {}", value);
            hmsMirrorConfig.setNoPurge(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.output-dir")
        // Will set this when the value is set externally.
    CommandLineRunner configOutputDir(HmsMirrorConfig hmsMirrorConfig, CliReporter reporter, @Value("${hms-mirror.config.output-dir}") String value) {
        return configOutputDirInternal(hmsMirrorConfig, reporter, value);
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.output-dir",
            havingValue = "false")
        // Will set this when the value is NOT set and picks up the default application.yaml (false) setting.
    CommandLineRunner configOutputDirNotSet(HmsMirrorConfig hmsMirrorConfig, CliReporter reporter) {
        String value = System.getenv("APP_OUTPUT_PATH");
        if (value != null) {
            return configOutputDirInternal(hmsMirrorConfig, reporter, value);
        } else {
            return configOutputDirInternal(hmsMirrorConfig, reporter,
                    System.getProperty("user.dir") + FileSystems.getDefault().getSeparator() + ".hms-mirror/reports/not-set");
        }
    }

    CommandLineRunner configOutputDirInternal(HmsMirrorConfig hmsMirrorConfig, CliReporter reporter, String value) {
        return args -> {
            log.info("output-dir: {}", value);
            hmsMirrorConfig.setOutputDirectory(value);
            File reportPathDir = new File(value);
            if (!reportPathDir.exists()) {
                reportPathDir.mkdirs();
            }
            reporter.setReportOutputFile(value + FileSystems.getDefault().getSeparator() + "<db>_hms-mirror.md|html|yaml");
            reporter.setLeftExecuteFile(value + FileSystems.getDefault().getSeparator() + "<db>_LEFT_execute.sql");
            reporter.setLeftCleanUpFile(value + FileSystems.getDefault().getSeparator() + "<db>_LEFT_CleanUp_execute.sql");
            reporter.setRightExecuteFile(value + FileSystems.getDefault().getSeparator() + "<db>_RIGHT_execute.sql");
            reporter.setRightCleanUpFile(value + FileSystems.getDefault().getSeparator() + "<db>_RIGHT_CleanUp_execute.sql");

            File testFile = new File(value + FileSystems.getDefault().getSeparator() + ".dir-check");

            // Ensure the Retry Path is created.
            File retryPath = new File(System.getProperty("user.home") + FileSystems.getDefault().getSeparator() + ".hms-mirror" +
                    FileSystems.getDefault().getSeparator() + "retry");
            if (!retryPath.exists()) {
                retryPath.mkdirs();
            }

            // Test file to ensure we can write to it for the report.
            try {
                new FileOutputStream(testFile).close();
            } catch (IOException e) {
                throw new RuntimeException("Can't write to output directory: " + value, e);
            }

        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.password")
    CommandLineRunner configPassword(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.password}") String value) {
        return args -> {
            log.info("password: {}", "********");
            hmsMirrorConfig.setPassword(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.password-key")
    CommandLineRunner configPasswordKey(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.password-key}") String value) {
        return args -> {
            log.info("password-key: {}", value);
            hmsMirrorConfig.setPasswordKey(value);
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
            if (overrides != null)
                hmsMirrorConfig.getOptimization().getOverrides().setPropertyOverridesStr(overrides, Overrides.Side.BOTH);
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
            if (overrides != null)
                hmsMirrorConfig.getOptimization().getOverrides().setPropertyOverridesStr(overrides, Overrides.Side.LEFT);
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
            if (overrides != null)
                hmsMirrorConfig.getOptimization().getOverrides().setPropertyOverridesStr(overrides, Overrides.Side.RIGHT);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.quiet")
    CommandLineRunner configQuiet(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.quiet}") String value) {
        return args -> {
            log.info("quiet: {}", value);
            hmsMirrorConfig.setQuiet(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.read-only")
    CommandLineRunner configReadOnly(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.read-only}") String value) {
        return args -> {
            log.info("read-only: {}", value);
            hmsMirrorConfig.setReadOnly(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.replay-directory")
    CommandLineRunner configReportDirectory(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.replay-directory}") String value) {
        return args -> {
            log.info("replay-directory: {}", value);
            // TODO: Implement Replay
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.reset-right")
    CommandLineRunner configResetRight(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.reset-right}") String value) {
        return args -> {
            log.info("reset-right: {}", value);
            // TODO: Implement.  Does this still make sense?
//            config.
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.reset-to-default-location")
    CommandLineRunner configResetToDefaultLocation(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.reset-to-default-location}") String value) {
        return args -> {
            log.info("reset-to-default-location: {}", value);
            hmsMirrorConfig.setResetToDefaultLocation(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.right-is-disconnected")
    CommandLineRunner configRightIsDisconnected(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.right-is-disconnected}") String value) {
        return args -> {
            log.info("right-is-disconnected: {}", value);
            if (null != hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2())
                hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2().setDisconnected(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.setup")
    CommandLineRunner configSetup(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.setup}") String value) {
        return args -> {
            log.info("setup: {}", value);
            throw new NotImplementedException("Setup is not implemented yet.");
            // TODO: Implement Setup
//            configFile = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror/cfg/default.yaml";
//            File defaultCfg = new File(configFile);
//            if (defaultCfg.exists()) {
//                Scanner scanner = new Scanner(System.in);
//                System.out.print("Default Config exists.  Proceed with overwrite:(Y/N) ");
//                String response = scanner.next();
//                if (response.equalsIgnoreCase("y")) {
//                    Config.setup(configFile);
//                    System.exit(0);
//                }
//            } else {
//                Config.setup(configFile);
//                System.exit(0);
//            }

        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-features")
    CommandLineRunner configSkipFeatures(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.skip-features}") String value) {
        return args -> {
            log.info("skip-features: {}", value);
            hmsMirrorConfig.setSkipFeatures(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-legacy-translation")
    CommandLineRunner configSkipLegacyTranslation(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.skip-legacy-translation}") String value) {
        return args -> {
            log.info("skip-legacy-translation: {}", value);
            hmsMirrorConfig.setSkipLegacyTranslation(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-link-check")
    CommandLineRunner configSkipLinkCheck(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.skip-link-check}") String value) {
        return args -> {
            log.info("skip-link-check: {}", value);
            hmsMirrorConfig.setSkipLinkCheck(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-optimizations")
    CommandLineRunner configSkipOptimizations(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.skip-optimizations}") String value) {
        return args -> {
            log.info("skip-optimizations: {}", value);
            hmsMirrorConfig.getOptimization().setSkip(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.skip-stats-collection")
    CommandLineRunner configSkipStatsCollection(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.skip-stats-collection}") String value) {
        return args -> {
            log.info("skip-stats-collection: {}", value);
            hmsMirrorConfig.getOptimization().setSkipStatsCollection(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.sort-dynamic-partition-inserts")
    CommandLineRunner configSortDynamicPartitionInserts(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.sort-dynamic-partition-inserts}") String value) {
        return args -> {
            log.info("sort-dynamic-partition-inserts: {}", value);
            hmsMirrorConfig.getOptimization().setSortDynamicPartitionInserts(Boolean.parseBoolean(value));
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
            hmsMirrorConfig.getTransfer().setCommonStorage(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.sync")
    CommandLineRunner configSync(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.sync}") String value) {
        return args -> {
            log.info("sync: {}", value);
            if (hmsMirrorConfig.getDataStrategy() != DataStrategyEnum.DUMP) {
                hmsMirrorConfig.setSync(Boolean.parseBoolean(value));
            }
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
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.transfer-ownership")
    CommandLineRunner configTransferOwnership(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.transfer-ownership}") String value) {
        return args -> {
            log.info("transfer-ownership: {}", value);
            hmsMirrorConfig.setTransferOwnership(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.web-interface")
    CommandLineRunner configUi(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.web-interface}") String value) {
        return args -> {
            log.info("web-interface: {}", value);
            hmsMirrorConfig.setWebInterface(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.views-only")
    CommandLineRunner configViewsOnly(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.views-only}") String value) {
        return args -> {
            log.info("views-only: {}", value);
            hmsMirrorConfig.getMigrateVIEW().setOn(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.warehouse-directory")
    CommandLineRunner configWarehouseDirectory(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.warehouse-directory}") String value) {
        return args -> {
            log.info("warehouse-directory: {}", value);
            if (hmsMirrorConfig.getTransfer().getWarehouse() == null)
                hmsMirrorConfig.getTransfer().setWarehouse(new WarehouseConfig());
            String wdStr = value;
            // Remove/prevent duplicate namespace config.
            if (hmsMirrorConfig.getTransfer().getCommonStorage() != null) {
                if (wdStr.startsWith(hmsMirrorConfig.getTransfer().getCommonStorage())) {
                    wdStr = wdStr.substring(hmsMirrorConfig.getTransfer().getCommonStorage().length());
                    log.warn("Managed Warehouse Location Modified (stripped duplicate namespace): {}", wdStr);
                }
            }
            hmsMirrorConfig.getTransfer().getWarehouse().setManagedDirectory(wdStr);
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

        Option webInterfaceOption = new Option("wi", "web-interface", false,
                "Start Web-Interface.");
        webInterfaceOption.setOptionalArg(Boolean.FALSE);
        webInterfaceOption.setRequired(Boolean.FALSE);
        options.addOption(webInterfaceOption);

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
                        "external connection to systems like: HBase, Kafka, JDBC");
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
                        "external connection to systems like: HBase, Kafka, JDBC");
        mnnOption.setArgs(1);
        mnnOption.setOptionalArg(Boolean.TRUE);
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

        Option transferOwnershipOption = new Option("to", "transfer-ownership", false,
                "If available (supported) on LEFT cluster, extract and transfer the tables owner to the " +
                        "RIGHT cluster. Note: This will make an 'exta' SQL call on the LEFT cluster to determine " +
                        "the ownership.  This won't be supported on CDH 5 and some other legacy Hive platforms. " +
                        "Beware the cost of this extra call for EVERY table, as it may slow down the process for " +
                        "a large volume of tables.");
        transferOwnershipOption.setRequired(Boolean.FALSE);
        options.addOption(transferOwnershipOption);

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
    CommandLineRunner postHmsCmdLnOptConfigProcessing(HmsMirrorCfgService hmsMirrorCfgService,
                                                      ConnectionPoolService connectionPoolService,
                                                      Progression progression) {
        return args -> {
            HmsMirrorConfig hmsMirrorConfig = hmsMirrorCfgService.getHmsMirrorConfig();

            // Decode Password if necessary.
            if (hmsMirrorConfig.getPassword() != null || hmsMirrorConfig.getDecryptPassword() != null) {
                // Used to generate encrypted password.
                if (hmsMirrorConfig.getPasswordKey() != null) {
                    Protect protect = new Protect(hmsMirrorConfig.getPasswordKey());
                    // Set to control execution flow.
                    hmsMirrorConfig.addError(MessageCode.PASSWORD_CFG);
                    if (hmsMirrorConfig.getPassword() != null) {
                        String epassword = null;
                        try {
                            epassword = protect.encrypt(hmsMirrorConfig.getPassword());
                            hmsMirrorConfig.setDecryptPassword(epassword);
                            hmsMirrorConfig.addWarning(MessageCode.ENCRYPTED_PASSWORD, epassword);
                        } catch (Exception e) {
                            hmsMirrorConfig.addError(MessageCode.ENCRYPT_PASSWORD_ISSUE);
                        }
                    } else {
                        String password = null;
                        try {
                            password = protect.decrypt(hmsMirrorConfig.getDecryptPassword());
                            hmsMirrorConfig.setPassword(password);
                            hmsMirrorConfig.addWarning(MessageCode.DECRYPTED_PASSWORD, password);
                        } catch (Exception e) {
                            hmsMirrorConfig.addError(MessageCode.DECRYPTING_PASSWORD_ISSUE);
                        }
                    }
                } else {
                    hmsMirrorConfig.addError(MessageCode.PKEY_PASSWORD_CFG);
                }
            } else if (hmsMirrorConfig.getPasswordKey() != null) {
                // Decrypt Passwords
                Protect protect = new Protect(hmsMirrorConfig.getPasswordKey());
                if (hmsMirrorConfig.getCluster(Environment.LEFT) != null
                        && hmsMirrorConfig.getCluster(Environment.LEFT).getHiveServer2() != null
                        && hmsMirrorConfig.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().getProperty("password") != null) {
                    try {
                        hmsMirrorConfig.getCluster(Environment.LEFT).getHiveServer2()
                                .getConnectionProperties().setProperty("password",
                                        protect.decrypt(hmsMirrorConfig.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().getProperty("password")));
                        hmsMirrorConfig.addWarning(MessageCode.DECRYPTED_PASSWORD, "*******");
                    } catch (Exception e) {
                        hmsMirrorConfig.addError(MessageCode.DECRYPTING_PASSWORD_ISSUE, "LEFT HiveServer2");
                    }
                }
                if (hmsMirrorConfig.getCluster(Environment.RIGHT) != null
                        && hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2() != null
                        && hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().getProperty("password") != null) {
                    try {
                        hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2()
                                .getConnectionProperties().setProperty("password",
                                        protect.decrypt(hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().getProperty("password")));
                        hmsMirrorConfig.addWarning(MessageCode.DECRYPTED_PASSWORD, "*******");
                    } catch (Exception e) {
                        hmsMirrorConfig.addError(MessageCode.DECRYPTING_PASSWORD_ISSUE, "RIGHT HiveServer2");
                    }
                }
                if (hmsMirrorConfig.getCluster(Environment.LEFT).getMetastoreDirect() != null
                        && hmsMirrorConfig.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().getProperty("password") != null) {
                    try {
                        hmsMirrorConfig.getCluster(Environment.LEFT).getMetastoreDirect()
                                .getConnectionProperties().setProperty("password",
                                        protect.decrypt(hmsMirrorConfig.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().getProperty("password")));
                        hmsMirrorConfig.addWarning(MessageCode.DECRYPTED_PASSWORD, "*******");
                    } catch (Exception e) {
                        hmsMirrorConfig.addError(MessageCode.DECRYPTING_PASSWORD_ISSUE, "LEFT MetastoreDirect");
                    }
                }
            }

            // Before Proceeding, check if we are just working with password encryption/decryption.
            long errorCode = hmsMirrorConfig.getProgression().getErrors().getReturnCode() * -1;

            if (BigInteger.valueOf(errorCode).testBit(MessageCode.DECRYPTING_PASSWORD_ISSUE.getCode())) {//|| BigInteger.valueOf(passcodeError).testBit(MessageCode.PASSWORD_CFG)) {
                // Print Warnings to show the results of the password encryption/decryption.
                for (String message : progression.getErrors().getMessages()) {
                    log.error(message);
                }
                return;
            }
            if (BigInteger.valueOf(errorCode).testBit(MessageCode.PASSWORD_CFG.getCode())) {//|| BigInteger.valueOf(passcodeError).testBit(MessageCode.PASSWORD_CFG)) {
                // Print Warnings to show the results of the password encryption/decryption.
                for (String message : progression.getWarnings().getMessages()) {
                    log.warn(message);
                }
                return;
            }

            if (!hmsMirrorCfgService.validate()) {
                for (String message : progression.getErrors().getMessages()) {
                    log.error(message);
                }
                return;
            } else {
                // Set to true so downstream will continue.
                hmsMirrorConfig.setValidated(Boolean.TRUE);
            }

            if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.DUMP) {
                hmsMirrorConfig.setExecute(Boolean.FALSE); // No Actions.
                hmsMirrorConfig.setSync(Boolean.FALSE);
            }

            // Make adjustments to the config clusters based on settings.
            // Buildout the right connection pool details.
            Set<Environment> hs2Envs = new HashSet<Environment>();
            switch (hmsMirrorConfig.getDataStrategy()) {
                case DUMP:
                    // Don't load the datasource for the right with DUMP strategy.
                    if (hmsMirrorConfig.getDumpSource() == Environment.RIGHT) {
                        // switch LEFT and RIGHT
                        hmsMirrorConfig.getClusters().remove(Environment.LEFT);
                        hmsMirrorConfig.getClusters().put(Environment.LEFT, hmsMirrorConfig.getCluster(Environment.RIGHT));
                        hmsMirrorConfig.getCluster(Environment.LEFT).setEnvironment(Environment.LEFT);
                        hmsMirrorConfig.getClusters().remove(Environment.RIGHT);
                    }
                case STORAGE_MIGRATION:
                    // Get Pool
                    connectionPoolService.getConnectionPools().addHiveServer2(Environment.LEFT, hmsMirrorConfig.getCluster(Environment.LEFT).getHiveServer2());
                    hs2Envs.add(Environment.LEFT);
                    break;
                case SQL:
                case SCHEMA_ONLY:
                case EXPORT_IMPORT:
                case HYBRID:
                    // When doing inplace downgrade of ACID tables, we're only dealing with the LEFT cluster.
                    if (!hmsMirrorConfig.getMigrateACID().isInplace() && null != hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2()) {
                        connectionPoolService.getConnectionPools().addHiveServer2(Environment.RIGHT, hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2());
                        hs2Envs.add(Environment.RIGHT);
                    }
                default:
                    connectionPoolService.getConnectionPools().addHiveServer2(Environment.LEFT, hmsMirrorConfig.getCluster(Environment.LEFT).getHiveServer2());
                    hs2Envs.add(Environment.LEFT);
                    break;
            }
            if (hmsMirrorCfgService.loadPartitionMetadata()) {
                if (hmsMirrorConfig.getCluster(Environment.LEFT).getMetastoreDirect() != null) {
                    connectionPoolService.getConnectionPools().addMetastoreDirect(Environment.LEFT, hmsMirrorConfig.getCluster(Environment.LEFT).getMetastoreDirect());
                }
                if (hmsMirrorConfig.getCluster(Environment.RIGHT).getMetastoreDirect() != null) {
                    connectionPoolService.getConnectionPools().addMetastoreDirect(Environment.RIGHT, hmsMirrorConfig.getCluster(Environment.RIGHT).getMetastoreDirect());
                }
            }
            try {
                connectionPoolService.getConnectionPools().init();
                for (Environment target : hs2Envs) {
                    Connection conn = null;
                    Statement stmt = null;
                    try {
                        conn = connectionPoolService.getConnectionPools().getHS2EnvironmentConnection(target);
                        if (conn == null) {
                            if (target == Environment.RIGHT && hmsMirrorConfig.getCluster(target).getHiveServer2().isDisconnected()) {
                                // Skip error.  Set Warning that we're disconnected.
                                progression.addWarning(ENVIRONMENT_DISCONNECTED, target);
                            } else if (!hmsMirrorConfig.isLoadingTestData()) {
                                progression.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
                            }
                        } else {
                            // Exercise the connection.
                            stmt = conn.createStatement();
                            stmt.execute("SELECT 1");
                        }
                    } catch (SQLException se) {
                        if (target == Environment.RIGHT && hmsMirrorConfig.getCluster(target).getHiveServer2().isDisconnected()) {
                            // Set warning that RIGHT is disconnected.
                            progression.addWarning(ENVIRONMENT_DISCONNECTED, target);
                        } else {
                            log.error(se.getMessage(), se);
                            progression.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
                        }
                    } catch (Throwable t) {
                        log.error(t.getMessage(), t);
                        progression.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
                    } finally {
                        if (stmt != null) {
                            stmt.close();
                        }
                        if (conn != null) {
                            conn.close();
                        }
                    }
                }
            } catch (SQLException cnfe) {
                log.error("Issue initializing connections.  Check driver locations", cnfe);
                throw new RuntimeException(cnfe);
            }

            hmsMirrorConfig.getCluster(Environment.LEFT).setPools(connectionPoolService.getConnectionPools());
            switch (hmsMirrorConfig.getDataStrategy()) {
                case DUMP:
                    // Don't load the datasource for the right with DUMP strategy.
                    break;
                default:
                    // Don't set the Pools when Disconnected.
                    if (hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2() != null && !hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
                        hmsMirrorConfig.getCluster(Environment.RIGHT).setPools(connectionPoolService.getConnectionPools());
                    }
            }

            if (hmsMirrorCfgService.isConnectionKerberized()) {
                log.debug("Detected a Kerberized JDBC Connection.  Attempting to setup/initialize GSS.");
                hmsMirrorCfgService.setupGSS();
            }
            log.debug("Checking Hive Connections");
            if (!hmsMirrorConfig.isLoadingTestData() && !hmsMirrorCfgService.checkConnections()) {
                log.error("Check Hive Connections Failed.");
                if (hmsMirrorCfgService.isConnectionKerberized()) {
                    log.error("Check Kerberos configuration if GSS issues are encountered.  See the running.md docs for details.");
                }
                throw new RuntimeException("Check Hive Connections Failed.  Check Logs.");
            }

        };
    }

    public String[] toSpringBootOption(String[] args) {
        CommandLine cmd = getCommandLine(args);
        List<String> springOptions = new ArrayList<>();
        for (Option option : cmd.getOptions()) {
            String opt = option.getLongOpt();
            String[] values = option.getValues();
            if (opt.equals("config")) {
                // Handle the config file differently
                springOptions.add("--hms-mirror.config-filename" + opt + "=\"" + String.join(",", values) + "\"");
            } else {
                if (values != null && values.length > 0) {
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
