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

package com.cloudera.utils.hms.mirror.cli.config;

import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.cli.CliReporter;
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.DomainService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Configuration
@Slf4j
public class CliInit {

    private ConfigService configService;
    private DomainService domainService;
    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setDomainService(DomainService domainService) {
        this.domainService = domainService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    private HmsMirrorConfig initializeConfig(String configFilename) {
        HmsMirrorConfig hmsMirrorConfig;
        log.info("Initializing Config.");
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        URL cfgUrl = null;
        try {
            // Load file from classpath and convert to string
            File cfgFile = new File(configFilename);
            if (!cfgFile.exists()) {
                // Try loading from resource (classpath).  Mostly for testing.
                cfgUrl = this.getClass().getResource(configFilename);
                if (isNull(cfgUrl)) {
                    throw new RuntimeException("Couldn't locate configuration file: " + configFilename);
                }
                log.info("Using 'classpath' config: {}", configFilename);
            } else {
                log.info("Using filesystem config: {}", configFilename);
                try {
                    cfgUrl = cfgFile.toURI().toURL();
                } catch (MalformedURLException mfu) {
                    throw new RuntimeException("Couldn't locate configuration file: "
                            + configFilename, mfu);
                }
            }

            String yamlCfgFile = IOUtils.toString(cfgUrl, StandardCharsets.UTF_8);
            hmsMirrorConfig = mapper.readerFor(HmsMirrorConfig.class).readValue(yamlCfgFile);
//            hmsMirrorConfig.setRunStatus(runStatus);
            // Link the translator to the config
//            hmsMirrorConfig.getTranslator().setHmsMirrorConfig(hmsMirrorConfig);
//            for (Cluster cluster : hmsMirrorConfig.getClusters().values()) {
//                cluster.setHmsMirrorConfig(hmsMirrorConfig);
//            }
            hmsMirrorConfig.setConfigFilename(configFilename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("Config loaded.");
        return hmsMirrorConfig;
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.setup",
            havingValue = "false")
    public HmsMirrorConfig loadHmsMirrorConfig(@Value("${hms-mirror.config.path}") String configPath,
                                               @Value("${hms-mirror.config.filename}") String configFile) {
        String fullConfigPath;
        // If file is absolute, use it.  Otherwise, use the path.
        if (configFile.startsWith(File.separator)) {
            fullConfigPath = configFile;
        } else {
            fullConfigPath = configPath + File.separator + configFile;
        }
        return domainService.deserializeConfig(fullConfigPath);
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.setup",
            havingValue = "true")
    /*
    Init empty for framework to fill in.
     */
    public HmsMirrorConfig loadHmsMirrorConfigWithSetup() {
        return new HmsMirrorConfig();
    }

//    @Bean(name = "runStatus")
//    @Order(10)
//    @ConditionalOnProperty(
//            name = "hms-mirror.conversion.test-filename",
//            havingValue = "false")
//    public RunStatus buildRunStatus() {
//        log.info("Building Clean RunStatus Instance");
//        return new RunStatus();
//    }

    /*

     */
    @Bean
    @Order(5)
    @ConditionalOnProperty(
            name = "hms-mirror.conversion.test-filename")
    public CommandLineRunner loadTestData(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.conversion.test-filename}") String filename) throws IOException {
//        RunStatus runStatus = new RunStatus();
        return args -> {
            hmsMirrorConfig.setLoadTestDataFile(filename);
        };
    }

    private void loadTestData(ExecuteSession session) {
        log.info("Loading Test Data");
        HmsMirrorConfig config = session.getConfig();
        try {
            String filename = config.getLoadTestDataFile();
            log.info("Reconstituting Conversion from test data file: {}", filename);
            log.info("Checking 'classpath' for test data file");
            URL configURL = this.getClass().getResource(filename);
            if (isNull(configURL)) {
                log.info("Checking filesystem for test data file: {}", filename);
                File conversionFile = new File(filename);
                if (!conversionFile.exists())
                    throw new RuntimeException("Couldn't locate test data file: " + filename);
                configURL = conversionFile.toURI().toURL();
            }
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            String yamlCfgFile = IOUtils.toString(configURL, StandardCharsets.UTF_8);
            Conversion conversion = mapper.readerFor(Conversion.class).readValue(yamlCfgFile);
            // Set Config Databases;
            List<String> databases = new ArrayList<>(conversion.getDatabases().keySet());
            config.setDatabases(databases);
            // Replace the conversion in the session.
            executeSessionService.getSession().setConversion(conversion);
        } catch (UnrecognizedPropertyException upe) {
            throw new RuntimeException("\nThere may have been a breaking change in the configuration since the previous " +
                    "release. Review the note below and remove the 'Unrecognized field' from the configuration and try " +
                    "again.\n\n", upe);
        } catch (Throwable t) {
            // Look for yaml update errors.
            if (t.toString().contains("MismatchedInputException")) {
                throw new RuntimeException("The format of the 'config' yaml file MAY HAVE CHANGED from the last release.  Please make a copy and run " +
                        "'-su|--setup' again to recreate in the new format", t);
            } else {
                log.error(t.getMessage(), t);
                throw new RuntimeException("A configuration element is no longer valid, progress.  Please remove the element from the configuration yaml and try again.", t);
            }
        }

    }

    @Bean
    // Needs to happen after all the configs have been set.
    @Order(15)
    public CommandLineRunner conversionPostProcessing(HmsMirrorConfig builtConfig,
                                                      @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) {
        return args -> {
            // TODO: Need to review this process for redundancy.
            ExecuteSession session = executeSessionService.createSession(null, builtConfig);
            executeSessionService.setCurrentSession(session);

            // Sync the concurrency for the connections.
            // We need to pass on a few scale parameters to the hs2 configs so the connections pools can handle the scale requested.
            if (nonNull(builtConfig.getCluster(Environment.LEFT)) && nonNull(builtConfig.getCluster(Environment.LEFT).getHiveServer2())) {
                Cluster cluster = builtConfig.getCluster(Environment.LEFT);
                cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(maxThreads / 2));
                cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(maxThreads / 2));
                if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(maxThreads));
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "10000");
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(maxThreads));
                }
            }
            if (nonNull(builtConfig.getCluster(Environment.RIGHT)) && nonNull(builtConfig.getCluster(Environment.RIGHT).getHiveServer2())) {
                Cluster cluster = builtConfig.getCluster(Environment.RIGHT);
                if (cluster.getHiveServer2() != null) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(maxThreads / 2));
                    cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(maxThreads / 2));
                    if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(maxThreads));
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "10000");
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(maxThreads));
                    }
                }
            }



            try {
                executeSessionService.transitionLoadedSessionToActive(maxThreads);
            } catch (SessionException e) {
                throw new RuntimeException(e);
            }

//            log.info("Session transitioned to active.");
//            builtConfig.setValidated(Boolean.TRUE);

            session = executeSessionService.getSession();

            HmsMirrorConfig config = executeSessionService.getSession().getConfig();

            Conversion conversion = null;
            log.info("Post Processing Conversion");
            if (config.isLoadingTestData()) {
                // Load Test Data.
                loadTestData(session);

                conversion = executeSessionService.getSession().getConversion();

                // Clean up the test data to match the configuration.
                for (DBMirror dbMirror : conversion.getDatabases().values()) {
                    String database = dbMirror.getName();
                    for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
                        String tableName = tableMirror.getName();
                        if (config.isDatabaseOnly()) {
                            // Only work with the database.
                            tableMirror.setRemove(true);
                        } else if (TableUtils.isACID(tableMirror.getEnvironmentTable(Environment.LEFT))
                                && !config.getMigrateACID().isOn()) {
                            tableMirror.setRemove(true);
                        } else if (!TableUtils.isACID(tableMirror.getEnvironmentTable(Environment.LEFT))
                                && config.getMigrateACID().isOnly()) {
                            tableMirror.setRemove(true);
                        } else {
                            // Same logic as in TableService.getTables to filter out tables that are not to be processed.
                            if (tableName.startsWith(config.getTransfer().getTransferPrefix())) {
                                log.info("Database: {} Table: {} was NOT added to list.  The name matches the transfer prefix and is most likely a remnant of a previous event. If this is a mistake, change the 'transferPrefix' to something more unique.", database, tableName);
                                tableMirror.setRemove(true);
                                tableMirror.setRemoveReason("Table name starts with transfer prefix.");
                            } else if (tableName.endsWith(config.getTransfer().getStorageMigrationPostfix())) {
                                log.info("Database: {} Table: {} was NOT added to list.  The name is the result of a previous STORAGE_MIGRATION attempt that has not been cleaned up.", database, tableName);
                                tableMirror.setRemove(true);
                                tableMirror.setRemoveReason("Table name ends with storage migration postfix.");
                            } else {
                                if (config.getFilter().getTblRegEx() != null) {
                                    // Filter Tables
                                    assert (config.getFilter().getTblFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblFilterPattern().matcher(tableName);
                                    if (!matcher.matches()) {
                                        log.info("{}:{} didn't match table regex filter and will NOT be added to processing list.", database, tableName);
                                        tableMirror.setRemove(true);
                                    }
                                } else if (config.getFilter().getTblExcludeRegEx() != null) {
                                    assert (config.getFilter().getTblExcludeFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblExcludeFilterPattern().matcher(tableName);
                                    if (matcher.matches()) { // ANTI-MATCH
                                        log.info("{}:{} matched exclude table regex filter and will NOT be added to processing list.", database, tableName);
                                        tableMirror.setRemove(true);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                conversion = executeSessionService.getSession().getConversion();
            }
            // Remove Tables from Map.
            for (DBMirror dbMirror : conversion.getDatabases().values()) {
                dbMirror.getTableMirrors().entrySet().removeIf(entry -> entry.getValue().isRemove());
            }
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
            executeSessionService.setReportOutputDirectory(value, false);
//            File reportPathDir = new File(value);
//            if (!reportPathDir.exists()) {
//                reportPathDir.mkdirs();
//            }
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
//            try {
//                new FileOutputStream(testFile).close();
//            } catch (IOException e) {
//                throw new RuntimeException("Can't write to output directory: " + value, e);
//            }

        };
    }

}
