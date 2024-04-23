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

package com.cloudera.utils.hms.mirror.config;

import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;

@Configuration
@Slf4j
@Getter
@Setter
class ConversionInitialization {

    @Bean(name = "conversion")
    @Order(10)
    @ConditionalOnProperty(
            name = "hms-mirror.conversion.test-filename",
            havingValue = "false")
    public Conversion buildConversion() {
        log.info("Building Clean Conversion Instance");
        return new Conversion();
    }

    @Bean(name = "conversion")
    @Order(500)
    @ConditionalOnProperty(
            name = "hms-mirror.conversion.test-filename")
    public Conversion loadTestData(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.conversion.test-filename}") String filename) throws IOException {
        Conversion conversion = null;
        hmsMirrorConfig.setLoadTestDataFile(filename);
        log.info("Reconstituting Conversion from test data file: {}", filename);
        try {
            log.info("Check 'classpath' for test data file");
            URL configURL = this.getClass().getResource(filename);
            if (configURL == null) {
                log.info("Checking filesystem for test data file");
                File conversionFile = new File(filename);
                if (!conversionFile.exists())
                    throw new RuntimeException("Couldn't locate test data file: " + filename);
                configURL = conversionFile.toURI().toURL();
            }
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            String yamlCfgFile = IOUtils.toString(configURL, StandardCharsets.UTF_8);
            conversion = mapper.readerFor(Conversion.class).readValue(yamlCfgFile);
            // Set Config Databases;
            hmsMirrorConfig.setDatabases(conversion.getDatabases().keySet().toArray(new String[0]));
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
        return conversion;
    }

    @Bean
    // Needs to happen after all the configs have been set.
    @Order(15)
    public CommandLineRunner conversionPostProcessing(HmsMirrorConfig hmsMirrorConfig, Conversion conversion) {
        return args -> {
            log.info("Post Processing Conversion");
            if (hmsMirrorConfig.isLoadingTestData()) {
                for (DBMirror dbMirror : conversion.getDatabases().values()) {
                    String database = dbMirror.getName();
                    for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
                        String tableName = tableMirror.getName();
                        if (TableUtils.isACID(tableMirror.getEnvironmentTable(Environment.LEFT))
                                && !hmsMirrorConfig.getMigrateACID().isOn()) {
                            tableMirror.setRemove(true);
                        } else if (!TableUtils.isACID(tableMirror.getEnvironmentTable(Environment.LEFT))
                                && hmsMirrorConfig.getMigrateACID().isOnly()) {
                            tableMirror.setRemove(true);
                        } else {
                            // Same logic as in TableService.getTables to filter out tables that are not to be processed.
                            if (tableName.startsWith(hmsMirrorConfig.getTransfer().getTransferPrefix())) {
                                log.info("Database: {} Table: {} was NOT added to list.  The name matches the transfer prefix and is most likely a remnant of a previous event. If this is a mistake, change the 'transferPrefix' to something more unique.", database, tableName);
                                tableMirror.setRemove(true);
                            } else if (tableName.endsWith("storage_migration")) {
                                log.info("Database: {} Table: {} was NOT added to list.  The name is the result of a previous STORAGE_MIGRATION attempt that has not been cleaned up.", database, tableName);
                                tableMirror.setRemove(true);
                            } else {
                                if (hmsMirrorConfig.getFilter().getTblRegEx() != null) {
                                    // Filter Tables
                                    assert (hmsMirrorConfig.getFilter().getTblFilterPattern() != null);
                                    Matcher matcher = hmsMirrorConfig.getFilter().getTblFilterPattern().matcher(tableName);
                                    if (!matcher.matches()) {
                                        log.info("{}:{} didn't match table regex filter and will NOT be added to processing list.", database, tableName);
                                        tableMirror.setRemove(true);
                                    }
                                } else if (hmsMirrorConfig.getFilter().getTblExcludeRegEx() != null) {
                                    assert (hmsMirrorConfig.getFilter().getTblExcludeFilterPattern() != null);
                                    Matcher matcher = hmsMirrorConfig.getFilter().getTblExcludeFilterPattern().matcher(tableName);
                                    if (matcher.matches()) { // ANTI-MATCH
                                        log.info("{}:{} matched exclude table regex filter and will NOT be added to processing list.", database, tableName);
                                        tableMirror.setRemove(true);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Remove Tables from Map.
            for (DBMirror dbMirror : conversion.getDatabases().values()) {
                dbMirror.getTableMirrors().entrySet().removeIf(entry -> entry.getValue().isRemove());
            }
        };
    }
}
