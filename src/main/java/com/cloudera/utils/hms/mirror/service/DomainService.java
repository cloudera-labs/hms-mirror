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

import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.isNull;

@Component
@Slf4j
public class DomainService {

    private ObjectMapper yamlMapper;

    @Autowired
    public void setYamlMapper(ObjectMapper yamlMapper) {
        this.yamlMapper = yamlMapper;
    }

    public DBMirror deserializeDBMirror(String fileName) {
        DBMirror dbMirror = null;
        String dbMirrorAsString = fileToString(fileName);
        try {
            dbMirror = yamlMapper.readerFor(DBMirror.class).readValue(dbMirrorAsString);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return dbMirror;
    }

    public Conversion deserializeConversion(String fileName) {
        String conversionAsString = fileToString(fileName);
        Conversion conversion = null;
        try {
            conversion = yamlMapper.readerFor(Conversion.class).readValue(conversionAsString);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return conversion;
    }

    protected String fileToString(String configFilename) {
        String fileAsString = null;
        // Check if absolute path.
        if (!configFilename.startsWith("/")) {
            // If filename contain a file.separator, assume the location is
            // relative to the working directory.
            if ((configFilename.contains(File.separator))) {
                // Load relative to the working directory.
                File workingDir = new File(".");
                configFilename = workingDir.getAbsolutePath() + File.separator + configFilename;
            } else {
                // Assume it's in the default config directory.
                configFilename = System.getProperty("user.home") + File.separator + ".hms-mirror/cfg"
                        + File.separator + configFilename;
            }
        }
        log.info("Loading config: {}", configFilename);
//        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
//        yamlMapper.enable(SerializationFeature.INDENT_OUTPUT);
        URL cfgUrl = null;
        try {
            // Load file from classpath and convert to string
            File cfgFile = new File(configFilename);
            if (!cfgFile.exists()) {
                // Try loading from resource (classpath).  Mostly for testing.
                cfgUrl = yamlMapper.getClass().getResource(configFilename);
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

            fileAsString = IOUtils.toString(cfgUrl, StandardCharsets.UTF_8);
//            config = yamlMapper.readerFor(HmsMirrorConfig.class).readValue(yamlCfgFile);

//            config.setConfigFilename(configFilename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fileAsString;
    }

    public HmsMirrorConfig deserializeConfig(String configFilename) {
        HmsMirrorConfig config;
        log.info("Initializing Config.");
        String yamlCfgFile = fileToString(configFilename);
        try {
            config = yamlMapper.readerFor(HmsMirrorConfig.class).readValue(yamlCfgFile);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        config.setConfigFilename(configFilename);
        return config;
    }

    public Map<String, Map<String, Set<String>>> deserializeDistCpWorkbook(String fileName) {
        String distcpWorkbookAsString = fileToString(fileName);
        Map<String, Map<String, Set<String>>> distcpWorkbook = null;
        try {
            distcpWorkbook = yamlMapper.readerFor(new TypeReference<Map<String, Map<String, Set<String>>>>() {
            }).readValue(distcpWorkbookAsString);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return distcpWorkbook;
    }

}
