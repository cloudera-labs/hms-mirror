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

import com.cloudera.utils.hms.mirror.Cluster;
import com.cloudera.utils.hms.mirror.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.Progression;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Configuration
@Slf4j
class HmsMirrorConfigInit {

    private HmsMirrorConfig initializeConfig(Progression progression, String configFilename) {
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
                if (cfgUrl == null) {
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
            hmsMirrorConfig.setProgression(progression);
            // Link the translator to the config
            hmsMirrorConfig.getTranslator().setHmsMirrorConfig(hmsMirrorConfig);
            for (Cluster cluster : hmsMirrorConfig.getClusters().values()) {
                cluster.setHmsMirrorConfig(hmsMirrorConfig);
            }
            hmsMirrorConfig.setConfigFilename(configFilename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("Config loaded.");
        log.info("Transfer Concurrency: {}", hmsMirrorConfig.getTransfer().getConcurrency());
        return hmsMirrorConfig;
    }

    @Bean
    @Order(1)
    public HmsMirrorConfig loadConfig(Progression progression, @Value("${hms-mirror.config-filename}") String configFilename) {
        return initializeConfig(progression, configFilename);
    }

}
