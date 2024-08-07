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

package com.cloudera.utils.hms.mirror.web.config;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.service.DomainService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;

@Configuration
@Slf4j
public class WebInit {

    private DomainService domainService;
    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setDomainService(DomainService domainService) {
        this.domainService = domainService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Bean
    public CommandLineRunner initDefaultConfig(@Value("${hms-mirror.config.path}") String configPath,
                                               @Value("${hms-mirror.config.filename}") String configFilename) {
        return args -> {
            // hms-mirror.config.filename is set in the application.yaml file with the
            //    default location.  It can be overridden by setting the commandline
            //    --hms-mirror.config.filename=<filename>.
            String configFullFilename = configPath + File.separator + configFilename;
            File cfg = new File(configFullFilename);
            HmsMirrorConfig hmsMirrorConfig;
            if (cfg.exists()) {
                log.info("Loading config from: {}", configFullFilename);
                hmsMirrorConfig = domainService.deserializeConfig(configFullFilename);
            } else {
                // Return empty config.  This will require the user to setup the config.
                log.warn("Default config not found.");
//                hmsMirrorConfig = new HmsMirrorConfig();
            }
//            ExecuteSession createdSession = executeSessionService.createSession(null, hmsMirrorConfig);
//            executeSessionService.setLoadedSession(createdSession);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.output-dir")
        // Will set this when the value is set externally.
    CommandLineRunner configOutputDir(@Value("${hms-mirror.config.output-dir}") String value) {
        return configOutputDirInternal(value);
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.output-dir",
            havingValue = "false")
        // Will set this when the value is NOT set and picks up the default application.yaml (false) setting.
    CommandLineRunner configOutputDirNotSet() {
        String value = System.getProperty("user.home") + File.separator + ".hms-mirror/reports";
        return configOutputDirInternal(value);
    }

    CommandLineRunner configOutputDirInternal(String value) {
        return args -> {
            log.info("output-dir: {}", value);
            executeSessionService.setReportOutputDirectory(value, true);
            File reportPathDir = new File(value);
            if (!reportPathDir.exists()) {
                reportPathDir.mkdirs();
            }

            File testFile = new File(value + FileSystems.getDefault().getSeparator() + ".dir-check");

            // Test file to ensure we can write to it for the report.
            try {
                new FileOutputStream(testFile).close();
            } catch (IOException e) {
                throw new RuntimeException("Can't write to report output directory: " + value, e);
            }
        };
    }

}
