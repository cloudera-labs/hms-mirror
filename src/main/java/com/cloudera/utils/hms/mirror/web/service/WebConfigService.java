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

package com.cloudera.utils.hms.mirror.web.service;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.service.DomainService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Set;
import java.util.TreeSet;

import static com.cloudera.utils.hms.mirror.util.ModelUtils.getSupportedDataStrategies;

@Service
@Slf4j
public class WebConfigService {

    private DomainService domainService;
    private String configPath = System.getProperty("user.home") + File.separator + ".hms-mirror/cfg";

    private ExecuteSessionService executeSessionService;

    @Bean
    @ConditionalOnProperty(
            name = "hms-mirror.config.path")
    CommandLineRunner setConfigPath(@Value("${hms-mirror.config.path}") String value) {
        // Check that directory exists.
        return args -> {
            File file = new File(value);
            if (!file.exists()) {
                file.mkdirs();
            }
            this.configPath = value;
        };
    }

    @Autowired
    public void setDomainService(DomainService domainService) {
        this.domainService = domainService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }


    /*
    Scan the config directory and return a list of all the config files.
     */
    public Set<String> getConfigList() {
        // Scan a directory and return a list of all the files with a .yaml extension.
        Set<String> configList = new TreeSet<>();

        // Users home directory
        String cfgPath = System.getProperty("user.home") + File.separator + ".hms-mirror/cfg";

        File folder = new File(cfgPath);
        File[] listOfFiles = folder.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return (name.toLowerCase().endsWith(".yml") | name.toLowerCase().endsWith(".yaml") |
                        name.toLowerCase().contains(".yml") | name.toLowerCase().contains(".yaml"));
            }
        });

        for (File file : listOfFiles) {
            if (file.isFile()) {
                // Attempt to load and check that it is one of the supported config.
                log.info("Checking config file: " + file.getName());
                try {
                    HmsMirrorConfig lclConfig = domainService.deserializeConfig(file.getName());
                    if (lclConfig != null && getSupportedDataStrategies().contains(lclConfig.getDataStrategy())) {
                        configList.add(file.getName());
                    } else {
                        log.warn("Config file: " + file.getName() + " is not supported.  It isn't one of the currently supported data strategies.");
                    }
                } catch (Exception e) {
                    log.error("Error loading config file: " + file.getName());
                }
            }
        }

        // Handle the case where no valid config files are found. This will help get us past the chicken and egg scenario
        //    with the UI/
        if (configList.isEmpty()) {
            configList.add("--non defined--");
            log.warn("No valid config files found in the config directory: " + cfgPath);
        }

        return configList;
    }

//    public HmsMirrorConfig getCurrentConfig() {
//        return executeSessionService.getHmsMirrorConfig();
//    }
//
//    public HmsMirrorConfig loadConfig(String configFileName) {
//        return executeSessionService.loadConfig(configFileName);
//    }
}
