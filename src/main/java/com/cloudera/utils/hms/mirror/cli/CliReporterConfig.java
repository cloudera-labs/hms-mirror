/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

@Configuration
@Getter
@Setter
@Slf4j
public class CliReporterConfig {
    private CliReporter cliReporter;

    @Autowired
    public CliReporterConfig(CliReporter cliReporter) {
        this.cliReporter = cliReporter;
    }

    @Bean
    @Order(21) // Needs to run before the main application logic in "ApplicationConfig".
    // Don't run when encrypting/decrypting passwords.
    @ConditionalOnProperty(
            name = "hms-mirror.config.password",
            matchIfMissing = true)
    public CommandLineRunner launchCliReporting() {
        return args -> {
            log.info("Launching CLI Reporting");
            getCliReporter().run();
        };
    }


}
