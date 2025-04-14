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

package com.cloudera.utils.hms.mirror.password;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

@Configuration
@Order(5)
@Slf4j
@Getter
@Setter
public class PasswordOptions {

//    @Bean
//    @Order(1)
//    @ConditionalOnProperty(prefix = "hms-mirror",
//            name = "config.filename",
//            matchIfMissing = true)
//    HmsMirrorConfig loadHmsMirrorConfigFromFile(@Value("${hms-mirror.config.filename}") String configFilename) {
//        HmsMirrorConfig hmsMirrorConfig;
//        try {
//            hmsMirrorConfig = HmsMirrorConfig.loadConfig(configFilename);
//        } catch(RuntimeException rte) {
//            // Couldn't locate the config file.
//            log.error("Couldn't locate the config file: {}. Creating empty hmsMirrorConfig object", configFilename);
//            hmsMirrorConfig = new HmsMirrorConfig();
//        }
//        return hmsMirrorConfig;
//    }

//    @Bean
//    ExecuteSession executeSession(HmsMirrorConfig hmsMirrorConfig) {
//        ExecuteSession executeSession = new ExecuteSession();
//        executeSession.setRunStatus(new RunStatus());
//        executeSession.setConfig(hmsMirrorConfig);
//        return executeSession;
//    }

    @Bean("password")
    @Order(1)
    Password configPasswordInstance() {
        Password password = new Password();
        return password;
    }

    @Bean
    @Order(2)
    @ConditionalOnProperty(
            name = "hms-mirror.config.decrypt-password")
    CommandLineRunner configDecryptPassword(Password password, @Value("${hms-mirror.config.decrypt-password}") String value) {
        return args -> {
            log.info("decrypt-password: {}", value);
            password.setValue(value);
            password.setConversion(Password.Conversion.DECRYPT);
//            hmsMirrorConfig.setEncryptedPassword(value);
        };
    }

    @Bean
    @Order(2)
    @ConditionalOnProperty(
            name = "hms-mirror.config.password")
    CommandLineRunner configPassword(Password password, @Value("${hms-mirror.config.password}") String value) {
        return args -> {
            log.info("password: {}", "********");
            password.setValue(value);
            password.setConversion(Password.Conversion.ENCRYPT);
//            executeSession.setPassword(value);
//            hmsMirrorConfig.setPassword(value);
        };
    }

    @Bean
    @Order(2)
    @ConditionalOnProperty(
            name = "hms-mirror.config.password-key")
    CommandLineRunner configPasswordKey(Password password, @Value("${hms-mirror.config.password-key}") String value) {
        return args -> {
            log.info("password-key: {}", value);
            password.setPasswordKey(value);
//            executeSession.setPasswordKey(value);
//            hmsMirrorConfig.setPasswordKey(value);
        };
    }

}
