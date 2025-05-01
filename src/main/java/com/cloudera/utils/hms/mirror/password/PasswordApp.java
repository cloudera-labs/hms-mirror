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

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.cli.HmsMirrorCommandLineOptions;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.text.MessageFormat;

@SpringBootApplication
//@ComponentScans({
//        // For the Hadoop CLI Interface
//        @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.service.runtime")
//})
//@EnableAsync
@Slf4j
public class PasswordApp {

    public static void main(String[] args) throws EncryptionException {
        // Translate the legacy command line arguments to Spring Boot arguments
        //    before starting the application.
        log.info("Translating command line arguments to Spring Boot arguments");
//        HmsMirrorCommandLineOptions hmsMirrorCommandLineOptions = new HmsMirrorCommandLineOptions();
        String[] springArgs = HmsMirrorCommandLineOptions.toSpringBootOption(Boolean.TRUE, args);
        log.info("Translated Spring Boot arguments: {}", String.join(" ", springArgs));
        log.info("STARTING THE APPLICATION");

        ConfigurableApplicationContext applicationContext = SpringApplication.run(PasswordApp.class, springArgs);

        PasswordService passwordService = new PasswordService();

        Password password = applicationContext.getBean(Password.class);

        switch (password.getConversion()) {
            case DECRYPT:
                String decryptedPassword = passwordService.decryptPassword(password.getPasswordKey(), password.getValue());
                String dmessage = MessageFormat.format(MessageCode.DECRYPTED_PASSWORD.getDesc(), decryptedPassword);
                log.warn(dmessage);
                break;
            case ENCRYPT:
                String encryptedPassword = passwordService.encryptPassword(password.getPasswordKey(), password.getValue());
                String emessage = MessageFormat.format(MessageCode.ENCRYPTED_PASSWORD.getDesc(), encryptedPassword);
                log.warn(emessage);
                break;
        }

        log.info("APPLICATION FINISHED");

        System.exit(0);

    }
}
