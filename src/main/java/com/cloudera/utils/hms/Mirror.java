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

import com.cloudera.utils.hms.mirror.Progression;
import com.cloudera.utils.hms.mirror.config.ApplicationConfig;
import com.cloudera.utils.hms.mirror.service.ApplicationService;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
@Slf4j
public class Mirror {

    public static void main(String[] args) {
        // Translate the legacy command line arguments to Spring Boot arguments
        //    before starting the application.
        log.info("Translating command line arguments to Spring Boot arguments");
        CommandLineOptions commandLineOptions = new CommandLineOptions();
        String[] springArgs = commandLineOptions.toSpringBootOption(args);
        log.info("Translated Spring Boot arguments: " + String.join(" ", springArgs));
        log.info("STARTING THE APPLICATION");
        ConfigurableApplicationContext applicationContext = SpringApplication.run(Mirror.class, springArgs);
//        ApplicationConfig applicationConfig = applicationContext.getBean(ApplicationConfig.class);
//        int rtn = applicationConfig.start();
        Progression progression = applicationContext.getBean(Progression.class);
        ApplicationService applicationService = applicationContext.getBean(ApplicationService.class);
        ConfigService configService = applicationContext.getBean(ConfigService.class);
        if (!configService.getConfig().isWebInterface()) {
            long returnCode = applicationService.getReturnCode();
            int rtnCode = (int) returnCode;
            if (returnCode > Integer.MAX_VALUE) {
                log.error("Return code is greater than Integer.MAX_VALUE.  Setting return code to Integer.MAX_VALUE. Check Logs for errors.");
                rtnCode = Integer.MAX_VALUE;
            }
            System.exit(rtnCode);
        } else {
            log.info("The Application Workflow has completed");
            log.info("Use the web interface to review the results");
        }
        log.info("APPLICATION FINISHED");
    }
}
