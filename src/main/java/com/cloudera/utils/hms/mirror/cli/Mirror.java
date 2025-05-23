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

package com.cloudera.utils.hms.mirror.cli;

import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.HMSMirrorAppService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@ComponentScans({
        // For the Hadoop CLI Interface
        @ComponentScan(basePackages = "com.cloudera.utils.hadoop")
        , @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.cli")
        , @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.datastrategy")
        , @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.domain")
        , @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.reporting")
        , @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.service")
        , @ComponentScan(basePackages = "com.cloudera.utils.hms.mirror.util")
})
//@ComponentScan("com.cloudera.utils.hadoop")
@EnableAsync
@Slf4j
public class Mirror {

    public static void main(String[] args) {
        // Translate the legacy command line arguments to Spring Boot arguments
        //    before starting the application.
        log.info("Translating command line arguments to Spring Boot arguments");
        String[] springArgs = HmsMirrorCommandLineOptions.toSpringBootOption(Boolean.TRUE, args);
        log.info("Translated Spring Boot arguments: {}", String.join(" ", springArgs));
        log.info("STARTING THE APPLICATION");

        ConfigurableApplicationContext applicationContext = SpringApplication.run(Mirror.class, springArgs);

        HMSMirrorAppService appService = applicationContext.getBean(HMSMirrorAppService.class);
        ExecuteSessionService executeSessionService = applicationContext.getBean(ExecuteSessionService.class);

        long returnCode = appService.getReturnCode();
        int rtnCode = (int) returnCode;
        if ((returnCode * -1) > Integer.MAX_VALUE) {
            log.error("Return code is greater than Integer.MAX_VALUE.  Setting return code to Integer.MAX_VALUE. Check Logs for errors.");
            for (String message : executeSessionService.getSession().getRunStatus().getErrors().getMessages()) {
                log.error(message);
            }
            rtnCode = Integer.MAX_VALUE;
        }

        log.info("APPLICATION FINISHED");

        System.exit(rtnCode);

    }
}
