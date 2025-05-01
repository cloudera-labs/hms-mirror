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
package com.cloudera.utils.hms.mirror.cli.run;

import com.cloudera.utils.hms.mirror.cli.CliReporter;
import com.cloudera.utils.hms.mirror.cli.HmsMirrorCommandLineOptions;
import com.cloudera.utils.hms.mirror.service.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.util.concurrent.Future;

/*
Using the config, go through the databases and tables and collect the current states.
Create the target databases, where needed to support the migration.
 */
@Configuration
@Slf4j
@Getter
@Setter
public class HmsMirrorAppCfg {

    private final ReportWriterService reportWriterService;
    private final CliReporter cliReporter;
    private final HmsMirrorCommandLineOptions hmsMirrorCommandLineOptions;
    private final ExecuteSessionService executeSessionService;
    private final ConnectionPoolService connectionPoolService;
    private final DatabaseService databaseService;
    private final HMSMirrorAppService hmsMirrorAppService;
    private final TableService tableService;
    private final TransferService transferService;

    public HmsMirrorAppCfg(
            ReportWriterService reportWriterService,
            CliReporter cliReporter,
            HmsMirrorCommandLineOptions hmsMirrorCommandLineOptions,
            ExecuteSessionService executeSessionService,
            ConnectionPoolService connectionPoolService,
            DatabaseService databaseService,
            HMSMirrorAppService hmsMirrorAppService,
            TableService tableService,
            TransferService transferService
    ) {
        this.reportWriterService = reportWriterService;
        this.cliReporter = cliReporter;
        this.hmsMirrorCommandLineOptions = hmsMirrorCommandLineOptions;
        this.executeSessionService = executeSessionService;
        this.connectionPoolService = connectionPoolService;
        this.databaseService = databaseService;
        this.hmsMirrorAppService = hmsMirrorAppService;
        this.tableService = tableService;
        this.transferService = transferService;
    }

    // TODO: Need to address failures here...
    @Bean
    @Order(1000) // Needs to be the last thing to run.
    // Don't run when encrypting/decrypting passwords.
    @ConditionalOnProperty(
            name = "hms-mirror.config.password",
            matchIfMissing = true)
    public CommandLineRunner start() {
        return args -> {
            // NOTE: The transitionToActive process happens in another bean....
            Future<Boolean> result = hmsMirrorAppService.run();
            while (!result.isDone()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            cliReporter.refresh(Boolean.TRUE);
        };
    }
}