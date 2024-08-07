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
import org.springframework.beans.factory.annotation.Autowired;
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
//    private static final Logger log = LoggerFactory.getLogger(Setup.class);

    @Getter
    private ReportWriterService reportWriterService = null;

    private CliReporter cliReporter = null;
    @Getter
    private HmsMirrorCommandLineOptions hmsMirrorCommandLineOptions = null;
    @Getter
    private ExecuteSessionService executeSessionService = null;
    @Getter
    private ConnectionPoolService connectionPoolService = null;
    @Getter
    private DatabaseService databaseService = null;
    @Getter
    private HMSMirrorAppService hmsMirrorAppService = null;
    @Getter
    private TableService tableService = null;
    @Getter
    private TransferService transferService = null;

    @Autowired
    public void setHmsMirrorAppService(HMSMirrorAppService hmsMirrorAppService) {
        this.hmsMirrorAppService = hmsMirrorAppService;
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

    @Autowired
    public void setCliReporter(CliReporter cliReporter) {
        this.cliReporter = cliReporter;
    }

    @Autowired
    public void setReportWriterService(ReportWriterService reportWriterService) {
        this.reportWriterService = reportWriterService;
    }

    @Autowired
    public void setHmsMirrorCommandLineOptions(HmsMirrorCommandLineOptions hmsMirrorCommandLineOptions) {
        this.hmsMirrorCommandLineOptions = hmsMirrorCommandLineOptions;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    @Autowired
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }

    @Autowired
    public void setTransferService(TransferService transferService) {
        this.transferService = transferService;
    }

}
