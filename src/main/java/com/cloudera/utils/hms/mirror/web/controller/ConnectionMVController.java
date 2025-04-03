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

package com.cloudera.utils.hms.mirror.web.controller;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.ProgressEnum;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.net.URISyntaxException;
import java.sql.SQLException;

@Controller
@RequestMapping(path = "/connections")
@Slf4j
public class ConnectionMVController {

    private final ConfigService configService;
    private final ConnectionPoolService connectionPoolService;
    private final ExecuteSessionService executeSessionService;
    private final EnvironmentService environmentService;

    private final UIModelService uiModelService;
    private final CliEnvironment cliEnvironment;

    public ConnectionMVController(ConfigService configService,
                                  ConnectionPoolService connectionPoolService,
                                  ExecuteSessionService executeSessionService,
                                  EnvironmentService environmentService,
                                  UIModelService uiModelService,
                                  CliEnvironment cliEnvironment) {
        this.configService = configService;
        this.connectionPoolService = connectionPoolService;
        this.executeSessionService = executeSessionService;
        this.environmentService = environmentService;
        this.uiModelService = uiModelService;
        this.cliEnvironment = cliEnvironment;
    }

    //    @Autowired
//    public void setCliEnvironment(CliEnvironment cliEnvironment) {
//        this.cliEnvironment = cliEnvironment;
//    }
//
//    @Autowired
//    public void setConfigService(ConfigService configService) {
//        this.configService = configService;
//    }
//
//    @Autowired
//    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
//        this.connectionPoolService = connectionPoolService;
//    }
//
//    @Autowired
//    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
//        this.executeSessionService = executeSessionService;
//    }
//
//    @Autowired
//    public void setUiModelService(UIModelService uiModelService) {
//        this.uiModelService = uiModelService;
//    }

    @RequestMapping(value = "/validate", method = RequestMethod.GET)
    public String validate(Model model) {
        uiModelService.sessionToModel(model, 1, false);
        return "connections/validate";
    }

    @RequestMapping(value = "/doValidate", method = RequestMethod.POST)
    public String doValidate(Model model) throws SQLException, SessionException,
            URISyntaxException, EncryptionException {

        environmentService.setupGSS();
        if (executeSessionService.startSession(1)) {
            try {
                RunStatus runStatus = executeSessionService.getSession().getRunStatus();
                ExecuteSession session = executeSessionService.getSession();
                if (configService.validateForConnections(session)) {

                    boolean connCheck = connectionPoolService.init();
                    if (connCheck) {
                        runStatus.setProgress(ProgressEnum.COMPLETED);
                    } else {
                        runStatus.setProgress(ProgressEnum.FAILED);
                        runStatus.addError(MessageCode.CONNECTION_ISSUE);
                    }
                } else {
                    runStatus.setProgress(ProgressEnum.FAILED);
                    runStatus.addError(MessageCode.CONNECTION_ISSUE);
                }
            } catch (RuntimeException rte) {
                log.error("Error in doValidate", rte);
                executeSessionService.getSession().getRunStatus().setProgress(ProgressEnum.FAILED);
                executeSessionService.getSession().getRunStatus().addError(MessageCode.CONNECTION_ISSUE);
                throw new SessionException(rte.getMessage());
            }
        }

        uiModelService.sessionToModel(model, 1, false);

        return "connections/validate";
    }
}
