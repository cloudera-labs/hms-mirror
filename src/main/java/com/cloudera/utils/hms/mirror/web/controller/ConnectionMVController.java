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
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.UIModelService;
import com.cloudera.utils.hms.util.NamespaceUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Controller
@RequestMapping(path = "/connections")
@Slf4j
public class ConnectionMVController {

    private ConfigService configService;
    private ConnectionPoolService connectionPoolService;
    private ExecuteSessionService executeSessionService;
    private UIModelService uiModelService;
    private CliEnvironment cliEnvironment;

    @Autowired
    public void setCliEnvironment(CliEnvironment cliEnvironment) {
        this.cliEnvironment = cliEnvironment;
    }

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @RequestMapping(value = "/validate", method = RequestMethod.GET)
    public String validate(Model model) {
        uiModelService.sessionToModel(model, 1, false);
        return "connections/validate";
    }

    @RequestMapping(value = "/doValidate", method = RequestMethod.POST)
    public String doValidate(Model model) throws SQLException, SessionException,
            URISyntaxException, EncryptionException {

        if (executeSessionService.startSession(1)) {
            try {
                RunStatus runStatus = executeSessionService.getSession().getRunStatus();
                boolean connCheck = connectionPoolService.init();
                if (connCheck) {
                    runStatus.setProgress(ProgressEnum.COMPLETED);
                } else {
                    runStatus.setProgress(ProgressEnum.FAILED);
                    runStatus.addError(MessageCode.CONNECTION_ISSUE);
                }
            } catch (RuntimeException rte) {
                log.error("Error in doValidate", rte);
                executeSessionService.getSession().getRunStatus().setProgress(ProgressEnum.FAILED);
                executeSessionService.getSession().getRunStatus().addError(MessageCode.CONNECTION_ISSUE);
                throw new SessionException(rte.getMessage());
            } finally {
                //executeSessionService.endSession();
            }
        }

        uiModelService.sessionToModel(model, 1, false);

        return "connections/validate";
    }
}
