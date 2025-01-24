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

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.ProgressEnum;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.Future;

@Service
@Getter
@Setter
@Slf4j
public class RuntimeService {

    private ConfigService configService;
    private DatabaseService databaseService;
    private ExecuteSessionService executeSessionService;
    private HMSMirrorAppService hmsMirrorAppService;
    private TranslatorService translatorService;


    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setHmsMirrorAppService(HMSMirrorAppService hmsMirrorAppService) {
        this.hmsMirrorAppService = hmsMirrorAppService;
    }

    @Autowired
    public void setTranslatorService(TranslatorService translatorService) {
        this.translatorService = translatorService;
    }

    public RunStatus start(boolean dryrun, //boolean autoGLM,
                           Integer concurrency) throws RequiredConfigurationException, MismatchException, SessionException, EncryptionException {
        RunStatus runStatus = null;
        ExecuteSession session = executeSessionService.getSession();
        if (executeSessionService.startSession(concurrency)) {

            session = executeSessionService.getSession();
            runStatus = session.getRunStatus();
//            if (configService.validate(session, executeSessionService.getCliEnvironment())) {

                if (runStatus.reset()) {
                    executeSessionService.getSession().getConfig().setExecute(!dryrun);

                    // Start job in a separate thread.
                    Future<Boolean> runningTask = hmsMirrorAppService.run();

                    // Set the running task reference in the RunStatus.
                    runStatus.setRunningTask(runningTask);
                }
//            } else {
//                runStatus.addError(MessageCode.CONFIG_INVALID);
//                runStatus.setProgress(ProgressEnum.FAILED);
//            }
        } else {
            throw new SessionException("Session is already running. " +
                    "You can't start a new session while the current session is running.");
        }
        return runStatus;
    }

}
