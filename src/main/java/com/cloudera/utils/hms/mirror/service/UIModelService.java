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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.util.ModelUtils;
import com.cloudera.utils.hms.mirror.web.controller.ControllerReferences;
import com.jcabi.manifests.Manifests;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.ui.Model;
import org.springframework.web.servlet.ModelAndView;

import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Service
@Getter
@Setter
@Slf4j
public class UIModelService implements ControllerReferences {

    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    public void sessionToModel(ModelAndView mv, Integer concurrency, Boolean testing) {
        sessionToModel(mv.getModel(), concurrency, testing);
    }

    public void sessionToModel(Model model, Integer concurrency, Boolean testing) {
        sessionToModel(model.asMap(), concurrency, testing);
    }

    public void sessionToModel(Map<String, Object> map, Integer concurrency, Boolean testing) {
        boolean lclTesting = testing != null && testing;

        map.put(CONCURRENCY, concurrency);

        ExecuteSession session = executeSessionService.getSession();

        RunContainer runContainer = new RunContainer();
        map.put(RUN_CONTAINER, runContainer);

        if (nonNull(session)) {
            runContainer.setSessionId(session.getSessionId());
            map.put(CONFIG, session.getConfig());
            map.put(CONNECTIONS_STATUS, session.getConnections());

            PersistContainer persistContainer = new PersistContainer();
            persistContainer.setSaveAs(session.getSessionId());
            map.put(PERSIST, persistContainer);

            // Set the flag to autoGLM if the GLM is empty. Which means they need to be built.
            if (session.getConfig().getTranslator().getOrderedGlobalLocationMap().isEmpty()) {
                runContainer.setAutoGLM(Boolean.TRUE);
            }

            // For testing only.
            if (lclTesting && isNull(session.getRunStatus())) {
                RunStatus runStatus = new RunStatus();
                runStatus.setConcurrency(concurrency);
                runStatus.addError(MessageCode.ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS);
                runStatus.addWarning(MessageCode.RESET_TO_DEFAULT_LOCATION);
                map.put(RUN_STATUS, runStatus);
            } else {
                RunStatus runStatus = session.getRunStatus();
                if (nonNull(runStatus)) {
                    runStatus.setConcurrency(concurrency);
                    map.put(RUN_STATUS, runStatus);
                }
            }
        }

        try {
            map.put(VERSION, Manifests.read("HMS-Mirror-Version"));
        } catch (IllegalArgumentException iae) {
            map.put(VERSION, "Unknown");
        }

        DataStrategyEnum dataStrategy = session == null ? DataStrategyEnum.SCHEMA_ONLY : session.getConfig().getDataStrategy();

        // Always editable.
        map.put(READ_ONLY, Boolean.FALSE);

        ModelUtils.allEnumsForMap(dataStrategy, map);
    }

}
