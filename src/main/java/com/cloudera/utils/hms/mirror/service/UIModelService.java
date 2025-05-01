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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.ui.Model;
import org.springframework.web.servlet.ModelAndView;

import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * UIModelService manages the process of transferring relevant session and configuration data
 * to a web model for UI rendering. This service assists controllers in populating their views
 * with configuration options, session data, run statuses, flags, and other context details required
 * for proper UI functionality, including beta and testing flags, concurrency management, and version info.
 */
@Service
@Getter
@Setter
@Slf4j
public class UIModelService implements ControllerReferences {

    @Value("${hms-mirror.config.beta}")
    private boolean beta = Boolean.FALSE;

    private final ExecuteSessionService executeSessionService;

    /**
     * Constructs UIModelService with the injected ExecuteSessionService.
     *
     * @param executeSessionService service managing session state and config
     */
    public UIModelService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    /**
     * Populates the model map of a ModelAndView with session, configuration, and UI state.
     *
     * @param mv         the target ModelAndView
     * @param concurrency concurrency value for operations
     * @param testing     flag for test mode, enables test-only run status population
     */
    public void sessionToModel(ModelAndView mv, Integer concurrency, Boolean testing) {
        sessionToModel(mv.getModel(), concurrency, testing);
    }

    /**
     * Populates the model map of a Model with session, configuration, and UI state.
     *
     * @param model      the target Model
     * @param concurrency concurrency value for operations
     * @param testing     flag for test mode, enables test-only run status population
     */
    public void sessionToModel(Model model, Integer concurrency, Boolean testing) {
        sessionToModel(model.asMap(), concurrency, testing);
    }

    /**
     * Populates the provided map with UI model values derived from the current session, run statuses,
     * configuration values, and context flags.
     * <ul>
     *  <li>{@code BETA} - UI beta flag</li>
     *  <li>{@code CONCURRENCY} - concurrency level for data operations</li>
     *  <li>{@code RUN_CONTAINER} - encapsulates run state/flags</li>
     *  <li>{@code CONFIG}, {@code CONNECTIONS_STATUS}, {@code PERSIST}, {@code RUN_STATUS}, {@code VERSION}, ...</li>
     * </ul>
     * Also handles default/fallback/test run statuses, application version, and selected DataStrategyEnum.
     *
     * @param map         model or view map to populate
     * @param concurrency concurrency level
     * @param testing     flag indicating if test flags should be set
     */
    public void sessionToModel(Map<String, Object> map, Integer concurrency, Boolean testing) {
        boolean lclTesting = testing != null && testing;

        // Set the Beta Flag
        map.put(BETA, beta);
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

            // Set autoGLM if the Global Location Map is empty
            if (session.getConfig().getTranslator().getOrderedGlobalLocationMap().isEmpty()) {
                runContainer.setAutoGLM(Boolean.TRUE);
            }

            // Set up RunStatus either for testing or use existing session run status
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

        // Add application version
        try {
            map.put(VERSION, Manifests.read("HMS-Mirror-Version"));
        } catch (IllegalArgumentException iae) {
            map.put(VERSION, "Unknown");
        }

        // Default data strategy if session/config is absent
        DataStrategyEnum dataStrategy = session == null
                ? DataStrategyEnum.SCHEMA_ONLY
                : session.getConfig().getDataStrategy();

        // Model is always editable in the UI
        map.put(READ_ONLY, Boolean.FALSE);

        // Populate enums or other reference data for UI
        ModelUtils.allEnumsForMap(dataStrategy, map);
    }
}