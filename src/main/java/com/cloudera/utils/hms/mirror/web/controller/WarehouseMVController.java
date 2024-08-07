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

import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.validation.constraints.NotNull;
import java.sql.SQLException;
import java.util.List;

import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.*;

@Controller
@RequestMapping(path = "/warehouse")
@Slf4j
public class WarehouseMVController {

    private ConfigService configService;
    private ExecuteSessionService executeSessionService;
    private DatabaseService databaseService;
    private ConnectionPoolService connectionPoolService;
    private UIModelService uiModelService;

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
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @RequestMapping(value = "/plan/add", method = RequestMethod.GET)
    public String addWarehousePlan(Model model) throws SQLException, SessionException, EncryptionException {
        if (executeSessionService.transitionLoadedSessionToActive(1)
                && configService.validateForConnections(executeSessionService.getSession())) {
            if (!connectionPoolService.isConnected()) {
                connectionPoolService.init();
            }

            List<String> availableDatabases = databaseService.listAvailableDatabases(Environment.LEFT);
            model.addAttribute(AVAILABLE_DATABASES, availableDatabases);
            uiModelService.sessionToModel(model, 1, Boolean.FALSE);

            return "warehouse/plan/add";
        } else {
            uiModelService.sessionToModel(model, 1, Boolean.FALSE);
            model.addAttribute(TYPE, "Connections");
            model.addAttribute(MESSAGE, "Issue validating connections.  Review Messages and try again.");
            return "error";
        }
    }

    @RequestMapping(value = "/plan/save", method = RequestMethod.POST)
    public String addDatabase(Model model,
                              @RequestParam(value = DATABASE, required = true) String database,
                              @RequestParam(value = EXTERNAL_DIRECTORY, required = true) String externalDirectory,
                              @RequestParam(value = MANAGED_DIRECTORY, required = true) String managedDirectory
    ) throws RequiredConfigurationException {
        // Don't reload if running.
//        executeSessionService.clearActiveSession();

        log.info("Adding Warehouse Plan: {} E:{} M:{}", database, externalDirectory, managedDirectory);

        databaseService.addWarehousePlan(database, externalDirectory, managedDirectory);

//        model.addAttribute(ACTION, "view");
//        model.addAttribute(READ_ONLY, Boolean.TRUE);
//        sessionToModel(model, Boolean.FALSE);

        return "redirect:/config/view";
    }

    @RequestMapping(value = "/plan/saveAdd", method = RequestMethod.POST)
    public String addDatabaseAgain(Model model,
                              @RequestParam(value = DATABASE, required = true) String database,
                              @RequestParam(value = EXTERNAL_DIRECTORY, required = true) String externalDirectory,
                              @RequestParam(value = MANAGED_DIRECTORY, required = true) String managedDirectory
    ) throws RequiredConfigurationException {
        // Don't reload if running.
//        executeSessionService.clearActiveSession();

        log.info("Adding Warehouse Plan: {} E:{} M:{}", database, externalDirectory, managedDirectory);

        databaseService.addWarehousePlan(database, externalDirectory, managedDirectory);

        List<String> availableDatabases = databaseService.listAvailableDatabases(Environment.LEFT);
        model.addAttribute(AVAILABLE_DATABASES, availableDatabases);

        uiModelService.sessionToModel(model, 1, Boolean.FALSE);

        return "warehouse/plan/add";
//        return "redirect:/config/view";
    }


    @RequestMapping(value = "/plan/{database}/delete", method = RequestMethod.GET)
    public String deleteWarehousePlan(Model model,
                                      @PathVariable @NotNull String database) throws SessionException {
        executeSessionService.clearActiveSession();

        databaseService.removeWarehousePlan(database);

        return "redirect:/config/view";
    }



}
