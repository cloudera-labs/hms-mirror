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

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.DatabaseService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.validation.constraints.NotNull;

@Controller
@RequestMapping(path = "/database")
@Slf4j
public class DatabaseMVController implements ControllerReferences {

    private final ConfigService configService;
    private final DatabaseService databaseService;
    private final ExecuteSessionService executeSessionService;

    public DatabaseMVController(ConfigService configService, DatabaseService databaseService,
                                ExecuteSessionService executeSessionService) {
        this.configService = configService;
        this.databaseService = databaseService;
        this.executeSessionService = executeSessionService;
    }

    @RequestMapping(value = "/add", method = RequestMethod.POST)
    public String addDatabase(Model model,
                              @RequestParam(value = DATABASES, required = true) String databases) throws SessionException {
        executeSessionService.closeSession();
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        String[] dbs = databases.split(",");
        for (String db : dbs) {
            config.getDatabases().add(db);
        }
        configService.validate(session, null);
        return "redirect:/config/edit";
    }

    @RequestMapping(value = "/{database}/delete", method = RequestMethod.GET)
    public String deleteDatabase(Model model,
                                 @PathVariable @NotNull String database) throws SessionException {
        executeSessionService.closeSession();
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        config.getDatabases().remove(database);
        return "redirect:/config/edit";
    }

    @RequestMapping(value = "/property/add", method = RequestMethod.POST)
    public String addDatabaseSkipProperty(Model model,
                                          @RequestParam(value = DBPROPERTIES, required = true) String properties) throws SessionException {
        executeSessionService.closeSession();
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        String[] props = properties.split(",");
        for (String property : props) {
            config.getFilter().addDbPropertySkipItem(property);
        }
        configService.validate(session, null);
        return "redirect:/config/edit";
    }

    @RequestMapping(value = "/property/{index}/delete", method = RequestMethod.GET)
    public String deleteDatabaseSkipProperty(Model model,
                                             @PathVariable @NotNull Integer index) throws SessionException {
        executeSessionService.closeSession();
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        config.getFilter().removeDbPropertySkipItemByIndex(index);
        return "redirect:/config/edit";
    }
}