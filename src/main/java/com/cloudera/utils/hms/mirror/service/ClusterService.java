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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service for managing cluster-related operations.
 * This service coordinates between different services to perform cluster-level operations.
 */
@Service
@Slf4j
@Getter
public class ClusterService {

    private final ExecuteSessionService executeSessionService;
    private final DatabaseService databaseService;

    /**
     * Constructor for ClusterService.
     *
     * @param executeSessionService Service for executing sessions
     * @param databaseService Service for database operations
     */
    public ClusterService(ExecuteSessionService executeSessionService, DatabaseService databaseService) {
        this.executeSessionService = executeSessionService;
        this.databaseService = databaseService;
        log.debug("ClusterService initialized");
    }
}
