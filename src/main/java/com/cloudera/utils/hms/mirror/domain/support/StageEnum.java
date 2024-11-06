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

package com.cloudera.utils.hms.mirror.domain.support;

public enum StageEnum {

    VALIDATING_CONFIG("Validating Configuration"),
    VALIDATE_CONNECTION_CONFIG("Validating Connection Configuration"),
    CONNECTION("Connecting to Endpoints"),
    GATHERING_DATABASES("Gathering Databases"),
    ENVIRONMENT_VARS("Retrieving Environment Variables"),
    DATABASES("Loading Databases"),
    TABLES("Loading Tables"),
    GLM_BUILD("Building GLM's from Sources"),
    CREATE_DATABASES("Creating Databases"),
    LOAD_TABLE_METADATA("Loading Table Metadata"),
    MIGRATE_TABLES("Migrating Tables"),
    SAVING_REPORTS("Saving Reports");

    private final String stage;

    StageEnum(String stage) {
        this.stage = stage;
    }

    public String getStage() {
        return stage;
    }
}
