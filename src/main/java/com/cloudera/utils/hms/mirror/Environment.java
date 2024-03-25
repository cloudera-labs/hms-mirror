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

package com.cloudera.utils.hms.mirror;

public enum Environment {
    /*
    Table lives on RIGHT cluster and usually points to LEFT data.  Used to migrate
    data from the LEFT to the RIGHT.  Temp table and should be deleted after action.
    Should not own data.
     */
    SHADOW(Boolean.FALSE),
    /*
    Table lives on LEFT cluster and will own the data.  This is usually where we'd move
    ACID table "data" to (EXTERNAL table) so we can attach to it via "SHADOW" table on RIGHT
    cluster and finish migration to final RIGHT table.
     */
    TRANSFER(Boolean.FALSE),
    /*
    Abstractions from LEFT and RIGHT for use internally.
     */
    LEFT(Boolean.TRUE),
    RIGHT(Boolean.TRUE);

    private final Boolean visible;

    Environment(Boolean visible) {
        this.visible = visible;
    }

    public boolean isVisible() {
        return visible;
    }
}
