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

public enum PhaseState {
    INIT, CALCULATING_SQL, ERROR,
    CALCULATED_SQL, APPLYING_SQL, PROCESSED,
    @Deprecated
    SUCCESS, // No longer used, but left in to handle reprocessing of historical runs.
    CALCULATED_SQL_WARNING,
    // This happens on RETRY only when it was previously CALCULATED_SQL.
    RETRY_SKIPPED_PAST_SUCCESS
}
