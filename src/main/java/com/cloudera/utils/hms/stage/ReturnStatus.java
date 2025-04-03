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

package com.cloudera.utils.hms.stage;

import com.cloudera.utils.hms.mirror.domain.TableMirror;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
@Getter
public class ReturnStatus {
    private Status status = null;
    private TableMirror tableMirror = null;
    private Throwable exception = null;

    public enum Status {
        SUCCESS,
        /*
         * The next step has been set and the caller should continue with the next step
         */
        NEXTSTEP,
        ERROR,
        INCOMPLETE,
        FATAL,
        SKIP
    }

}
