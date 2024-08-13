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

package com.cloudera.utils.hms.mirror.domain;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Schema(description = "Acceptance criteria for the migration that ensures the user is aware of the potential risks")
public class Acceptance implements Cloneable {

    private boolean silentOverride = Boolean.FALSE;
    private boolean backedUpHDFS= Boolean.FALSE;
    private boolean backedUpMetastore = Boolean.FALSE;
    private boolean trashConfigured = Boolean.FALSE;
    private boolean potentialDataLoss = Boolean.FALSE;

    @Override
    public Acceptance clone() {
        try {
            Acceptance clone = (Acceptance) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
