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

package com.cloudera.utils.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Getter
@Setter
public class Progression {
    @JsonIgnore
    private final Messages errors = new Messages(150);
    @JsonIgnore
    private final Messages warnings = new Messages(150);

    public void addError(MessageCode code) {
        errors.set(code);
    }

    public void addError(MessageCode code, Object... messages) {
        errors.set(code, messages);
    }

    public void addWarning(MessageCode code) {
        warnings.set(code);
    }

    public void addWarning(MessageCode code, Object... message) {
        warnings.set(code, message);
    }

    public String getErrorMessage(MessageCode code) {
        return errors.getMessage(code.getCode());
    }

    public String getWarningMessage(MessageCode code) {
        return warnings.getMessage(code.getCode());
    }

    public Boolean hasErrors() {
        if (errors.getReturnCode() != 0) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public Boolean hasWarnings() {
        if (warnings.getReturnCode() != 0) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

}
