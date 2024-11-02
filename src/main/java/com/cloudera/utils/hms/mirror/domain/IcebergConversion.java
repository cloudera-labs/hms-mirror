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

package com.cloudera.utils.hms.mirror.domain;

import com.cloudera.utils.hms.mirror.domain.support.IcebergFileTypeTranslationEnum;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Slf4j
@Schema(description = "Conversion is where you define how tables might be converted from one type to another.  This works " +
        "for strategies that process data through the SQL engine.  It can also be applied to the SCHEMA_ONLY strategy, as " +
        "long as the user does NOT select 'distcp' as the data movement strategy.")
public class IcebergConversion implements Cloneable {
    private boolean enable = Boolean.FALSE;
    private IcebergFileTypeTranslationEnum fileTypeTranslation = IcebergFileTypeTranslationEnum.STANDARD;
    private int version = 2;
    private Map<String, String> tableProperties = new HashMap<String, String>();

    @JsonIgnore
    public void setPropertyOverridesStr(String[] overrides) {
        if (overrides != null) {
            for (String property : overrides) {
                try {
                    String[] keyValue = property.split("=");
                    if (keyValue.length == 2) {
                        getTableProperties().put(keyValue[0], keyValue[1]);
                    }
                } catch (Throwable t) {
                    log.error("Problem setting property override: {}", property, t);
                }
            }
        }
    }

}
