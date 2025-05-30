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

import com.cloudera.utils.hms.mirror.domain.support.SideType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.TreeMap;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class Overrides {
    private Map<String, Map<SideType, String>> properties = new TreeMap<String, Map<SideType, String>>();

    public void addProperty(String key, String value, SideType side) {
        // Don't save unless key is present and not blank.
        // The value can be blank/null.
        if (!isBlank(key)) {
            if (!properties.containsKey(key)) {
                properties.put(key, new TreeMap<SideType, String>());
            }
            if (!isBlank(value)) {
                properties.get(key).put(side, value);
            } else {
                properties.get(key).put(side, null);
            }
        }
    }

    public void setPropertyOverridesStr(String[] inPropsStr, SideType side) {
        if (inPropsStr != null) {
            for (String property : inPropsStr) {
                try {
                    String[] keyValue = property.split("=");
                    if (keyValue.length == 2) {
                        switch (side) {
                            case BOTH:
                                addProperty(keyValue[0],keyValue[1],SideType.BOTH);
                                break;
                            case LEFT:
                                addProperty(keyValue[0],keyValue[1],SideType.LEFT);
                                break;
                            case RIGHT:
                                addProperty(keyValue[0],keyValue[1],SideType.RIGHT);
                                break;
                        }
                    } else if (keyValue.length == 1) {
                        addProperty(keyValue[0],null,side);
                    }
                } catch (Throwable t) {
                    log.error("Error setting property overrides: {}", t.getMessage());
                }
            }
        }
    }

}
