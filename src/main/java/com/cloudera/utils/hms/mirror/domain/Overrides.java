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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.TreeMap;

@Slf4j
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class Overrides {
    private Map<String, Map<SideType, String>> properties = new TreeMap<String, Map<SideType, String>>();

    public void addProperty(String key, String value, SideType side) {
        if (!properties.containsKey(key)) {
            properties.put(key, new TreeMap<SideType, String>());
        }
        properties.get(key).put(side, value);
    }

    @JsonIgnore
    public Map<String, String> getLeft() {
        Map<String, String> left = new TreeMap<String, String>();
        for (Map.Entry<String, Map<SideType, String>> entry : properties.entrySet()) {
            if (entry.getValue().containsKey(SideType.LEFT)) {
                left.put(entry.getKey(), entry.getValue().get(SideType.LEFT));
            }
            if (entry.getValue().containsKey(SideType.BOTH)) {
                left.put(entry.getKey(), entry.getValue().get(SideType.BOTH));
            }
        }
        return left;
    }

    @JsonIgnore
    public Map<String, String> getRight() {
        Map<String, String> right = new TreeMap<String, String>();
        for (Map.Entry<String, Map<SideType, String>> entry : properties.entrySet()) {
            if (entry.getValue().containsKey(SideType.RIGHT)) {
                right.put(entry.getKey(), entry.getValue().get(SideType.RIGHT));
            }
            if (entry.getValue().containsKey(SideType.BOTH)) {
                right.put(entry.getKey(), entry.getValue().get(SideType.BOTH));
            }
        }
        return right;
    }

    public void setPropertyOverridesStr(String[] inPropsStr, SideType side) {
        if (inPropsStr != null) {
            for (String property : inPropsStr) {
                try {
                    String[] keyValue = property.split("=");
                    if (keyValue.length == 2) {
                        switch (side) {
                            case BOTH:
                                getLeft().put(keyValue[0], keyValue[1]);
                                getRight().put(keyValue[0], keyValue[1]);
                                break;
                            case LEFT:
                                getLeft().put(keyValue[0], keyValue[1]);
                                break;
                            case RIGHT:
                                getRight().put(keyValue[0], keyValue[1]);
                                break;
                        }
                    }
                } catch (Throwable t) {
                    log.error("Error setting property overrides: {}", t.getMessage());
                }
            }
        }
    }

}
