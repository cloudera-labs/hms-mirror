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

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.TreeMap;

@Slf4j
@Getter
@Setter
public class Overrides {
    private Map<String, String> left = null;

    private Map<String, String> right = null;

    public Map<String, String> getLeft() {
        if (left == null)
            left = new TreeMap<String, String>();
        return left;
    }

    public Map<String, String> getRight() {
        if (right == null)
            right = new TreeMap<String, String>();
        return right;
    }

    public void setPropertyOverridesStr(String[] inPropsStr, Side side) {
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

    public enum Side {BOTH, LEFT, RIGHT}

}
