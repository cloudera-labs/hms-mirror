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

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Slf4j
public class IcebergConfig implements Cloneable {

    private int version = 2;
    private Map<String, String> tableProperties = new HashMap<String, String>();

    private void addTableProperty(String key, String value) {
        tableProperties.put(key, value);
    }

    @Override
    public IcebergConfig clone() {
        try {
            IcebergConfig clone = (IcebergConfig) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public void setVersion(int version) {
        if (version == 1 || version == 2) {
            this.version = version;
        } else {
            log.error("Invalid Iceberg Version {}", version);
//            throw new RuntimeException("Invalid Iceberg Version: " + version);
        }
    }

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
