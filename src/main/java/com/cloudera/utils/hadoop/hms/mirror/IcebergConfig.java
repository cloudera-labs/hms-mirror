/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.mirror;

import net.minidev.json.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class IcebergConfig {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergConfig.class);

    private int version = 2;
    private Map<String, String> tableProperties = new HashMap<String, String>();

    private void addTableProperty(String key, String value) {
        tableProperties.put(key, value);
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
                    LOG.error("Problem setting property override: " + property, t);
                }
            }
        }
    }

    public Map<String, String> getTableProperties() {
        return tableProperties;
    }

    public void setTableProperties(Map<String, String> tableProperties) {
        this.tableProperties = tableProperties;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        if (version == 1 || version == 2) {
            this.version = version;
        } else {
            throw new RuntimeException("Invalid Iceberg Version: " + version);
        }
    }

}
