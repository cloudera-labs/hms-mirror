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

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;

import java.util.Properties;

@Getter
@Setter
public class HiveServer2Config {
    @JsonIgnore
    public static final String APACHE_HIVE_DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";
    private String uri = null;
    private boolean disconnected = Boolean.FALSE;
    private Properties connectionProperties;
    private String driverClassName = APACHE_HIVE_DRIVER_CLASS_NAME; // default driver.
    private String jarFile = null;

    public Properties getConnectionProperties() {
        if (connectionProperties == null) {
            setConnectionProperties(new Properties());
        }
        return connectionProperties;
    }

    @JsonIgnore
    public Boolean isKerberosConnection() {
        if (!isDisconnected()) {
            if (getUri() != null && getUri().contains("principal")) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        } else {
            return Boolean.FALSE;
        }
    }

    @JsonIgnore
    public boolean isValidUri() {
        Boolean rtn = Boolean.TRUE;
        if (!isDisconnected()) {
            if (getUri() == null || !getUri().startsWith("jdbc:hive2://")) {
                rtn = Boolean.FALSE;
            }
        }
        return rtn;
    }

    @JsonIgnore
    public boolean isZooKeeperConnection() {
        if (getUri() != null && getUri().contains("serviceDiscoveryMode=zooKeeper")) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

}
