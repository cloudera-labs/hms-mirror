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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Properties;

public class HiveServer2Config {
    public static final String APACHE_HIVE_DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";
    private String uri = null;
    private Boolean disconnected = Boolean.FALSE;
    private Properties connectionProperties;
    private String driverClassName = APACHE_HIVE_DRIVER_CLASS_NAME; // default driver.
    private String jarFile = null;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public Boolean isDisconnected() {
        return disconnected;
    }

    public void setDisconnected(Boolean disconnected) {
        this.disconnected = disconnected;
    }

    public Properties getConnectionProperties() {
        if (connectionProperties == null) {
            setConnectionProperties(new Properties());
        }
        return connectionProperties;
    }

    public void setConnectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getJarFile() {
        return jarFile;
    }

    public void setJarFile(String jarFile) {
        this.jarFile = jarFile;
    }

    @JsonIgnore
    public Boolean isValidUri() {
        Boolean rtn = Boolean.TRUE;
        if (getUri() == null || !getUri().startsWith("jdbc:hive2://")) {
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    @JsonIgnore
    public Boolean isKerberosConnection() {
        if (getUri().contains("principal")) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    @JsonIgnore
    public Boolean isZooKeeperConnection() {
        if (getUri().contains("serviceDiscoveryMode=zooKeeper")) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }
}
