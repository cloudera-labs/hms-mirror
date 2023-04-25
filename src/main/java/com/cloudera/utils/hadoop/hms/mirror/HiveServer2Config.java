/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Properties;

public class HiveServer2Config {
    private String uri = null;
    private Boolean disconnected = Boolean.FALSE;
    private Properties connectionProperties;
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
        // Don't restrict max connections to the default.  Allow this to
        // be controlled by the 'concurrency' value of the 'transfer'.
        String maxConnections = connectionProperties.getProperty("maxTotal", "-1");
        connectionProperties.setProperty("maxTotal", maxConnections);
        // Default to 5 secs for timeout.
        String maxWait = connectionProperties.getProperty("maxWaitMillis", "5000");
        connectionProperties.setProperty("maxWaitMillis", maxWait);
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
