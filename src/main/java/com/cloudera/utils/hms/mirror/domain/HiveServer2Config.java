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
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.util.Properties;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Getter
@Setter
public class HiveServer2Config implements Cloneable {
    @JsonIgnore
    public static final String APACHE_HIVE_DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";
    @Schema(description = "The JDBC URI for the HiveServer2 connections. EG: jdbc:hive2://<host>:<port>")
    private String uri = null;
    private boolean disconnected = Boolean.FALSE;
    @Schema(description = "The connections properties for the HiveServer2 connections. EG: user, password, etc.")
    private Properties connectionProperties = new Properties();
    @Schema(description = "The driver class name for the HiveServer2 connections. Default is org.apache.hive.jdbc.HiveDriver. Specify this if you are using a different driver.")
    private String driverClassName = APACHE_HIVE_DRIVER_CLASS_NAME; // default driver.
    @Schema(description = "The path to the jar file for the HiveServer2 connections. Must be specified for non-kerberos connections.  Should be null for kerberos connections and driver should be in 'aux_libs'.")
    private String jarFile = null;
    private String version = null;

    @Override
    public HiveServer2Config clone() {
        try {
            HiveServer2Config clone = (HiveServer2Config) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public Properties getConnectionProperties() {
        if (isNull(connectionProperties)) {
            setConnectionProperties(new Properties());
        }
        return connectionProperties;
    }

    @JsonIgnore
    public boolean isKerberosConnection() {
        if (!isDisconnected()) {
            if (!isBlank(getUri()) && getUri().contains("principal")) {
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
            if (isBlank(getUri()) || !getUri().startsWith("jdbc:hive2://")) {
                rtn = Boolean.FALSE;
            }
        }
        return rtn;
    }

    @JsonIgnore
    public boolean isZooKeeperConnection() {
        if (!isBlank(getUri()) && getUri().contains("serviceDiscoveryMode=zooKeeper")) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

}
