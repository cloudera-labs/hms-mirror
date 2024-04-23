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

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.connections.ConnectionPools;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Getter
@Setter
public class Cluster implements Comparable<Cluster> {

    @JsonIgnore
    private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @JsonIgnore
    private HmsMirrorConfig hmsMirrorConfig;

    @JsonIgnore
    private ConnectionPools pools = null;

    @JsonIgnore
    private boolean initialized = Boolean.FALSE;
    @JsonIgnore
    private Map<String, String> envVars = new HashMap<>();

    private Environment environment = null;
    private boolean legacyHive = Boolean.TRUE;
    private boolean createIfNotExists = Boolean.FALSE;

    /*
    HDP Hive 3 aconfignd Manage table creation and location methods weren't mature and had a lot of
    bugs/incomplete features.

    Hive 3 Databases have an MANAGEDLOCATION attribute used to override the 'warehouse' location
    specified in the hive metastore as the basis for the root directory of ACID tables in Hive.
    Unfortunately, this setting isn't available in HDP Hive 3.  It was added later in CDP Hive 3.

    In lew of this, when this flag is set to true (default is false), we will NOT strip the location
    element from the tables CREATE for ACID tables.  This is the only method we have to control the
    tables' location.  THe DATABASE doesn't yet support the MANAGEDLOCATION attribute, so we will not
    build/run the ALTER DATABASE ...  SET MANAGEDLOCATION ... for this configuration.

    We will add a DB properties that can be used later on to set the DATABASE's MANAGEDLOCATION property
    after you've upgraded to CDP Hive 3.
     */
    private boolean hdpHive3 = Boolean.FALSE;
    private String hcfsNamespace = null;
    private HiveServer2Config hiveServer2 = null;
    @JsonProperty(value = "metastore_direct")
    private DBStore metastoreDirect = null;
    private PartitionDiscovery partitionDiscovery = new PartitionDiscovery();
    private boolean enableAutoTableStats = Boolean.FALSE;
    private boolean enableAutoColumnStats = Boolean.FALSE;

    public Cluster() {
    }

    public void addEnvVar(String varSet) {
        String[] var = varSet.split("=");
        String key;
        if (var.length > 0) {
            key = var[0];
        } else {
            key = "";
        }
        String value;
        if (var.length > 1) {
            value = var[1];
        } else {
            value = "";
        }
        log.debug("{}: Adding Environment Variable: {}={}", getEnvironment(), key, value);
        this.envVars.put(key, value);
    }

    @Override
    public int compareTo(Cluster o) {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Cluster cluster = (Cluster) o;

        if (legacyHive != (cluster.legacyHive)) return false;
        if (!hcfsNamespace.equals(cluster.hcfsNamespace)) return false;
        return hiveServer2.equals(cluster.hiveServer2);
    }

    @JsonIgnore
    public Connection getConnection() throws SQLException {
        Connection conn = null;
        if (pools != null) {
            try {
                conn = pools.getHS2EnvironmentConnection(getEnvironment());
            } catch (RuntimeException rte) {
                getHmsMirrorConfig().addError(MessageCode.CONNECTION_ISSUE);
                throw rte;
            }
        }
        return conn;
    }

    @JsonIgnore
    public Connection getMetastoreDirectConnection() throws SQLException {
        Connection conn = null;
        if (pools != null) {
            try {
                conn = pools.getMetastoreDirectEnvironmentConnection(getEnvironment());
            } catch (RuntimeException rte) {
                getHmsMirrorConfig().addError(MessageCode.CONNECTION_ISSUE);
                throw rte;
            }
        }
        return conn;
    }

    @Override
    public int hashCode() {
        int result = legacyHive ? 1 : 0;
        result = 31 * result + hcfsNamespace.hashCode();
        result = 31 * result + hiveServer2.hashCode();
        return result;
    }

    public Boolean isHdpHive3() {
        return hdpHive3;
    }

    public Boolean isInitialized() {
        if (hiveServer2 != null && !hiveServer2.isDisconnected()) {
            return initialized;
        } else {
            return Boolean.FALSE;
        }
    }

    public void setHiveServer2(HiveServer2Config hiveServer2) {
        this.hiveServer2 = hiveServer2;
        this.initialized = Boolean.TRUE;
    }

    public void setInitialized(Boolean initialized) {
        this.initialized = initialized;
    }
}
