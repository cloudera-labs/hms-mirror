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

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.WarehouseSource;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import static com.cloudera.utils.hms.mirror.SessionVars.EXT_DB_LOCATION_PROP;
import static com.cloudera.utils.hms.mirror.SessionVars.MNGD_DB_LOCATION_PROP;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class Cluster implements Comparable<Cluster>, Cloneable {

    @JsonIgnore
    private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

//    @JsonIgnore
//    private HmsMirrorConfig hmsMirrorConfig;

//    @JsonIgnore
//    private ConnectionPools pools = null;

    @JsonIgnore
    private boolean initialized = Boolean.FALSE;
    @JsonIgnore
    private Map<String, String> envVars = new HashMap<>();

    private Environment environment = null;

    @Schema(description = "Treat the target Hive environment as Legacy. This means it's Hive 1/2.  Hive 3 is NOT legacy.  This is used to determine how to create tables.")
    private boolean legacyHive = Boolean.TRUE;
    @Schema(description = "Use IF NOT EXIST syntax in CREATE statements for Database and Table.")
    private boolean createIfNotExists = Boolean.FALSE;

    /*
    HDP Hive 3 aconfignd Manage table creation and location methods weren't mature and had a lot of
    bugs/incomplete features.

    Hive 3 Databases have an MANAGEDLOCATION attribute used to override the 'warehouse' location
    specified in the hive metastoreDirect as the basis for the root directory of ACID tables in Hive.
    Unfortunately, this setting isn't available in HDP Hive 3.  It was added later in CDP Hive 3.

    In lew of this, when this flag is set to true (default is false), we will NOT strip the location
    element from the tables CREATE for ACID tables.  This is the only method we have to control the
    tables' location.  THe DATABASE doesn't yet support the MANAGEDLOCATION attribute, so we will not
    build/run the ALTER DATABASE ...  SET MANAGEDLOCATION ... for this configuration.

    We will add a DB properties that can be used later on to set the DATABASE's MANAGEDLOCATION property
    after you've upgraded to CDP Hive 3.
     */
    @Schema(description = "HDP Hive 3 and Manage table creation and location methods weren't mature and had a lot of bugs/incomplete features. Set this to 'true' when using HDP Hive 3.")
    private boolean hdpHive3 = Boolean.FALSE;
    @Schema(description = "The namespace for the HCFS system.  Only used for Link check.  Namespace information is pulled from database, table, and partition information.  The target namespace is defined elsewhere and is the namespace used for translations.")
//    @Deprecated
    private String hcfsNamespace = null;
    @Schema(description = "The HiveServer2 configuration for the cluster.")
    private HiveServer2Config hiveServer2 = null;
    @JsonProperty(value = "metastore_direct")
    @JsonAlias("metastore_direct")
    @Schema(description = "The direct connections to the Hive Metastore. Optimization used for extracting table and partition details that aren't efficient through the JDBC interface")
    private DBStore metastoreDirect = null;
    @Schema(description = "Methods used to find/discover partitions during initialization and on-going.")
    private PartitionDiscovery partitionDiscovery = new PartitionDiscovery();
    private boolean enableAutoTableStats = Boolean.FALSE;
    private boolean enableAutoColumnStats = Boolean.FALSE;

    @JsonIgnore
    private Warehouse environmentWarehouse = null;

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

    protected void buildEnvironmentWarehouse() {
        String extDir = this.getEnvVars().get(EXT_DB_LOCATION_PROP);
        String manDir = this.getEnvVars().get(MNGD_DB_LOCATION_PROP);
        if (!isBlank(extDir) && !isBlank(manDir)) {
            environmentWarehouse = new Warehouse(WarehouseSource.ENVIRONMENT, extDir, manDir);
        }
    }

    public Warehouse getEnvironmentWarehouse() {
        if (isNull(environmentWarehouse)) {
            buildEnvironmentWarehouse();
        }
        return environmentWarehouse;
    }

    @Override
    public Cluster clone() {
        try {
            Cluster clone = (Cluster) super.clone();
            if (nonNull(hiveServer2)) {
                HiveServer2Config hiveServer2Clone = hiveServer2.clone();
                clone.setHiveServer2(hiveServer2Clone);
            }
            if (nonNull(metastoreDirect)) {
                DBStore metastoreDirectClone = metastoreDirect.clone();
                clone.setMetastoreDirect(metastoreDirectClone);
            }
            if (nonNull(partitionDiscovery)) {
                PartitionDiscovery partitionDiscoveryClone = partitionDiscovery.clone();
                clone.setPartitionDiscovery(partitionDiscoveryClone);
            }
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    @Override
    public int compareTo(Cluster o) {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (isNull(o) || getClass() != o.getClass()) return false;

        Cluster cluster = (Cluster) o;

        if (legacyHive != (cluster.legacyHive)) return false;
//        if (!hcfsNamespace.equals(cluster.hcfsNamespace)) return false;
        return hiveServer2.equals(cluster.hiveServer2);
    }

//    @JsonIgnore
//    public Connection getConnection() throws SQLException {
//        Connection conn = null;
//        if (pools != null) {
//            try {
//                conn = pools.getHS2EnvironmentConnection(getEnvironment());
//            } catch (RuntimeException rte) {
//                getHmsMirrorConfig().addError(MessageCode.CONNECTION_ISSUE);
//                throw rte;
//            }
//        }
//        return conn;
//    }

//    @JsonIgnore
//    public Connection getMetastoreDirectConnection() throws SQLException {
//        Connection conn = null;
//        if (pools != null) {
//            try {
//                conn = pools.getMetastoreDirectEnvironmentConnection(getEnvironment());
//            } catch (RuntimeException rte) {
//                getHmsMirrorConfig().addError(MessageCode.CONNECTION_ISSUE);
//                throw rte;
//            }
//        }
//        return conn;
//    }

    @Override
    public int hashCode() {
        int result = legacyHive ? 1 : 0;
//        result = 31 * result + hcfsNamespace.hashCode();
        result = 31 * result + hiveServer2.hashCode();
        return result;
    }

    public boolean isHdpHive3() {
        return hdpHive3;
    }

    public boolean isInitialized() {
        if (nonNull(hiveServer2) && !hiveServer2.isDisconnected()) {
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
