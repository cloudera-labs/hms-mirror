# Default Configuration Template

Use this as a template for the `default.yaml` configuration file used by the `cli` interface.


```yaml
# Copyright 2024 Cloudera, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

transfer:
  # Optional (default: 4)
  concurrency:         10
  # Optional (default: 'transfer_')
  transferPrefix:      "hms_mirror_transfer_"
  # This directory is appended to the 'clusters:...:hcfsNamespace' value to store the transfer package for hive export/import.
  # Optional (default: '/apps/hive/warehouse/export_')
  exportBaseDirPrefix: "/apps/hive/warehouse/export_"
clusters:
  LEFT:
    # Set for Hive 1/2 environments
    legacyHive:    true
    # Is the 'Hadoop COMPATIBLE File System' used to prefix data locations for this cluster.
    # It is mainly used as the transfer location for metadata (export)
    # If the primary storage for this cluster is 'hdfs' than use 'hdfs://...'
    # If the primary storage for this action is cloud storage, use the
    #    cloud storage prefix. IE: s3a://my_bucket
    hcfsNamespace: "<hdfs://namespace>"
    hiveServer2:
      # URI is the Hive JDBC URL in the form of:
      # jdbc:hive2://<server>:<port>
      # See docs for restrictions
      uri:     "<LEFT-cluster-jdbc-url>"
      connectionProperties:
        user:     "*****"
        password: "*****"
      # Standalone jar file used to connect via JDBC to the LEFT environment Hive Server 2
      # NOTE-1: Hive 3 jars will NOT work against Hive 1.  The protocol isn't compatible.
      # NOTE-2: You can specify a 'colon' separated list of jar files in the jarFile property. We've found this
      #       useful when the JDBC driver requires additional jar files to be present in the classpath to support it.
      #       For example, the CDH 5 Hive JDBC driver requires that the `hadoop-common` jar file to be present in the
      #       classpath. You can specify this as follows: jarFile: "/path/to/hive-jdbc.jar:/path/to/hadoop-common.jar"
      #    The order of the jar files is important. The first jar file in the list MUST have the JDBC driver class file.
      jarFile: "<environment-specific-jdbc-standalone-driver>"
    # Optional.  Required only for (-epl) with DUMP or SCHEMA_ONLY
    # This will require the user to install the jdbc driver for the metastoreDirect in $HOME/.hms-mirror/aux_libs
    metastore_direct:
      uri: "<jdbc_url_to_metastore_db_including_db>"
      type: MYSQL|POSTGRES|ORACLE
      connectionProperties:
        user: "<db_user>"
        password: "<db_password>"
      connectionPool:
        min: 3
        max: 5
  RIGHT:
    legacyHive:    false
    # Is the 'Hadoop COMPATIBLE File System' used to prefix data locations for this cluster.
    # It is mainly used to as a baseline for where "DATA" will be transfered in the
    # STORAGE stage.  The data location in the source location will be move to this
    # base location + the extended path where it existed in the source system.
    # The intent is to keep the data in the same relative location for this new cluster
    # as the old cluster.
    hcfsNamespace: "<hdfs://namespace>"
    hiveServer2:
      # URI is the Hive JDBC URL in the form of:
      # jdbc:hive2://<server>:<port>
      # See docs for restrictions
      uri:     "<RIGHT-cluster-jdbc-url>"
      connectionProperties:
        user:     "*****"
        password: "*****"
      # Standalone jar file used to connect via JDBC to the LEFT environment Hive Server 2
      # NOTE-1: Hive 3 jars will NOT work against Hive 1.  The protocol isn't compatible.
      # NOTE-2: You can specify a 'colon' separated list of jar files in the jarFile property. We've found this
      #       useful when the JDBC driver requires additional jar files to be present in the classpath to support it.
      #       For example, the CDH 5 Hive JDBC driver requires that the `hadoop-common` jar file to be present in the
      #       classpath. You can specify this as follows: jarFile: "/path/to/hive-jdbc.jar:/path/to/hadoop-common.jar"
      #    The order of the jar files is important. The first jar file in the list MUST have the JDBC driver class file.
      jarFile: "<environment-specific-jdbc-standalone-driver>"
    partitionDiscovery:
      # Addition HMS configuration needed for this "discover.partitions"="true"
      auto:     true
      # When a table is created, run MSCK when there are partitions.
      initMSCK: true
```