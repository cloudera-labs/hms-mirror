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

package com.cloudera.utils.hms.mirror.connections;

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionPools {

    String HIKARI_CONNECTION_TIMEOUT = "hikari.connectionTimeout";
    String HIKARI_CONNECTION_TIMEOUT_DEFAULT = "60000";
    String HIKARI_VALIDATION_TIMEOUT = "hikari.validationTimeout";
    String HIKARI_VALIDATION_TIMEOUT_DEFAULT = "30000";
    String HIKARI_INITIALIZATION_FAIL_TIMEOUT = "hikari.initializationFailTimeout";
    String HIKARI_INITIALIZATION_FAIL_TIMEOUT_DEFAULT = "10000";

    void addHiveServer2(Environment environment, HiveServer2Config hiveServer2);

    void addMetastoreDirect(Environment environment, DBStore dbStore);

    void close();

    Connection getHS2EnvironmentConnection(Environment environment) throws SQLException;

    Connection getMetastoreDirectEnvironmentConnection(Environment environment) throws SQLException;

    void init() throws SQLException, SessionException, EncryptionException, URISyntaxException;

}
