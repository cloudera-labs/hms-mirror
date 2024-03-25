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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.HiveServer2Config;
import com.cloudera.utils.hms.mirror.connections.ConnectionPools;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsDBCP2Impl;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsHikariImpl;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsHybridImpl;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;

@Component
@Getter
@Setter
@Slf4j
public class ConnectionPoolService implements ConnectionPools {

    private ConfigService configService = null;

    private ConnectionPools connectionPools = null;

    @Autowired
    public ConnectionPoolService(ConfigService configService) {
        this.configService = configService;
    }

    @Override
    public void addHiveServer2(Environment environment, HiveServer2Config hiveServer2) {
        getConnectionPools().addHiveServer2(environment, hiveServer2);
    }

    @Override
    public void addMetastoreDirect(Environment environment, DBStore dbStore) {
        getConnectionPools().addMetastoreDirect(environment, dbStore);
    }

    @Override
    public void close() {
        getConnectionPools().close();
    }

    public ConnectionPools getConnectionPools() {
        if (connectionPools == null) {
            try {
                connectionPools = getConnectionPoolsImpl();
            } catch (SQLException e) {
                log.error("Error creating connection pools", e);
                throw new RuntimeException(e);
            }
        }
        return connectionPools;
    }

    private ConnectionPools getConnectionPoolsImpl() throws SQLException {
        ConnectionPools rtn = null;
        switch (getConfigService().getConfig().getConnectionPoolLib()) {
            case DBCP2:
                log.info("Using DBCP2 Connection Pooling Libraries");
                rtn = new ConnectionPoolsDBCP2Impl(getConfigService());
                break;
            case HIKARICP:
                log.info("Using HIKARICP Connection Pooling Libraries");
                rtn = new ConnectionPoolsHikariImpl(getConfigService());
                break;
            case HYBRID:
                log.info("Using HYBRID Connection Pooling Libraries");
                rtn = new ConnectionPoolsHybridImpl(getConfigService());
                break;
        }
        // Initialize the connection pools
        rtn.init();
        return rtn;
    }

    @Override
    public Connection getHS2EnvironmentConnection(Environment environment) throws SQLException {
        Connection conn = getConnectionPools().getHS2EnvironmentConnection(environment);
        return conn;
    }

    @Override
    public Connection getMetastoreDirectEnvironmentConnection(Environment environment) throws SQLException {
        Connection conn = getConnectionPools().getMetastoreDirectEnvironmentConnection(environment);
        return conn;
    }

    @Override
    public void init() throws SQLException {
        // Gets called on first use.
//        getConnectionPools().init();
    }

}
