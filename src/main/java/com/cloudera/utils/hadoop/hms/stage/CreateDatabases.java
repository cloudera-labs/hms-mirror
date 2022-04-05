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

package com.cloudera.utils.hadoop.hms.stage;

import com.cloudera.utils.hadoop.HadoopSession;
import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.concurrent.Callable;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.*;

public class CreateDatabases implements Callable<ReturnStatus> {
    private static Logger LOG = LogManager.getLogger(CreateDatabases.class);

    private Config config = null;
    private Conversion conversion = null;

    // Flag use to determine is creating transition db in lower cluster
    // or target db in upper cluster.
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public CreateDatabases(Config config, Conversion conversion) {
        this.config = config;
        this.conversion = conversion;
    }

    @Override
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();
        LOG.debug("Create Databases");
        try {
            for (String database : config.getDatabases()) {
                DBMirror dbMirror = conversion.getDatabase(database);

                String[] dbCreate = null;
                dbCreate = dbMirror.dbCreate(config);

                LOG.info("Strategy: " + config.getDataStrategy());
                switch (config.getDataStrategy()) {
                    case HYBRID:
                    case EXPORT_IMPORT:
                        if (!(config.getMigrateACID().isOn() && config.getMigrateACID().isOnly())) {
                            LOG.info("Creating Transfer DB to support EXPORT_IMPORT tables.");
                            config.getCluster(Environment.LEFT).createDatabase(config, config.getTransfer().getTransferPrefix() + database);
                        }
                        config.getCluster(Environment.RIGHT).createDatabase(config, database, dbCreate[0]);
                        break;
                    case SQL:
                        // Transfer DB
                        config.getCluster(Environment.RIGHT).createDatabase(config, config.getTransfer().getTransferPrefix() + database);
                    case SCHEMA_ONLY:
                    case DUMP:
                    case COMMON:
                    case LINKED:
//                    case INTERMEDIATE:
                        // If sync'ing and ro, don't create db if the directory doesn't exist.  This will corrupt the
                        // RO properties of the FileSystem.
                        if (config.isReadOnly()) {
                            LOG.debug("Config set to 'read-only'.  Validating FS before continuing");
                            HadoopSession main = null;
                            try {
                                main = config.getCliPool().borrow();
                                String[] api = {"-api"};
                                try {
                                    main.start(api);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                                CommandReturn cr = main.processInput("connect");
                                // Check that location exists.
                                String dbLocation = null;
                                if (dbCreate[1] != null) {
                                    dbLocation = dbCreate[1];
                                } else {
                                    // Get location for DB. If it's not there than:
                                    //     SQL query to get default from Hive.
                                    String defaultDBLocProp = null;
                                    if (config.getCluster(Environment.RIGHT).getLegacyHive()) {
                                        defaultDBLocProp = MirrorConf.LEGACY_DB_LOCATION_PROP;
                                    } else {
                                        defaultDBLocProp = MirrorConf.EXT_DB_LOCATION_PROP;
                                    }

                                    Connection conn = null;
                                    Statement stmt = null;
                                    ResultSet resultSet = null;
                                    try {
                                        conn = config.getCluster(Environment.RIGHT).getConnection();

                                        stmt = conn.createStatement();

                                        String dbLocSql = "SET " + defaultDBLocProp;
                                        resultSet = stmt.executeQuery(dbLocSql);
                                        if (resultSet.next()) {
                                            String propset = resultSet.getString(1);
                                            String dbLocationPrefix = propset.split("=")[1];
                                            dbLocation = dbLocationPrefix + dbMirror.getName().toLowerCase(Locale.ROOT) + ".db";
                                            LOG.debug(database + " location is: " + dbLocation);

                                        } else {
                                            // Could get property.
                                            throw new RuntimeException("Could not determine DB Location for: " + database);
                                        }

                                    } catch (SQLException throwables) {
                                        LOG.error("Issue", throwables);
                                        throw new RuntimeException(throwables);
                                    } finally {
                                        if (resultSet != null) {
                                            try {
                                                resultSet.close();
                                            } catch (SQLException sqlException) {
                                                // ignore
                                            }
                                        }
                                        if (stmt != null) {
                                            try {
                                                stmt.close();
                                            } catch (SQLException sqlException) {
                                                // ignore
                                            }
                                        }
                                        if (conn != null) {
                                            try {
                                                conn.close();
                                            } catch (SQLException throwables) {
                                                //
                                            }
                                        }
                                    }


                                }
                                if (dbLocation != null) {
                                    CommandReturn testCr = main.processInput("test -d " + dbLocation);
                                    if (testCr.isError()) {
                                        // Doesn't exist.  So we can't create the DB in a "read-only" mode.
                                        config.getErrors().set(RO_DB_DOESNT_EXIST.getCode(), dbLocation,
                                                testCr.getCode(), testCr.getCommand(), dbMirror.getName());
                                        dbMirror.addIssue(config.getErrors().getMessage(RO_DB_DOESNT_EXIST.getCode()));
                                        throw new RuntimeException(config.getErrors().getMessage(RO_DB_DOESNT_EXIST.getCode()));
                                    } else {
                                        config.getCluster(Environment.RIGHT).createDatabase(config, database, dbCreate[0]);
                                    }
                                } else {
                                    // Can't determine location.
                                    // TODO: What to do here.
                                    LOG.error(dbMirror.getName() + ": Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in " +
                                            "'read-only' mode without knowing where it should go and validating existance.");
                                    throw new RuntimeException(dbMirror.getName() + ": Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in " +
                                            "'read-only' mode without knowing where it should go and validating existance.");
                                }
                            } finally {
                                config.getCliPool().returnSession(main);
                            }
                        } else {
                            config.getCluster(Environment.RIGHT).createDatabase(config, database, dbCreate[0]);
                        }
                }

            }
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } catch (Throwable t) {
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(t);
        }
        return rtn;
    }
}
