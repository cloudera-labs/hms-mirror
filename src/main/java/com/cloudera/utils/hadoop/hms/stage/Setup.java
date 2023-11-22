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

package com.cloudera.utils.hadoop.hms.stage;

import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.mirror.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.GET_ENV_VARS;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.SHOW_DATABASES;

/*
Using the config, go through the databases and tables and collect the current states.

Create the target databases, where needed to support the migration.
 */
public class Setup {
    private static final Logger LOG = LoggerFactory.getLogger(Setup.class);

    private Config config = null;
    private Conversion conversion = null;

    private Config getConfig() {
        if (config == null) {
            config = Context.getInstance().getConfig();
        }
        return config;
    }

    public Setup(Conversion conversion) {
        this.conversion = conversion;
    }


    // TODO: Need to address failures here...
    public Boolean collect() {
        Context.getInstance().setInitializing(Boolean.TRUE);
//        initializing = Boolean.TRUE;
        Boolean rtn = Boolean.TRUE;
        Date startTime = new Date();
        LOG.info("GATHERING METADATA: Start Processing for databases: " + Arrays.toString((getConfig().getDatabases())));

        // Check dbRegEx
        if (getConfig().getFilter().getDbRegEx() != null && !getConfig().isLoadingTestData()) {
            // Look for the dbRegEx.
            Connection conn = null;
            Statement stmt = null;
            List<String> databases = new ArrayList<String>();
            try {
                conn = getConfig().getCluster(Environment.LEFT).getConnection();
                if (conn != null) {
                    LOG.info("Retrieved LEFT Cluster Connection");
                    stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(SHOW_DATABASES);
                    while (rs.next()) {
                        String db = rs.getString(1);
                        Matcher matcher = getConfig().getFilter().getDbFilterPattern().matcher(db);
                        if (matcher.find()) {
                            databases.add(db);
                        }
                    }
                    String[] dbs = databases.toArray(new String[0]);
                    getConfig().setDatabases(dbs);
                }
            } catch (SQLException se) {
                // Issue
                LOG.error("Issue getting databases for dbRegEx");
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        if (!getConfig().isLoadingTestData()) {
            // Look for the dbRegEx.
            Connection conn = null;
            Statement stmt = null;
            LOG.info("Loading Environment Variables");
            try {
                conn = getConfig().getCluster(Environment.LEFT).getConnection();
                if (conn != null) {
                    LOG.info("Retrieving LEFT Cluster Connection");
                    stmt = conn.createStatement();
                    // Load Session Environment Variables.
                    ResultSet rs = stmt.executeQuery(GET_ENV_VARS);
                    while (rs.next()) {
                        String envVarSet = rs.getString(1);
                        getConfig().getCluster(Environment.LEFT).addEnvVar(envVarSet);
                    }
                }
            } catch (SQLException se) {
                // Issue
                LOG.error("Issue getting LEFT database connection");
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            try {
                conn = getConfig().getCluster(Environment.RIGHT).getConnection();
                if (conn != null) {
                    LOG.info("Retrieving RIGHT Cluster Connection");
                    stmt = conn.createStatement();
                    // Load Session Environment Variables.
                    ResultSet rs = stmt.executeQuery(GET_ENV_VARS);
                    while (rs.next()) {
                        String envVarSet = rs.getString(1);
                        getConfig().getCluster(Environment.RIGHT).addEnvVar(envVarSet);
                    }
                }
            } catch (SQLException se) {
                // Issue
                LOG.error("Issue getting RIGHT databases connection");
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        if (getConfig().getDatabases() == null || getConfig().getDatabases().length == 0) {
            throw new RuntimeException("No databases specified OR found if you used dbRegEx");
        }

        List<ScheduledFuture<ReturnStatus>> gtf = new ArrayList<ScheduledFuture<ReturnStatus>>();
        if (!getConfig().isLoadingTestData()) {
            for (String database : getConfig().getDatabases()) {
                DBMirror dbMirror = conversion.addDatabase(database);
                try {
                    // Get the Database definitions for the LEFT and RIGHT clusters.
                    if (getConfig().getCluster(Environment.LEFT).getDatabase(config, dbMirror)) {
                        getConfig().getCluster(Environment.RIGHT).getDatabase(config, dbMirror);
                    } else {
                        // LEFT DB doesn't exists.
                        dbMirror.addIssue(Environment.LEFT, "DB doesn't exist. Check permissions for user running process");
                        return Boolean.FALSE;
                    }
                } catch (SQLException se) {
                    throw new RuntimeException(se);
                }

                // Build out the table in a database.
                if (!getConfig().getDatabaseOnly()) {
                    Callable<ReturnStatus> gt = new GetTables(config, dbMirror);
                    gtf.add(getConfig().getTransferThreadPool().schedule(gt, 1, TimeUnit.MILLISECONDS));
                }
            }

            // Collect Table Information
            while (true) {
                boolean check = true;
                for (Future<ReturnStatus> sf : gtf) {
                    if (!sf.isDone()) {
                        check = false;
                        break;
                    }
                    try {
                        if (sf.isDone() && sf.get() != null) {
                            if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                                rtn = Boolean.FALSE;
//                            throw new RuntimeException(sf.get().getException());
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (check)
                    break;
            }
            gtf.clear(); // reset

            // Failure, report and exit with FALSE
            if (!rtn) {
                getConfig().getErrors().set(COLLECTING_TABLES.getCode());
                return Boolean.FALSE;
            }
        }

        // Need to filter out tables that don't match our criteria.




        // Create the databases we'll need on the LEFT and RIGHT
        Callable<ReturnStatus> createDatabases = new CreateDatabases(conversion);
        gtf.add(getConfig().getTransferThreadPool().schedule(createDatabases, 1, TimeUnit.MILLISECONDS));

        // Check and Build DB's First.
        while (true) {
            boolean check = true;
            for (Future<ReturnStatus> sf : gtf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
                try {
                    if (sf.isDone() && sf.get() != null) {
                        ReturnStatus returnStatus = sf.get();
                        if (returnStatus != null && returnStatus.getStatus() == ReturnStatus.Status.ERROR) {
//                            throw new RuntimeException(sf.get().getException());
                            rtn = Boolean.FALSE;
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            if (check)
                break;
        }
        gtf.clear(); // reset

        // Failure, report and exit with FALSE
        if (!rtn) {
            getConfig().getErrors().set(DATABASE_CREATION.getCode());
            return Boolean.FALSE;
        }

        // Shortcut.  Only DB's.
        if (!getConfig().getDatabaseOnly()
//                && !getConfig().isLoadingTestData()
        ) {

            // Get the table METADATA for the tables collected in the databases.
            LOG.info(">>>>>>>>>>> Getting Table Metadata");
            Set<String> collectedDbs = conversion.getDatabases().keySet();
            for (String database : collectedDbs) {
                DBMirror dbMirror = conversion.getDatabase(database);
                Set<String> tables = dbMirror.getTableMirrors().keySet();
                for (String table : tables) {
                    TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                    GetTableMetadata tmd = new GetTableMetadata(dbMirror, tblMirror);
                    gtf.add(getConfig().getMetadataThreadPool().schedule(tmd, 1, TimeUnit.MILLISECONDS));
                }
            }

            while (true) {
                boolean check = true;
                for (Future<ReturnStatus> sf : gtf) {
                    if (!sf.isDone()) {
                        check = false;
                        break;
                    }
                    try {
                        if (sf.isDone() & sf.get() != null) {
                            if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                                throw new RuntimeException(sf.get().getException());
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (check)
                    break;
            }
            gtf.clear(); // reset

            if (!rtn) {
                getConfig().getErrors().set(COLLECTING_TABLE_DEFINITIONS.getCode());
            }

            LOG.info("==============================");
            LOG.info(conversion.toString());
            LOG.info("==============================");
            Date endTime = new Date();
            DecimalFormat df = new DecimalFormat("#.###");
            df.setRoundingMode(RoundingMode.CEILING);
            LOG.info("GATHERING METADATA: Completed in " + df.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");
        }
        Context.getInstance().setInitializing(Boolean.FALSE);
        return rtn;
    }

}
