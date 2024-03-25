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

package com.cloudera.utils.hms.mirror.config;

import com.cloudera.utils.hms.CommandLineOptions;
import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.cli.CliReportWriter;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.stage.ReturnStatus;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;

/*
Using the config, go through the databases and tables and collect the current states.

Create the target databases, where needed to support the migration.
 */
@Configuration
@Slf4j
@Getter
@Setter
public class ApplicationConfig {
//    private static final Logger log = LoggerFactory.getLogger(Setup.class);

    @Getter
    private CliReportWriter cliReportWriter = null;
    @Getter
    private CommandLineOptions commandLineOptions = null;
    @Getter
    private ConfigService configService = null;
    @Getter
    private Conversion conversion = null;
    @Getter
    private ConnectionPoolService connectionPoolService = null;
    @Getter
    private DatabaseService databaseService = null;
    @Getter
    private Progression progression = null;
    @Getter
    private TableService tableService = null;
    @Getter
    private TransferService transferService = null;

    // TODO: Need to address failures here...
    @Bean
    @Order(1000) // Needs to be the last thing to run.
    public CommandLineRunner start() {
        return args -> {
//        context.setInitializing(Boolean.TRUE);
//        initializing = Boolean.TRUE;

            log.info("Starting Application Workflow");
            Boolean rtn = Boolean.TRUE;
            Config config = getConfigService().getConfig();
            if (!config.isValidated()) {
                log.error("Configuration is not valid.  Exiting.");
                return;
            }
            log.info("Setting 'running' to TRUE");
            getConfigService().getRunning().set(Boolean.TRUE);

            Date startTime = new Date();
            log.info("GATHERING METADATA: Start Processing for databases: " + Arrays.toString((config.getDatabases())));

            // Check dbRegEx
            if (config.getFilter().getDbRegEx() != null && !config.isLoadingTestData()) {
                // Look for the dbRegEx.
                Connection conn = null;
                Statement stmt = null;
                List<String> databases = new ArrayList<String>();
                try {
                    conn = getConnectionPoolService().getHS2EnvironmentConnection(Environment.LEFT);
                    //getConfig().getCluster(Environment.LEFT).getConnection();
                    if (conn != null) {
                        log.info("Retrieved LEFT Cluster Connection");
                        stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(MirrorConf.SHOW_DATABASES);
                        while (rs.next()) {
                            String db = rs.getString(1);
                            Matcher matcher = config.getFilter().getDbFilterPattern().matcher(db);
                            if (matcher.find()) {
                                databases.add(db);
                            }
                        }
                        String[] dbs = databases.toArray(new String[0]);
                        config.setDatabases(dbs);
                    }
                } catch (SQLException se) {
                    // Issue
                    log.error("Issue getting databases for dbRegEx");
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

            if (!config.isLoadingTestData()) {
                // Look for the dbRegEx.
                Connection conn = null;
                Statement stmt = null;
                log.info("Loading Environment Variables");
                try {
                    conn = getConnectionPoolService().getHS2EnvironmentConnection(Environment.LEFT);
                    //getConfig().getCluster(Environment.LEFT).getConnection();
                    if (conn != null) {
                        log.info("Retrieving LEFT Cluster Connection");
                        stmt = conn.createStatement();
                        // Load Session Environment Variables.
                        ResultSet rs = stmt.executeQuery(MirrorConf.GET_ENV_VARS);
                        while (rs.next()) {
                            String envVarSet = rs.getString(1);
                            config.getCluster(Environment.LEFT).addEnvVar(envVarSet);
                        }
                    }
                } catch (SQLException se) {
                    // Issue
                    log.error("Issue getting LEFT database connection");
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
                    conn = getConnectionPoolService().getHS2EnvironmentConnection(Environment.RIGHT);
                    //getConfig().getCluster(Environment.RIGHT).getConnection();
                    if (conn != null) {
                        log.info("Retrieving RIGHT Cluster Connection");
                        stmt = conn.createStatement();
                        // Load Session Environment Variables.
                        ResultSet rs = stmt.executeQuery(MirrorConf.GET_ENV_VARS);
                        while (rs.next()) {
                            String envVarSet = rs.getString(1);
                            config.getCluster(Environment.RIGHT).addEnvVar(envVarSet);
                        }
                    }
                } catch (SQLException se) {
                    // Issue
                    log.error("Issue getting RIGHT databases connection");
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

            if (config.getDatabases() == null || config.getDatabases().length == 0) {
                throw new RuntimeException("No databases specified OR found if you used dbRegEx");
            }

            List<Future<ReturnStatus>> gtf = new ArrayList<Future<ReturnStatus>>();
            // ========================================
            // Get the Database definitions for the LEFT and RIGHT clusters.
            // ========================================
            if (!config.isLoadingTestData()) {
                for (String database : config.getDatabases()) {
                    DBMirror dbMirror = conversion.addDatabase(database);
                    try {
                        // Get the Database definitions for the LEFT and RIGHT clusters.

                        if (getDatabaseService().getDatabase(dbMirror, Environment.LEFT)) { //getConfig().getCluster(Environment.LEFT).getDatabase(config, dbMirror)) {
                            getDatabaseService().getDatabase(dbMirror, Environment.RIGHT);
                            //getConfig().getCluster(Environment.RIGHT).getDatabase(config, dbMirror);
                        } else {
                            // LEFT DB doesn't exists.
                            dbMirror.addIssue(Environment.LEFT, "DB doesn't exist. Check permissions for user running process");
                            rtn = Boolean.FALSE;
                        }
                    } catch (SQLException se) {
                        throw new RuntimeException(se);
                    }

                    // Build out the table in a database.
                    if (!config.isDatabaseOnly()) {
                        Future<ReturnStatus> gt = getTableService().getTables(dbMirror);
                        gtf.add(gt);
//                    Callable<ReturnStatus> gt = new GetTables(config, dbMirror);
//                    gtf.add(getConfig().getTransferThreadPool().schedule(gt, 1, TimeUnit.MILLISECONDS));
                    }
                }

                // Collect Table Information and ensure process is complete before moving on.
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
                    getProgression().getErrors().set(MessageCode.COLLECTING_TABLES);
                    rtn = Boolean.FALSE;
                }
            }

            if (!getDatabaseService().createDatabases()) {
                getProgression().getErrors().set(MessageCode.DATABASE_CREATION);
                rtn = Boolean.FALSE;

            }
            // Create the databases we'll need on the LEFT and RIGHT
//        Callable<ReturnStatus> createDatabases = new CreateDatabases(conversion);
//        gtf.add(getConfig().getTransferThreadPool().schedule(createDatabases, 1, TimeUnit.MILLISECONDS));

            // Check and Build DB's First.
//        while (true) {
//            boolean check = true;
//            for (Future<ReturnStatus> sf : gtf) {
//                if (!sf.isDone()) {
//                    check = false;
//                    break;
//                }
//                try {
//                    if (sf.isDone() && sf.get() != null) {
//                        ReturnStatus returnStatus = sf.get();
//                        if (returnStatus != null && returnStatus.getStatus() == ReturnStatus.Status.ERROR) {
////                            throw new RuntimeException(sf.get().getException());
//                            rtn = Boolean.FALSE;
//                        }
//                    }
//                } catch (InterruptedException | ExecutionException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//            if (check)
//                break;
//        }
//        gtf.clear(); // reset

            // Failure, report and exit with FALSE
//        if (!rtn) {
//            getProgression().getErrors().set(DATABASE_CREATION.getCode());
//            return Boolean.FALSE;
//        }

            // Shortcut.  Only DB's.
            if (!config.isDatabaseOnly()
//                && !getConfig().isLoadingTestData()
            ) {

                // ========================================
                // Get the table METADATA for the tables collected in the databases.
                // ========================================
                log.info(">>>>>>>>>>> Getting Table Metadata");
                Set<String> collectedDbs = conversion.getDatabases().keySet();
                for (String database : collectedDbs) {
                    DBMirror dbMirror = conversion.getDatabase(database);
                    Set<String> tables = dbMirror.getTableMirrors().keySet();
                    for (String table : tables) {
                        TableMirror tableMirror = dbMirror.getTableMirrors().get(table);
                        gtf.add(tableService.getTableMetadata(tableMirror));
//                    GetTableMetadataService tmd = new GetTableMetadataService(dbMirror, tblMirror);
//                    gtf.add(getConfig().getMetadataThreadPool().schedule(tmd, 1, TimeUnit.MILLISECONDS));
                    }
                }

                List<Future<ReturnStatus>> migrationFuture = new ArrayList<Future<ReturnStatus>>();

                // Go through the Futures and check status.
                // When SUCCESSFUL, move on to the next step.
                // ========================================
                // Check that a tables metadata has been retrieved.  When it has (ReturnStatus.Status.SUCCESS),
                // move on to the NEXTSTEP and actual do the transfer.
                // ========================================
                while (true) {
                    boolean check = true;
                    for (Future<ReturnStatus> sf : gtf) {
                        if (!sf.isDone()) {
                            check = false;
                            break;
                        }
                        try {
                            if (sf.isDone() && sf.get() != null){
//                                ReturnStatus sfStatus = sf.get(100, java.util.concurrent.TimeUnit.MILLISECONDS);
//                                if (sfStatus != null) {
                                switch (sf.get().getStatus()) {
                                    case SUCCESS:
                                        // Trigger next step and set status.
                                        // TODO: Next Step
                                        sf.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                        // Launch the next step, which is the transfer.
                                        migrationFuture.add(getTransferService().transfer(sf.get().getTableMirror()));
                                        break;
                                    case ERROR:
                                        rtn = Boolean.FALSE;
                                        throw new RuntimeException(sf.get().getException());
                                    case FATAL:
                                        rtn = Boolean.FALSE;
                                        throw new RuntimeException(sf.get().getException());
                                    case NEXTSTEP:
                                        break;
                                }
//                                } else {
//                                    check = false;
//                                }
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
                    getProgression().getErrors().set(MessageCode.COLLECTING_TABLE_DEFINITIONS);
                }

                // Check the Migration Futures are done.
                while (true) {
                    boolean check = true;
                    for (Future<ReturnStatus> sf : migrationFuture) {
                        if (!sf.isDone()) {
                            check = false;
                            break;
                        }
                        try {
                            if (sf.isDone() && sf.get() != null) {
                                if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                                    rtn = Boolean.FALSE;
//                                    throw new RuntimeException(sf.get().getException());
                                }
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (check)
                        break;
                }

                log.info("Wrapping up the Application Workflow");
                log.info("Setting 'running' to FALSE");
                getConfigService().getRunning().set(Boolean.FALSE);

                // Give the underlying threads a chance to finish.
                Thread.sleep(200);
                getCliReportWriter().writeReport();

                log.info("==============================");
                log.info(conversion.toString());
                log.info("==============================");
                Date endTime = new Date();
                DecimalFormat df = new DecimalFormat("#.###");
                df.setRoundingMode(RoundingMode.CEILING);
//                log.info("GATHERING METADATA: Completed in " + df.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");
            }
        };
    }

    @Autowired
    public void setCliReportWriter(CliReportWriter cliReportWriter) {
        this.cliReportWriter = cliReportWriter;
    }

    @Autowired
    public void setCommandLineOptions(CommandLineOptions commandLineOptions) {
        this.commandLineOptions = commandLineOptions;
    }

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    @Autowired
    public void setConversion(Conversion conversion) {
        this.conversion = conversion;
    }

    @Autowired
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Autowired
    public void setProgression(Progression progression) {
        this.progression = progression;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }

    @Autowired
    public void setTransferService(TransferService transferService) {
        this.transferService = transferService;
    }

}
