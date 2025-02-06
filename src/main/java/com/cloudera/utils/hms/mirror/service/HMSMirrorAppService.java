/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.stage.ReturnStatus;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static java.lang.Thread.sleep;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Getter
@Slf4j
public class HMSMirrorAppService {

    private final ConfigService configService;
    private final ConnectionPoolService connectionPoolService;
    private final DatabaseService databaseService;
    private final EnvironmentService environmentService;
    private final ExecuteSessionService executeSessionService;
    private final ReportWriterService reportWriterService;
    private final TableService tableService;
    private final TranslatorService translatorService;
    private final TransferService transferService;

    public HMSMirrorAppService(ExecuteSessionService executeSessionService,
                               ConnectionPoolService connectionPoolService,
                               DatabaseService databaseService,
                               ReportWriterService reportWriterService,
                               TableService tableService,
                               TranslatorService translatorService,
                               TransferService transferService,
                               ConfigService configService,
                               EnvironmentService environmentService) {
        this.executeSessionService = executeSessionService;
        this.connectionPoolService = connectionPoolService;
        this.databaseService = databaseService;
        this.reportWriterService = reportWriterService;
        this.tableService = tableService;
        this.translatorService = translatorService;
        this.transferService = transferService;
        this.configService = configService;
        this.environmentService = environmentService;
    }

    public long getReturnCode() {
        long rtn = 0L;
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();
        Conversion conversion = executeSessionService.getSession().getConversion();
        rtn = runStatus.getReturnCode();
        // If app ran, then check for unsuccessful table conversions.
        if (rtn == 0) {
            rtn = conversion.getUnsuccessfullTableCount();
        }
        return rtn;
    }

    public long getWarningCode() {
        long rtn = 0L;
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();
        Conversion conversion = executeSessionService.getSession().getConversion();
        rtn = runStatus.getWarningCode();
        return rtn;
    }

    @Async("executionThreadPool")
    public Future<Boolean> run() {
        Boolean rtn = Boolean.TRUE;
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        // Clean up session before continuing.
        config.reset();
        RunStatus runStatus = session.getRunStatus();
        Conversion conversion = session.getConversion();
        // Reset Start time to the actual 'execution' start time.
        runStatus.setStart(new Date());
        runStatus.setProgress(ProgressEnum.STARTED);
        OperationStatistics stats = runStatus.getOperationStatistics();
        log.info("Starting HMSMirrorAppService.run()");
        runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.IN_PROGRESS);
        if (configService.validate(session, executeSessionService.getCliEnvironment())) {
            runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.COMPLETED);
        } else {
            runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.ERRORED);
            reportWriterService.wrapup();
            runStatus.setProgress(ProgressEnum.FAILED);
            return new AsyncResult<>(Boolean.FALSE);
        }

        if (!config.isLoadingTestData()) {
            try {// Refresh the connections pool.
                runStatus.setStage(StageEnum.VALIDATE_CONNECTION_CONFIG, CollectionEnum.IN_PROGRESS);

                configService.validateForConnections(session);
                runStatus.setStage(StageEnum.VALIDATE_CONNECTION_CONFIG, CollectionEnum.COMPLETED);

                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.IN_PROGRESS);

                // Make sure that the Kerberos Environment is initialized.
                environmentService.setupGSS();

                // The 'reset' should close and init all connections.
                boolean lclConnCheck = connectionPoolService.init();
                if (lclConnCheck) {
                    runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.COMPLETED);
                } else {
                    runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                    runStatus.setProgress(ProgressEnum.FAILED);
                    connectionPoolService.close();
                    runStatus.setProgress(ProgressEnum.FAILED);
                    return new AsyncResult<>(Boolean.FALSE);
                }
            } catch (SQLException sqle) {
                log.error("Issue refreshing connections pool", sqle);
                runStatus.addError(CONNECTION_ISSUE, sqle.getMessage());
                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return new AsyncResult<>(Boolean.FALSE);
            } catch (URISyntaxException e) {
                log.error("URI issue with connections pool", e);
                runStatus.addError(CONNECTION_ISSUE, e.getMessage());
                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return new AsyncResult<>(Boolean.FALSE);
            } catch (SessionException se) {
                log.error("Issue with Session", se);
                runStatus.addError(SESSION_ISSUE, se.getMessage());
                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return new AsyncResult<>(Boolean.FALSE);
            } catch (EncryptionException ee) {
                log.error("Issue with Decryption", ee);
                runStatus.addError(ENCRYPTION_ISSUE, ee.getMessage());
                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return new AsyncResult<>(Boolean.FALSE);
            } catch (RuntimeException rte) {
                log.error("Runtime Issue", rte);
                runStatus.addError(SESSION_ISSUE, rte.getMessage());
                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return new AsyncResult<>(Boolean.FALSE);
            }
        } else {
            runStatus.setStage(StageEnum.VALIDATE_CONNECTION_CONFIG, CollectionEnum.SKIPPED);
            runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.SKIPPED);
        }


        // Correct the load data issue ordering.
        if (config.isLoadingTestData() &&
                (!config.loadMetadataDetails() && config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
            // Remove Partition Data to ensure we don't use it.  Sets up a clean run like we're starting from scratch.
            log.info("Resetting Partition Data for Test Data Load");
            for (DBMirror dbMirror : conversion.getDatabases().values()) {
                runStatus.getOperationStatistics().getCounts().incrementDatabases();
                for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
                    runStatus.getOperationStatistics().getCounts().incrementTables();
                    for (EnvironmentTable et : tableMirror.getEnvironments().values()) {
                        et.getPartitions().clear();
                    }
                }
            }
        }

        log.info("Starting Application Workflow");
        runStatus.setProgress(ProgressEnum.IN_PROGRESS);

        Date startTime = new Date();
        runStatus.setStage(StageEnum.GATHERING_DATABASES, CollectionEnum.IN_PROGRESS);

        if (config.isLoadingTestData()) {
            log.info("Loading Test Data.  Skipping Database Collection.");
            Set<String> databases = new TreeSet<>();
            for (DBMirror dbMirror : conversion.getDatabases().values()) {
                stats.getCounts().incrementDatabases();
                databases.add(dbMirror.getName());
            }
//            String[] dbs = databases.toArray(new String[0]);
            config.setDatabases(databases);
        } else if (!isBlank(config.getFilter().getDbRegEx())) {
            // Look for the dbRegEx.
            log.info("Using dbRegEx: {}", config.getFilter().getDbRegEx());
            Connection conn = null;
            Statement stmt = null;
            Set<String> databases = new TreeSet<>();
            try {
                log.info("Getting LEFT Cluster Connection");
                conn = connectionPoolService.getHS2EnvironmentConnection(Environment.LEFT);
                //getConfig().getCluster(Environment.LEFT).getConnection();
                if (nonNull(conn)) {
                    log.info("Retrieved LEFT Cluster Connection");
                    stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(MirrorConf.SHOW_DATABASES);
                    while (rs.next()) {
                        String db = rs.getString(1);
                        Matcher matcher = config.getFilter().getDbFilterPattern().matcher(db);
                        if (matcher.find()) {
                            stats.getCounts().incrementDatabases();
                            databases.add(db);
                        }
                    }
                    config.setDatabases(databases);
                }
            } catch (SQLException se) {
                // Issue
                log.error("Issue getting databases for dbRegEx", se);
                stats.getFailures().incrementDatabases();
                runStatus.setStage(StageEnum.GATHERING_DATABASES, CollectionEnum.ERRORED);
                executeSessionService.getSession().addError(MISC_ERROR, "LEFT:Issue getting databases for dbRegEx");
                reportWriterService.wrapup();
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return new AsyncResult<>(Boolean.FALSE);
            } finally {
                if (nonNull(conn)) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        log.error("Issue closing connections for LEFT", e);
                        executeSessionService.getSession().addError(MISC_ERROR, "LEFT:Issue closing connections.");
                    }
                }
            }
        }
        runStatus.setStage(StageEnum.GATHERING_DATABASES, CollectionEnum.COMPLETED);
        log.info("Start Processing for databases: {}", String.join(",", config.getDatabases()));

        if (!config.isLoadingTestData()) {
            runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.IN_PROGRESS);
            rtn = databaseService.loadEnvironmentVars();
            if (rtn) {
                runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.ERRORED);
                reportWriterService.wrapup();
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return new AsyncResult<>(Boolean.FALSE);
            }
        } else {
            runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.SKIPPED);
        }

        if (isNull(config.getDatabases()) || config.getDatabases().isEmpty()) {
            log.error("No databases specified OR found if you used dbRegEx");
            runStatus.addError(MISC_ERROR, "No databases specified OR found if you used dbRegEx");
            connectionPoolService.close();
            runStatus.setProgress(ProgressEnum.FAILED);
            return new AsyncResult<>(Boolean.FALSE);
        }

        List<Future<ReturnStatus>> gtf = new ArrayList<>();
        // ========================================
        // Get the Database definitions for the LEFT and RIGHT clusters.
        // ========================================
        log.info("RunStatus Stage: {} is {}", StageEnum.DATABASES, runStatus.getStage(StageEnum.DATABASES));
        if (!config.isLoadingTestData()) {
            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.IN_PROGRESS);
            for (String database : config.getDatabases()) {
                runStatus.getOperationStatistics().getCounts().incrementDatabases();
                DBMirror dbMirror = conversion.addDatabase(database);
                try {
                    // Get the Database definitions for the LEFT and RIGHT clusters.
                    if (getDatabaseService().getDatabase(dbMirror, Environment.LEFT)) { //getConfig().getCluster(Environment.LEFT).getDatabase(config, dbMirror)) {
                        getDatabaseService().getDatabase(dbMirror, Environment.RIGHT);
                    } else {
                        // LEFT DB doesn't exists.
                        dbMirror.addIssue(Environment.LEFT, "DB doesn't exist. Check permissions for user running process");
                        runStatus.getOperationStatistics().getFailures().incrementDatabases();
                        rtn = Boolean.FALSE;
                    }
                } catch (SQLException se) {
                    log.error("Issue getting databases", se);
                    executeSessionService.getSession().addError(MISC_ERROR, "Issue getting databases");
                    runStatus.getOperationStatistics().getFailures().incrementDatabases();
                    runStatus.setStage(StageEnum.DATABASES, CollectionEnum.ERRORED);
                    reportWriterService.wrapup();
                    connectionPoolService.close();
                    runStatus.setProgress(ProgressEnum.FAILED);
                    return new AsyncResult<>(Boolean.FALSE);
                } catch (RuntimeException rte) {
                    log.error("Runtime Issue", rte);
                    runStatus.addError(MISC_ERROR, rte.getMessage());
                    runStatus.getOperationStatistics().getFailures().incrementDatabases();
                    runStatus.setStage(StageEnum.DATABASES, CollectionEnum.ERRORED);
                    reportWriterService.wrapup();
                    connectionPoolService.close();
                    runStatus.setProgress(ProgressEnum.FAILED);
                    return new AsyncResult<>(Boolean.FALSE);
                }

                // Build out the table in a database.
                if (!config.isLoadingTestData() && !config.isDatabaseOnly()) {
                    runStatus.setStage(StageEnum.TABLES, CollectionEnum.IN_PROGRESS);
                    Future<ReturnStatus> gt = getTableService().getTables(dbMirror);
                    gtf.add(gt);
                }
            }
            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.COMPLETED);

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
                        log.error("Interrupted Table collection", e);
                        rtn = Boolean.FALSE;
//                        throw new RuntimeException(e);
                    }
                }
                if (check)
                    break;
            }
            runStatus.setStage(StageEnum.TABLES, CollectionEnum.COMPLETED);
            gtf.clear(); // reset

            // Failure, report and exit with FALSE
            if (!rtn) {
                runStatus.setStage(StageEnum.TABLES, CollectionEnum.ERRORED);
                runStatus.addError(MessageCode.COLLECTING_TABLES);
                reportWriterService.wrapup();
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return new AsyncResult<>(Boolean.FALSE);
            }
        } else {
            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.SKIPPED);
            runStatus.setStage(StageEnum.TABLES, CollectionEnum.SKIPPED);
        }

        if (config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().isEmpty()) {
            runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.SKIPPED);
        } else {
            try {
                int defaultConsolidationLevel = 1;
                runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.IN_PROGRESS);

                if (config.loadMetadataDetails()) {
                    databaseService.buildDatabaseSources(defaultConsolidationLevel, false);
                    translatorService.buildGlobalLocationMapFromWarehousePlansAndSources(false, defaultConsolidationLevel);
                    runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.COMPLETED);
                } else {
                    runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.SKIPPED);
                }

            } catch (EncryptionException | SessionException | RequiredConfigurationException | MismatchException e) {
                log.error("Issue validating configuration", e);
                runStatus.addError(RUNTIME_EXCEPTION, e.getMessage());
                log.error("Configuration is not valid.  Exiting.");

                runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.ERRORED);
                runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.ERRORED);

                reportWriterService.wrapup();
                connectionPoolService.close();

                runStatus.setProgress(ProgressEnum.FAILED);

                return new AsyncResult<>(Boolean.FALSE);
            }
        }

        runStatus.setStage(StageEnum.BUILDING_DATABASES, CollectionEnum.IN_PROGRESS);
        // Don't build DB with VIEW Migrations.
        if (!config.getMigrateVIEW().isOn()) {
            if (getDatabaseService().build()) {
                runStatus.getOperationStatistics().getSuccesses().incrementDatabases();
            } else {
                runStatus.addError(MessageCode.DATABASE_CREATION);
                runStatus.getOperationStatistics().getFailures().incrementDatabases();
                rtn = Boolean.FALSE;
            }
            if (rtn) {
                runStatus.setStage(StageEnum.BUILDING_DATABASES, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.BUILDING_DATABASES, CollectionEnum.ERRORED);
                runStatus.addError(MessageCode.BUILDING_DATABASES_ISSUE);
            }
        } else {
            runStatus.setStage(StageEnum.BUILDING_DATABASES, CollectionEnum.SKIPPED);
            conversion.getDatabases().values().forEach(db -> {
                        db.addIssue(Environment.LEFT, "No Database DDL when migrating 'views'." +
                            "All database constructs expected to be in-place already.");
                        db.addIssue(Environment.RIGHT, "No Database DDL when migrating 'views'." +
                                "All database constructs expected to be in-place already.");
                    }
            );
        }

        // Shortcut.  Only DB's.
        if (!config.isDatabaseOnly()) {
            Set<String> collectedDbs = conversion.getDatabases().keySet();
            // ========================================
            // Get the table METADATA for the tables collected in the databases.
            // ========================================
            List<Future<ReturnStatus>> migrationFuture = new ArrayList<>();

            runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.IN_PROGRESS);
            if (rtn) {
                for (String database : collectedDbs) {
                    DBMirror dbMirror = conversion.getDatabase(database);
                    Set<String> tables = dbMirror.getTableMirrors().keySet();
                    for (String table : tables) {
                        TableMirror tableMirror = dbMirror.getTableMirrors().get(table);
                        gtf.add(tableService.getTableMetadata(tableMirror));
                    }
                }

                runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.IN_PROGRESS);

                // Go through the Futures and check status.
                // When SUCCESSFUL, move on to the next step.
                // ========================================
                // Check that a tables metadata has been retrieved.  When it has (ReturnStatus.Status.CALCULATED_SQL),
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
                            if (sf.isDone() && sf.get() != null) {
                                switch (sf.get().getStatus()) {
                                    case SUCCESS:
                                        runStatus.getOperationStatistics().getCounts().incrementTables();
                                        // Trigger next step and set status.
                                        // TODO: Next Step
                                        sf.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                        // Launch the next step, which is the transfer.
                                        runStatus.getOperationStatistics().getSuccesses().incrementTables();

                                        migrationFuture.add(getTransferService().build(sf.get().getTableMirror()));
                                        break;
                                    case ERROR:
                                        runStatus.getOperationStatistics().getCounts().incrementTables();
                                        sf.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                        break;
                                    case FATAL:
                                        runStatus.getOperationStatistics().getCounts().incrementTables();
                                        runStatus.getOperationStatistics().getFailures().incrementTables();
                                        rtn = Boolean.FALSE;
                                        sf.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                        log.error("FATAL: ", sf.get().getException());
                                    case NEXTSTEP:
                                        break;
                                    case SKIP:
                                        runStatus.getOperationStatistics().getCounts().incrementTables();
                                        // Set for tables that are being removed.
                                        runStatus.getOperationStatistics().getSkipped().incrementTables();
                                        sf.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                        break;
                                }
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            rtn = Boolean.FALSE;
                            log.error("Interrupted", e);
                        }
                    }
                    if (check)
                        break;
                    try {
                        // Slow down the loop.
                        sleep(2000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                if (rtn) {
                    runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.COMPLETED);
                } else {
                    runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.ERRORED);
                    runStatus.addError(MessageCode.COLLECTING_TABLE_DEFINITIONS);
                }

                gtf.clear(); // reset

                // Remove the tables that are marked for removal.
                for (String database : collectedDbs) {
                    DBMirror dbMirror = conversion.getDatabase(database);
                    Set<String> tables = dbMirror.getTableMirrors().keySet();
                    for (String table : tables) {
                        TableMirror tableMirror = dbMirror.getTableMirrors().get(table);
                        if (tableMirror.isRemove()) {
                            // Setup the filtered out tables so they can be reported w/ reason.
//                        stats.getSkipped().incrementTables();
                            log.info("Table: {}.{} is being removed from further processing. Reason: {}", dbMirror.getName(), table, tableMirror.getRemoveReason());
                            dbMirror.getFilteredOut().put(table, tableMirror.getRemoveReason());
                        }
                    }
                    log.info("Removing tables marked for removal from further processing.");
                    dbMirror.getTableMirrors().values().removeIf(TableMirror::isRemove);
                    log.info("Tables marked for removal have been removed from further processing.");
                }

            } else {
                runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.SKIPPED);
                runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.SKIPPED);
            }

            Set<TableMirror> migrationExecutions = new HashSet<>();

            // Check the Migration Futures are done.
            while (true) {
                boolean check = true;
                for (Future<ReturnStatus> sf : migrationFuture) {
                    if (!sf.isDone()) {
                        check = false;
                        continue;
                    }
                    try {
                        if (sf.isDone() && sf.get() != null) {
                            TableMirror tableMirror = sf.get().getTableMirror();
                            // Only push SUCCESSFUL tables to the migrationExecutions list.
                            if (sf.get().getStatus() == ReturnStatus.Status.SUCCESS) {
                                // Success means add table the execution list.
                                migrationExecutions.add(tableMirror);
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Interrupted", e);
                        rtn = Boolean.FALSE;
//                        throw new RuntimeException(e);
                    }
                }
                if (check)
                    break;
                try {
                    // Slow down the loop.
                    sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (rtn) {
                runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.ERRORED);
//                runStatus.addError(MessageCode.BUILDING_TABLES_ISSUE);
            }

            migrationFuture.clear(); // reset

            // Validate the SET statements.
            runStatus.setStage(StageEnum.VALIDATING_ENVIRONMENT_SETS, CollectionEnum.IN_PROGRESS);
            if (rtn) {
                // Check the Unique SET statements.
                for (String database : collectedDbs) {
                    DBMirror dbMirror = conversion.getDatabase(database);
                    if (!databaseService.checkSqlStatements(dbMirror)) {
                        rtn = Boolean.FALSE;
                    }
                }
                if (rtn) {
                    runStatus.setStage(StageEnum.VALIDATING_ENVIRONMENT_SETS, CollectionEnum.COMPLETED);
                } else {
                    runStatus.setStage(StageEnum.VALIDATING_ENVIRONMENT_SETS, CollectionEnum.ERRORED);
                    runStatus.addError(MessageCode.VALIDATE_SQL_STATEMENT_ISSUE);

                }
            } else {
                runStatus.setStage(StageEnum.VALIDATING_ENVIRONMENT_SETS, CollectionEnum.SKIPPED);
            }

            // Process the SQL for the Databases;
            runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.IN_PROGRESS);
            if (rtn) {
                if (config.isExecute()) {
                    if (getDatabaseService().execute()) {
                        runStatus.getOperationStatistics().getSuccesses().incrementDatabases();
                        runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.COMPLETED);
                    } else {
                        runStatus.addError(MessageCode.DATABASE_CREATION);
                        runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.ERRORED);
                        runStatus.getOperationStatistics().getFailures().incrementDatabases();
                        rtn = Boolean.FALSE;
                    }
                } else {
                    runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.SKIPPED);
                }
                // Set error if issue during processing.
                if (!rtn)
                    runStatus.addError(MessageCode.PROCESSING_DATABASES_ISSUE);

            } else {
                runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.SKIPPED);
            }

            // Process the SQL for the Tables;

            runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.IN_PROGRESS);
            if (rtn) {
                if (config.isExecute()) {
                    // Using the migrationExecute List, create futures for the table executions.
                    for (TableMirror tableMirror : migrationExecutions) {
                        migrationFuture.add(getTransferService().execute(tableMirror));
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
                                    TableMirror tableMirror = sf.get().getTableMirror();
                                    if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                                        // Check if the table was removed, so that's not a processing error.
                                        if (tableMirror != null) {
                                            if (!tableMirror.isRemove()) {
                                                rtn = Boolean.FALSE;
                                            }
                                        }
                                    }
                                }
                            } catch (InterruptedException | ExecutionException e) {
                                log.error("Interrupted", e);
                                rtn = Boolean.FALSE;
//                        throw new RuntimeException(e);
                            }
                        }
                        if (check)
                            break;
                        try {
                            // Slow down the loop.
                            sleep(2000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    // If still TRUE, then we're good.
                    if (rtn) {
                        runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.COMPLETED);
                    } else {
                        runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.ERRORED);
                        runStatus.addError(MessageCode.PROCESSING_TABLES_ISSUE);
                    }
                } else {
                    runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.SKIPPED);
                }
            } else {
                runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.SKIPPED);
            }

        }

        // Clean up Environment for Reports.
        switch (config.getDataStrategy()) {
            case STORAGE_MIGRATION:
            case DUMP:
                // Clean up RIGHT cluster definition.
                if (nonNull(config.getCluster(Environment.RIGHT))) {
                    config.getClusters().remove(Environment.RIGHT);
                }
            default:
                if (nonNull(config.getCluster(Environment.SHADOW))) {
                    config.getClusters().remove(Environment.SHADOW);
                }
                if (nonNull(config.getCluster(Environment.TRANSFER))) {
                    config.getClusters().remove(Environment.TRANSFER);
                }
                break;
        }

        runStatus.setStage(StageEnum.SAVING_REPORTS, CollectionEnum.IN_PROGRESS);
        // Set RunStatus End Date.
        runStatus.setEnd(new Date());

        // Set Run Status Progress.
        if (rtn) {
            runStatus.setProgress(ProgressEnum.COMPLETED);
        } else {
            runStatus.setProgress(ProgressEnum.FAILED);
        }

        try {
            runStatus.setStage(StageEnum.SAVING_REPORTS, CollectionEnum.COMPLETED);
            reportWriterService.wrapup();
        } catch (RuntimeException rte) {
            log.error("Issue saving reports", rte);
            runStatus.addError(MISC_ERROR, rte.getMessage());
            runStatus.setStage(StageEnum.SAVING_REPORTS, CollectionEnum.ERRORED);
            rtn = Boolean.FALSE;
        } finally {
            // Close down the connections to free up resources.
            connectionPoolService.close();
        }

        return new AsyncResult<>(rtn);
    }
}
