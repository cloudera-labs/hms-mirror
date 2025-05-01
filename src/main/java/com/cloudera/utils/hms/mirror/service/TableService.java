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

//import com.cloudera.utils.hadoop.HadoopSession;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.stage.ReturnStatus;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;

import static com.cloudera.utils.hms.mirror.MessageCode.METASTORE_PARTITION_LOCATIONS_NOT_FETCHED;
import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.HMS_STORAGE_MIGRATION_FLAG;
import static com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.DUMP;
import static com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.STORAGE_MIGRATION;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Getter
@Setter
@Slf4j
public class TableService {
    private final DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
    private final DateFormat tdf = new SimpleDateFormat("HH:mm:ss.SSS");

    private final ConfigService configService;
    private final ExecuteSessionService executeSessionService;
    private final ConnectionPoolService connectionPoolService;
    private final QueryDefinitionsService queryDefinitionsService;
    private final TranslatorService translatorService;
    private final StatsCalculatorService statsCalculatorService;

    public TableService(
            ConfigService configService,
            ExecuteSessionService executeSessionService,
            ConnectionPoolService connectionPoolService,
            QueryDefinitionsService queryDefinitionsService,
            TranslatorService translatorService,
            StatsCalculatorService statsCalculatorService
    ) {
        this.configService = configService;
        this.executeSessionService = executeSessionService;
        this.connectionPoolService = connectionPoolService;
        this.queryDefinitionsService = queryDefinitionsService;
        this.translatorService = translatorService;
        this.statsCalculatorService = statsCalculatorService;
    }

    protected void checkTableFilter(TableMirror tableMirror, Environment environment) {
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        RunStatus runStatus = session.getRunStatus();
        OperationStatistics stats = runStatus.getOperationStatistics();

        if (isNull(et) | isEmpty(et.getDefinition())) {
            return;
        }

        if (config.getMigrateVIEW().isOn() && config.getDataStrategy() != DUMP) {
            if (!TableUtils.isView(et)) {
                tableMirror.setRemove(Boolean.TRUE);
                tableMirror.setRemoveReason("VIEW's only processing selected.");
            }
        } else {
            // Check if ACID for only the LEFT cluster.  If it's the RIGHT cluster, other steps will deal with
            // the conflict.  IE: Rename or exists already.
            if (TableUtils.isManaged(et)) {
                if (TableUtils.isACID(et)) {
                    // For ACID tables, check that Migrate is ON.
                    if (config.getMigrateACID().isOn()) {
                        tableMirror.addStep("TRANSACTIONAL", Boolean.TRUE);
                    } else {
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("ACID table and ACID processing not selected (-ma|-mao).");
                    }
                } else if (config.getMigrateACID().isOnly()) {
                    // Non ACID Tables should NOT be process if 'isOnly' is set.
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("Non-ACID table and ACID only processing selected `-mao`");
                }
            } else if (TableUtils.isHiveNative(et)) {
                // Non ACID Tables should NOT be process if 'isOnly' is set.
                if (config.getMigrateACID().isOnly()) {
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("Non-ACID table and ACID only processing selected `-mao`");
                }
            } else if (TableUtils.isView(et)) {
                if (config.getDataStrategy() != DUMP) {
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("This is a VIEW and VIEW processing wasn't selected.");
                }
            } else {
                // Non-Native Tables.
                if (!config.isMigrateNonNative()) {
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("This is a Non-Native hive table and non-native process wasn't " +
                            "selected.");
                }
            }
        }
//        }

        // Check for tables migration flag, to avoid 're-migration'.
        if (config.getDataStrategy() == STORAGE_MIGRATION) {
            String smFlag = TableUtils.getTblProperty(HMS_STORAGE_MIGRATION_FLAG, et);
            if (smFlag != null) {
                tableMirror.setRemove(Boolean.TRUE);
                tableMirror.setRemoveReason("The table has already gone through the STORAGE_MIGRATION process on " +
                        smFlag + " If this isn't correct, remove the TBLPROPERTY '" + HMS_STORAGE_MIGRATION_FLAG + "' " +
                        "from the table and try again.");
            }
        }

        // Check for table size filter
        if (config.getFilter().getTblSizeLimit() != null && config.getFilter().getTblSizeLimit() > 0) {
            Long dataSize = (Long) et.getStatistics().get(DATA_SIZE);
            if (dataSize != null) {
                if (config.getFilter().getTblSizeLimit() * (1024 * 1024) < dataSize) {
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("The table dataset size exceeds the specified table filter size limit: " +
                            config.getFilter().getTblSizeLimit() + "Mb < " + dataSize);
                }
            }
        }

    }

    public String getCreateStatement(TableMirror tableMirror, Environment environment) {
        StringBuilder createStatement = new StringBuilder();
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        Cluster cluster = config.getCluster(environment);
        Boolean cine = Boolean.FALSE;
        if (nonNull(cluster)) {
            cine = cluster.isCreateIfNotExists();
        }
        List<String> tblDef = tableMirror.getTableDefinition(environment);
        if (tblDef != null) {
            Iterator<String> iter = tblDef.iterator();
            while (iter.hasNext()) {
                String line = iter.next();
                if (cine && line.startsWith("CREATE TABLE")) {
                    line = line.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                } else if (cine && line.startsWith("CREATE EXTERNAL TABLE")) {
                    line = line.replace("CREATE EXTERNAL TABLE", "CREATE EXTERNAL TABLE IF NOT EXISTS");
                }
                createStatement.append(line);
                if (iter.hasNext()) {
                    createStatement.append("\n");
                }
            }
        } else {
            log.error("Couldn't location definition for table: {} in environment: {}", tableMirror.getName(), environment.toString());
        }
        return createStatement.toString();
    }

    public void getTableDefinition(TableMirror tableMirror, Environment environment) throws SQLException {
        // The connections should already be in the database;
        log.debug("Getting table definition for {}:{}.{}",
                environment, tableMirror.getParent().getName(), tableMirror.getName());
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        // Fetch Table Definition.
        if (hmsMirrorConfig.isLoadingTestData()) {
            // Already loaded from before.
        } else {
            loadSchemaFromCatalog(tableMirror, environment);
        }

        checkTableFilter(tableMirror, environment);

        if (!tableMirror.isRemove() && !hmsMirrorConfig.isLoadingTestData()) {
            switch (hmsMirrorConfig.getDataStrategy()) {
                case SCHEMA_ONLY:
                case CONVERT_LINKED:
                case DUMP:
                case LINKED:
                    // These scenario don't require stats.
                    break;
                case SQL:
                case HYBRID:
                case EXPORT_IMPORT:
                case STORAGE_MIGRATION:
                case COMMON:
                case ACID:
                    if (!TableUtils.isView(et) && TableUtils.isHiveNative(et)) {
                        try {
                            loadTableStats(tableMirror, environment);
                        } catch (DisabledException e) {
                            log.warn("Stats collection is disabled because the CLI Interface has been disabled. " +
                                    " Skipping stats collection for table: {}", et.getName());
                        } catch (RuntimeException rte) {
                            log.error("Issue loading table stats for {}.{}", et.getName(), et.getParent().getName());
                            tableMirror.addIssue(environment, rte.getMessage());
                            log.error(rte.getMessage(), rte);
                        }
                    }
                    break;
            }
        }

        Boolean partitioned = TableUtils.isPartitioned(et);
        if (environment == Environment.LEFT && partitioned
                && !tableMirror.isRemove() && !hmsMirrorConfig.isLoadingTestData()) {
            /*
            If we are -epl, we need to load the partition metadata for the table. And we need to use the
            metastore_direct connections to do so. Trying to load this through the standard Hive SQL process
            is 'extremely' slow.
             */
            if (hmsMirrorConfig.loadMetadataDetails()) {
                loadTablePartitionMetadataDirect(tableMirror, environment);
            }
        }

        // Check for table partition count filter
        if (hmsMirrorConfig.getFilter().getTblPartitionLimit() != null && hmsMirrorConfig.getFilter().getTblPartitionLimit() > 0) {
            Integer partLimit = hmsMirrorConfig.getFilter().getTblPartitionLimit();
            if (et.getPartitions().size() > partLimit) {
                tableMirror.setRemove(Boolean.TRUE);
                tableMirror.setRemoveReason("The table partition count exceeds the specified table filter partition limit: " +
                        hmsMirrorConfig.getFilter().getTblPartitionLimit() + " < " + et.getPartitions().size());

            }
        }
        log.info("Completed table definition for {}:{}.{}",
                environment, tableMirror.getParent().getName(), tableMirror.getName());
    }

    @Async("metadataThreadPool")
    public CompletableFuture<ReturnStatus> getTableMetadata(TableMirror tableMirror) {
        ReturnStatus rtn = new ReturnStatus();
        rtn.setTableMirror(tableMirror);
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();

        try {
            getTableDefinition(tableMirror, Environment.LEFT);
            if (tableMirror.isRemove()) {
                rtn.setStatus(ReturnStatus.Status.SKIP);
                return CompletableFuture.completedFuture(rtn);
            } else {
                switch (hmsMirrorConfig.getDataStrategy()) {
                    case DUMP:
                    case STORAGE_MIGRATION:
                        // Make a clone of the left as a working copy.
                        try {
                            tableMirror.getEnvironments().put(Environment.RIGHT, tableMirror.getEnvironmentTable(Environment.LEFT).clone());
                        } catch (CloneNotSupportedException e) {
                            log.error("Clone not supported for table: {}.{}", tableMirror.getParent().getName(), tableMirror.getName());
                        }
                        rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
                        break;
                    default:
                        getTableDefinition(tableMirror, Environment.RIGHT);
                        rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
                }
            }
        } catch (SQLException throwables) {
            log.error(throwables.getMessage(), throwables);
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(throwables);
        }
        return CompletableFuture.completedFuture(rtn);
    }

    @Async("metadataThreadPool")
    public CompletableFuture<ReturnStatus> getTables(DBMirror dbMirror) {
        ReturnStatus rtn = new ReturnStatus();
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig hmsMirrorConfig = session.getConfig();
        RunStatus runStatus = session.getRunStatus();
        log.debug("Getting tables for Database {}", dbMirror.getName());
        try {
            getTables(dbMirror, Environment.LEFT);
            if (hmsMirrorConfig.isSync()) {
                // Get the tables on the RIGHT side.  Used to determine if a table has been dropped on the LEFT
                // and later needs to be removed on the RIGHT.
                try {
                    getTables(dbMirror, Environment.RIGHT);
                } catch (SQLException se) {
                    // OK, if the db doesn't exist yet.
                }
            }
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } catch (SQLException throwables) {
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(throwables);
        } catch (RuntimeException rte) {
            log.error("Runtime Issue getting tables for Database: {}", dbMirror.getName(), rte);
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(rte);
        }
        return CompletableFuture.completedFuture(rtn);
    }

    public void getTables(DBMirror dbMirror, Environment environment) throws SQLException {
        Connection conn = null;
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        RunStatus runStatus = session.getRunStatus();
        OperationStatistics stats = runStatus.getOperationStatistics();

        try {
            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
            if (conn != null) {
                String database = (environment == Environment.LEFT ?
                        dbMirror.getName() : HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config));

                log.info("Loading tables for {}:{}", environment, database);

                Statement stmt = null;
                ResultSet resultSet = null;
                // Stub out the tables
                try {
                    stmt = conn.createStatement();
                    log.debug("Setting Hive DB Session Context {}:{}", environment, database);
                    stmt.execute(MessageFormat.format(MirrorConf.USE, database));
                    List<String> shows = new ArrayList<String>();
                    if (!config.getCluster(environment).isLegacyHive()) {
                        if (config.getMigrateVIEW().isOn()) {
                            shows.add(MirrorConf.SHOW_VIEWS);
                            if (config.getDataStrategy() == DUMP) {
                                shows.add(MirrorConf.SHOW_TABLES);
                            }
                        } else {
                            shows.add(MirrorConf.SHOW_TABLES);
                        }
                    } else {
                        shows.add(MirrorConf.SHOW_TABLES);
                    }
                    for (String show : shows) {
                        resultSet = stmt.executeQuery(show);
                        log.debug("Running show statement: {} to collect objects", show);
                        while (resultSet.next()) {
                            String tableName = resultSet.getString(1);
                            log.trace("Table: {}", tableName);
                            if (tableName.startsWith(config.getTransfer().getTransferPrefix())) {
                                TableMirror tableMirror = dbMirror.addTable(tableName);
                                tableMirror.setRemove(Boolean.TRUE);
                                tableMirror.setRemoveReason("Table name matches the transfer prefix.  " +
                                        "This is most likely a remnant of a previous event.  If this is a mistake, " +
                                        "change the 'transferPrefix' to something more unique.");
                                log.info("{}.{} was NOT added to list.  " +
                                        "The name matches the transfer prefix and is most likely a remnant of a previous " +
                                        "event. If this is a mistake, change the 'transferPrefix' to something more unique.", database, tableName);
                            } else if (tableName.startsWith(config.getTransfer().getShadowPrefix())) {
                                TableMirror tableMirror = dbMirror.addTable(tableName);
                                tableMirror.setRemove(Boolean.TRUE);
                                tableMirror.setRemoveReason("Table name matches the shadow prefix.  " +
                                        "This is most likely a remnant of a previous event.  If this is a mistake, " +
                                        "change the 'shadowPrefix' to something more unique.");
                                log.info("{}.{} was NOT added to list.  " +
                                        "The name matches the shadow prefix and is most likely a remnant of a previous " +
                                        "event. If this is a mistake, change the 'shadowPrefix' to something more unique.", database, tableName);
                            } else if (tableName.endsWith(config.getTransfer().getStorageMigrationPostfix())) {
                                TableMirror tableMirror = dbMirror.addTable(tableName);
                                tableMirror.setRemove(Boolean.TRUE);
                                tableMirror.setRemoveReason("Table name matches the 'storage migration' suffix.  " +
                                        "This is most likely a remnant of a previous event.  If this is a mistake, " +
                                        "change the 'StorageMigrationPostfix' to something more unique.");
                                log.info("{}.{} was NOT added to list.  " +
                                        "The name is the result of a previous STORAGE_MIGRATION attempt that has not been " +
                                        "cleaned up.", database, tableName);
                            } else {
                                if (isBlank(config.getFilter().getTblRegEx()) && isBlank(config.getFilter().getTblExcludeRegEx())) {
                                    log.info("{}.{} added to processing list.", database, tableName);
                                    TableMirror tableMirror = dbMirror.addTable(tableName);
                                    tableMirror.setUnique(df.format(config.getInitDate()));
                                    tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                } else if (!isBlank(config.getFilter().getTblRegEx())) {
                                    // Filter Tables
                                    assert (config.getFilter().getTblFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblFilterPattern().matcher(tableName);
                                    if (matcher.matches()) {
                                        log.info("{}.{} added to processing list.", database, tableName);
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setUnique(df.format(config.getInitDate()));
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    } else {
                                        log.info("{}.{} didn't match table regex filter and " +
                                                "will NOT be added to processing list.", database, tableName);
                                    }
                                } else if (config.getFilter().getTblExcludeRegEx() != null) {
                                    assert (config.getFilter().getTblExcludeFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblExcludeFilterPattern().matcher(tableName);
                                    if (!matcher.matches()) { // ANTI-MATCH
                                        log.info("{}.{} added to processing list.", database, tableName);
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setUnique(df.format(config.getInitDate()));
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    } else {
                                        log.info("{}.{} matched exclude table regex filter and " +
                                                "will NOT be added to processing list.", database, tableName);
                                    }
                                }
                            }
                        }
                    }
                } catch (SQLException se) {
                    log.error("{}:{} ", environment, database, se);
                    // This is helpful if the user running the process doesn't have permissions.
                    dbMirror.addIssue(environment, database + " " + se.getMessage());
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
                }
            }
        } catch (SQLException se) {
            throw se;
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    public void loadSchemaFromCatalog(TableMirror tableMirror, Environment environment) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        String database = null;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        if (environment == Environment.LEFT) {
            database = tableMirror.getParent().getName();
        } else {
            database = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);
        }
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);

        try {

            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);

            if (conn != null) {
                stmt = conn.createStatement();
                String useStatement = MessageFormat.format(MirrorConf.USE, database);
                stmt.execute(useStatement);
                String showStatement = MessageFormat.format(MirrorConf.SHOW_CREATE_TABLE, tableMirror.getName());
                resultSet = stmt.executeQuery(showStatement);
                List<String> tblDef = new ArrayList<String>();
                ResultSetMetaData meta = resultSet.getMetaData();
                if (meta.getColumnCount() >= 1) {
                    while (resultSet.next()) {
                        try {
                            tblDef.add(resultSet.getString(1).trim());
                        } catch (NullPointerException npe) {
                            // catch and continue.
                            log.error("Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. " +
                                            "ResultSet record(line) is null. Skipping. {}:{}.{}",
                                    environment, database, tableMirror.getName());
                        }
                    }
                } else {
                    log.error("Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. No Metadata. {}:{}.{}"
                            , environment, database, tableMirror.getName());
                }
                et.setDefinition(tblDef);
                et.setName(tableMirror.getName());
                // Identify that the table existed in the Database before other activity.
                et.setExists(Boolean.TRUE);
                tableMirror.addStep(environment.toString(), "Fetched Schema");

                // TODO: Don't do this is table removed from list.
                if (config.getOwnershipTransfer().isTable()) {
                    try {
                        String ownerStatement = MessageFormat.format(MirrorConf.SHOW_TABLE_EXTENDED, tableMirror.getName());
                        resultSet = stmt.executeQuery(ownerStatement);
                        String owner = null;
                        while (resultSet.next()) {

                            if (resultSet.getString(1).startsWith("owner")) {
                                String[] ownerLine = resultSet.getString(1).split(":");
                                try {
                                    owner = ownerLine[1];
                                } catch (Throwable t) {
                                    // Parsing issue.
                                    log.error("Couldn't parse 'owner' value from: {} for table {}:{}.{}"
                                            , resultSet.getString(1), environment, database, tableMirror.getName());
//                                            " for table: " + tableMirror.getParent().getName() + "." + tableMirror.getName());
                                }
                                break;
                            }
                        }
                        if (owner != null) {
                            et.setOwner(owner);
                        }
                    } catch (SQLException sed) {
                        // Failed to gather owner details.
                    }
                }

            }
        } catch (SQLException throwables) {
            if (throwables.getMessage().contains("Table not found") || throwables.getMessage().contains("Database does not exist")) {
                // This is ok in the upper cluster where we don't expect the table to exist if the process hadn't run before.
                tableMirror.addStep(environment.toString(), "No Schema");
            } else {
                log.error(throwables.getMessage(), throwables);
                et.addError(throwables.getMessage());
            }
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
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }

        }
    }

    protected void loadTableOwnership(TableMirror tableMirror, Environment environment) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        if (hmsMirrorConfig.getOwnershipTransfer().isTable()) {
            try {
                conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
                if (conn != null) {
                    stmt = conn.createStatement();

                    try {
                        String ownerStatement = MessageFormat.format(MirrorConf.SHOW_TABLE_EXTENDED, tableMirror.getName());
                        resultSet = stmt.executeQuery(ownerStatement);
                        String owner = null;
                        while (resultSet.next()) {

                            if (resultSet.getString(1).startsWith("owner")) {
                                String[] ownerLine = resultSet.getString(1).split(":");
                                try {
                                    owner = ownerLine[1];
                                } catch (Throwable t) {
                                    // Parsing issue.
                                    log.error("Couldn't parse 'owner' value from: {} for table: {}.{}", resultSet.getString(1), tableMirror.getParent().getName(), tableMirror.getName());
                                }
                                break;
                            }
                        }
                        if (owner != null) {
                            et.setOwner(owner);
                        }
                    } catch (SQLException sed) {
                        // Failed to gather owner details.
                    }

                }
            } catch (SQLException throwables) {
                if (throwables.getMessage().contains("Table not found") || throwables.getMessage().contains("Database does not exist")) {
                    // This is ok in the upper cluster where we don't expect the table to exist if the process hadn't run before.
                    tableMirror.addStep(environment.toString(), "No Schema");
                } else {
                    log.error(throwables.getMessage(), throwables);
                    et.addError(throwables.getMessage());
                }
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
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
            }
        }
    }

    protected void loadTablePartitionMetadata(TableMirror tableMirror, Environment environment) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        String database = tableMirror.getParent().getName();
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        try {
            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
            if (conn != null) {

                stmt = conn.createStatement();
                log.debug("{}:{}.{}: Loading Partitions", environment, database, et.getName());

                resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_PARTITIONS, database, et.getName()));
                Map<String, String> partDef = new HashMap<String, String>();
                while (resultSet.next()) {
                    partDef.put(resultSet.getString(1), NOT_SET);
                }
                et.setPartitions(partDef);

            }
        } catch (SQLException throwables) {
            et.addError(throwables.getMessage());
            log.error("{}:{}.{}: Issue loading Partitions.", environment, database, et.getName(), throwables);
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
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    protected void loadTablePartitionMetadataDirect(TableMirror tableMirror, Environment environment) {
        /*
        1. Get Metastore Direct Connection
        2. Get Query Definitions
        3. Get Query for 'part_locations'
        4. Execute Query
        5. Load Partition Data
         */
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        ExecuteSession session = executeSessionService.getSession();
        RunStatus runStatus = session.getRunStatus();
        HmsMirrorConfig config = session.getConfig();

        // TODO: Handle RIGHT Environment. At this point, we're only handling LEFT.
        if (!configService.isMetastoreDirectConfigured(session, environment)) {
            log.info("Metastore Direct Connection is not configured for {}.  Skipping.", environment);
            runStatus.addWarning(METASTORE_PARTITION_LOCATIONS_NOT_FETCHED);
            return;
        }

        String database = tableMirror.getParent().getName();
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        try {
            conn = getConnectionPoolService().getMetastoreDirectEnvironmentConnection(environment);
            log.info("Loading Partitions from Metastore Direct Connection {}:{}.{}", environment, database, et.getName());
            QueryDefinitions queryDefinitions = getQueryDefinitionsService().getQueryDefinitions(environment);
            if (queryDefinitions != null) {
                String partLocationQuery = queryDefinitions.getQueryDefinition("part_locations").getStatement();
                pstmt = conn.prepareStatement(partLocationQuery);
                pstmt.setString(1, database);
                pstmt.setString(2, et.getName());
                resultSet = pstmt.executeQuery();
                Map<String, String> partDef = new HashMap<String, String>();
                while (resultSet.next()) {
                    partDef.put(resultSet.getString(1), resultSet.getString(2));
                }
                et.setPartitions(partDef);
            }
            log.info("Loaded Partitions from Metastore Direct Connection {}:{}.{}", environment, database, et.getName());
        } catch (SQLException throwables) {
            et.addError(throwables.getMessage());
            log.error("Issue loading Partitions from Metastore Direct Connection. {}:{}.{}", environment, database, et.getName());
            log.error(throwables.getMessage(), throwables);
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    protected void loadTableStats(TableMirror tableMirror, Environment environment) throws DisabledException {
        // Considered only gathering stats for partitioned tables, but decided to gather for all tables to support
        //  smallfiles across the board.
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        if (hmsMirrorConfig.getOptimization().isSkipStatsCollection()) {
            log.debug("{}:{}: Skipping Stats Collection.", environment, et.getName());
            return;
        }
        switch (hmsMirrorConfig.getDataStrategy()) {
            case DUMP:
            case SCHEMA_ONLY:
                // We don't need stats for these.
                return;
            default:
                break;
        }

        // Determine File sizes in table or partitions.
        /*
        - Get Base location for table
        - Get HadoopSession
        - Do a 'count' of the location.
         */
        String location = TableUtils.getLocation(et.getName(), et.getDefinition());
        // Only run checks against hdfs and ozone namespaces.
        String[] locationParts = location.split(":");
        String protocol = locationParts[0];
        // Determine Table File Format
        TableUtils.getSerdeType(et);

        if (hmsMirrorConfig.getSupportFileSystems().contains(protocol)) {
            CliEnvironment cli = executeSessionService.getCliEnvironment();

            String countCmd = "count " + location;
            CommandReturn cr = cli.processInput(countCmd);
            if (!cr.isError() && cr.getRecords().size() == 1) {
                // We should only get back one record.
                List<Object> countRecord = cr.getRecords().get(0);
                // 0 = Folder Count
                // 1 = File Count
                // 2 = Size Summary
                try {
                    Double avgFileSize = (double) (Long.parseLong(countRecord.get(2).toString()) /
                            Integer.parseInt(countRecord.get(1).toString()));
                    et.getStatistics().put(DIR_COUNT, Integer.valueOf(countRecord.get(0).toString()));
                    et.getStatistics().put(FILE_COUNT, Integer.valueOf(countRecord.get(1).toString()));
                    et.getStatistics().put(DATA_SIZE, Long.valueOf(countRecord.get(2).toString()));
                    et.getStatistics().put(AVG_FILE_SIZE, avgFileSize);
                    et.getStatistics().put(TABLE_EMPTY, Boolean.FALSE);
                } catch (ArithmeticException ae) {
                    // Directory is probably empty.
                    et.getStatistics().put(TABLE_EMPTY, Boolean.TRUE);
                }
            } else {
                // Issue getting count.

            }
        }
    }

    /**
     * From this cluster, run the SQL built up in the tblMirror(environment)
     *
     * @param tblMirror
     * @param environment Allows to override cluster environment
     * @return
     */
    public Boolean runTableSql(TableMirror tblMirror, Environment environment) {
        Connection conn = null;
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable et = tblMirror.getEnvironmentTable(environment);

        rtn = runTableSql(et.getSql(), tblMirror, environment);

        return rtn;
    }

    public Boolean runTableSql(List<Pair> sqlList, TableMirror tblMirror, Environment environment) {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        // Check if there is anything to run.
        if (nonNull(sqlList) && !sqlList.isEmpty()) {
            // Check if the cluster is connected. This could happen if the cluster is a virtual cluster created as a place holder for processing.
            if (nonNull(config.getCluster(environment)) && nonNull(config.getCluster(environment).getHiveServer2()) &&
                    !config.getCluster(environment).getHiveServer2().isDisconnected()) {
                // Skip this if using test data.
                if (!config.isLoadingTestData()) {

                    try (Connection conn = getConnectionPoolService().getHS2EnvironmentConnection(environment)) {
                        if (isNull(conn) && config.isExecute() && !config.getCluster(environment).getHiveServer2().isDisconnected()) {
                            // this is a problem.
                            rtn = Boolean.FALSE;
                            tblMirror.addIssue(environment, "Connection missing. This is a bug.");
                        }

                        if (isNull(conn) && config.getCluster(environment).getHiveServer2().isDisconnected()) {
                            tblMirror.addIssue(environment, "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                                    "The scripts will need to be run 'manually'.");
                        }

                        if (rtn && nonNull(conn)) {
                            try (Statement stmt = conn.createStatement()) {
                                for (Pair pair : sqlList) {
                                    String action = pair.getAction();
                                    if (action.trim().isEmpty() || action.trim().startsWith("--")) {
                                        continue;
                                    } else {
                                        log.debug("{}:SQL:{}:{}", environment, pair.getDescription(), pair.getAction());
                                        tblMirror.setMigrationStageMessage("Executing SQL: " + pair.getDescription());
                                        if (config.isExecute()) {
                                            // Log the Return of 'set' commands.
                                            if (pair.getAction().trim().toLowerCase().startsWith("set")) {
                                                stmt.execute(pair.getAction());
                                                try {
                                                    // Check for a result set and print result if present.
                                                    ResultSet resultSet = stmt.getResultSet();
                                                    if (!isNull(resultSet)) {
                                                        while (resultSet.next()) {
                                                            tblMirror.addStep(environment.toString(), "Sql Run Complete for: " + pair.getDescription() + " : " + resultSet.getString(1));
                                                            log.info("{}:{}", pair.getAction(), resultSet.getString(1));
                                                        }
                                                    } else {
                                                        tblMirror.addStep(environment.toString(), "Sql Run Complete for: " + pair.getDescription());
                                                    }
                                                } catch (SQLException se) {
                                                    // Otherwise, just log command.
                                                    tblMirror.addStep(environment.toString(), "Sql Run Complete for: " + pair.getDescription());
                                                }
                                            } else {
                                                stmt.execute(pair.getAction());
                                                tblMirror.addStep(environment.toString(), "Sql Run Complete for: " + pair.getDescription());
                                            }
                                        } else {
                                            tblMirror.addStep(environment.toString(), "Sql Run SKIPPED (DRY-RUN) for: " + pair.getDescription());
                                        }
                                    }
                                }
                            } catch (SQLException throwables) {
                                log.error("{}:{}", environment.toString(), throwables.getMessage(), throwables);
                                String message = throwables.getMessage();
                                if (throwables.getMessage().contains("HiveAccessControlException Permission denied")) {
                                    message = message + " See [Hive SQL Exception / HDFS Permissions Issues](https://github.com/cloudera-labs/hms-mirror#hive-sql-exception--hdfs-permissions-issues)";
                                }
                                if (throwables.getMessage().contains("AvroSerdeException")) {
                                    message = message + ". It's possible the `avro.schema.url` referenced file doesn't exist at the target. " +
                                            "Use the `-asm` option and hms-mirror will attempt to copy it to the new cluster.";
                                }
                                tblMirror.getEnvironmentTable(environment).addError(message);
                                rtn = Boolean.FALSE;
                            }
                        }
                    } catch (SQLException throwables) {
                        tblMirror.getEnvironmentTable(environment).addError("Connecting: " + throwables.getMessage());
                        log.error("{}:{}", environment.toString(), throwables.getMessage(), throwables);
                        rtn = Boolean.FALSE;
                    }
                }
            }
        }
        return rtn;
    }
}
