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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

    // Assuming your logger is already defined, e.g.
    // private static final Logger log = LoggerFactory.getLogger(TableService.class);

    public TableService(
            ConfigService configService,
            ExecuteSessionService executeSessionService,
            ConnectionPoolService connectionPoolService,
            QueryDefinitionsService queryDefinitionsService,
            TranslatorService translatorService,
            StatsCalculatorService statsCalculatorService
    ) {
        log.debug("Initializing TableService with provided service dependencies");
        this.configService = configService;
        this.executeSessionService = executeSessionService;
        this.connectionPoolService = connectionPoolService;
        this.queryDefinitionsService = queryDefinitionsService;
        this.translatorService = translatorService;
        this.statsCalculatorService = statsCalculatorService;
    }

    /**
     * Checks the table filter.
     */
    protected void checkTableFilter(TableMirror tableMirror, Environment environment) {
        log.debug("Checking table filter for table: {} in environment: {}", tableMirror, environment);
        // ...existing logic...
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

        log.trace("Table filter checked for table: {}", tableMirror);
    }

    public String getCreateStatement(TableMirror tableMirror, Environment environment) {
        log.info("Getting CREATE statement for table: {} in environment: {}", tableMirror.getName(), environment);
        String createStatement = null;
        try {
            // ...existing logic to get the statement...
            StringBuilder createStatementBldr = new StringBuilder();
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
                    createStatementBldr.append(line);
                    if (iter.hasNext()) {
                        createStatementBldr.append("\n");
                    }
                }
            } else {
                log.error("Couldn't location definition for table: {} in environment: {}", tableMirror.getName(), environment.toString());
            }
            createStatement = createStatementBldr.toString();
            log.debug("Fetched CREATE statement for table {}: {}", tableMirror.getName(), createStatement);
            return createStatement;
        } catch (Exception e) {
            log.error("Failed to get CREATE statement for table: {}, environment: {}", tableMirror, environment, e);
            throw e;
        }
    }

    /**
     * Get the table definition for the given table mirror and environment.
     *
     * @param tableMirror The table mirror object.
     * @param environment The environment (LEFT or RIGHT).
     */
    public void getTableDefinition(TableMirror tableMirror, EnvironmentTable environmentTable, Environment environment) throws SQLException {
        final String tableId = String.format("%s:%s.%s",
                environment, tableMirror.getParent().getName(), tableMirror.getName());
        log.info("Fetching table definition for table: {} in environment: {}", tableMirror.getName(), environment);
//        EnvironmentTable environmentTable = null;
        log.info("Starting to get table definition for {}", tableId);
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
//            environmentTable = tableMirror.getEnvironmentTable(environment);
        // Fetch Table Definition
        if (config.isLoadingTestData()) {
            log.debug("Loading test data is enabled. Skipping schema load for {}", tableId);
        } else {
            log.debug("Loading schema from catalog for {}", tableId);
            loadSchemaFromCatalog(tableMirror, environment);
        }
        log.debug("Checking table filter for {}", tableId);
        checkTableFilter(tableMirror, environment);
        if (!tableMirror.isRemove() && !config.isLoadingTestData()) {
            log.debug("Table is not marked for removal. Proceeding with data strategy checks for {}", tableId);
            handleDataStrategy(config, tableMirror, environment, environmentTable, tableId);
        }
        Boolean partitioned = TableUtils.isPartitioned(environmentTable);
        if (environment == Environment.LEFT && partitioned
                && !tableMirror.isRemove() && !config.isLoadingTestData()) {
            log.debug("Table is partitioned. Checking metadata details for {}", tableId);
            if (config.loadMetadataDetails()) {
                log.debug("Loading partition metadata directly for {}", tableId);
                loadTablePartitionMetadataDirect(tableMirror, environment);
            }
        }
        Integer partLimit = config.getFilter().getTblPartitionLimit();
        if (partLimit != null && partLimit > 0) {
            log.debug("Checking partition count filter for {}", tableId);
            if (environmentTable.getPartitions().size() > partLimit) {
                log.info("Table partition count exceeds limit for {}. Limit: {}, Actual: {}",
                        tableId, partLimit, environmentTable.getPartitions().size());
                tableMirror.setRemove(Boolean.TRUE);
                tableMirror.setRemoveReason("The table partition count exceeds the specified table filter partition limit: " +
                        partLimit + " < " + environmentTable.getPartitions().size());
            }
        }
        log.info("Completed table definition for {}", tableId);
        log.debug("Successfully fetched table definition for table: {}", tableMirror);
    }

    private void handleDataStrategy(HmsMirrorConfig hmsMirrorConfig, TableMirror tableMirror, Environment environment,
                                    EnvironmentTable environmentTable, String tableId) {
        switch (hmsMirrorConfig.getDataStrategy()) {
            case SCHEMA_ONLY:
            case CONVERT_LINKED:
            case DUMP:
            case LINKED:
                log.debug("Data strategy {} does not require stats collection for {}", hmsMirrorConfig.getDataStrategy(), tableId);
                break;
            case SQL:
            case HYBRID:
            case EXPORT_IMPORT:
            case STORAGE_MIGRATION:
            case COMMON:
            case ACID:
                if (!TableUtils.isView(environmentTable) && TableUtils.isHiveNative(environmentTable)) {
                    log.debug("Collecting table stats for {}", tableId);
                    try {
                        loadTableStats(tableMirror, environment);
                    } catch (DisabledException e) {
                        log.warn("Stats collection is disabled. Skipping stats collection for {}", tableId);
                    } catch (RuntimeException rte) {
                        log.error("Error loading table stats for {}", tableId, rte);
                        tableMirror.addIssue(environment, rte.getMessage());
                    }
                }
                break;
        }
    }

    @Async("metadataThreadPool")
    public CompletableFuture<ReturnStatus> getTableMetadata(TableMirror tableMirror) {
        log.info("Fetching table metadata asynchronously for table: {}", tableMirror.getName());
        ReturnStatus rtn = new ReturnStatus();
        // Preset and overwrite the status when an issue or anomoly occurs.
        rtn.setStatus(ReturnStatus.Status.SUCCESS);
        return CompletableFuture.supplyAsync(() -> {
            try {
                // ...logic...
                rtn.setTableMirror(tableMirror);
                HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
                RunStatus runStatus = executeSessionService.getSession().getRunStatus();
                EnvironmentTable leftEnvTable = tableMirror.getEnvironmentTable(Environment.LEFT);
                try {
                    getTableDefinition(tableMirror, leftEnvTable, Environment.LEFT);
                    if (tableMirror.isRemove()) {
                        rtn.setStatus(ReturnStatus.Status.SKIP);
                        return rtn;
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
                                EnvironmentTable rightEnvTable = tableMirror.getEnvironmentTable(Environment.RIGHT);
                                try {
                                    getTableDefinition(tableMirror, rightEnvTable, Environment.RIGHT);
                                    rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
                                } catch (SQLException se) {
                                    // Can't find the table on the RIGHT.  This is OK if the table doesn't exist.
                                    log.debug("No table definition for {}:{}", tableMirror.getParent().getName(), tableMirror.getName(), se);
                                }
                        }
                    }
                } catch (SQLException throwables) {
                    // Check to see if the RIGHT exists.  This is for `--sync` mode.
                    // If it doesn't exist, then this is OK.
                    if (hmsMirrorConfig.isSync()) {
                        EnvironmentTable rightEnvTable = tableMirror.getEnvironmentTable(Environment.RIGHT);
                        try {
                            getTableDefinition(tableMirror, rightEnvTable, Environment.RIGHT);
                            rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
                        } catch (SQLException se) {
                            // OK, if the db doesn't exist yet.
                            handleSqlException(throwables, tableMirror, rightEnvTable, Environment.RIGHT);
                            rtn.setStatus(ReturnStatus.Status.ERROR);
                            rtn.setException(throwables);
                        }
                    } else {
                        handleSqlException(throwables, tableMirror, leftEnvTable, Environment.LEFT);
                        rtn.setStatus(ReturnStatus.Status.ERROR);
                        rtn.setException(throwables);
                    }
                }
                log.debug("Metadata fetch completed for table: {}", tableMirror);
                return rtn;
            } catch (Exception e) {
                log.error("Error occurred while fetching metadata for table: {}", tableMirror, e);
                rtn.setStatus(ReturnStatus.Status.ERROR);
                rtn.setException(e);
                return rtn;
            }
        });
    }

    @Async("metadataThreadPool")
    public CompletableFuture<ReturnStatus> getTables(DBMirror dbMirror) {
        log.info("Fetching tables asynchronously for DBMirror: {}", dbMirror.getName());
        return CompletableFuture.supplyAsync(() -> {
            ReturnStatus rtn = new ReturnStatus();
            try {
                // ...logic...
                ExecuteSession session = executeSessionService.getSession();
                HmsMirrorConfig config = session.getConfig();
                RunStatus runStatus = session.getRunStatus();
                log.debug("Getting tables for Database {}", dbMirror.getName());
                try {
                    getTables(dbMirror, Environment.LEFT);
                    if (config.isSync()) {
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
                log.debug("Tables fetch completed for DBMirror: {}", dbMirror);
                return rtn;
            } catch (Exception e) {
                log.error("Error occurred while fetching tables for DBMirror: {}", dbMirror.getName(), e);
                rtn.setStatus(ReturnStatus.Status.ERROR);
                rtn.setException(e);
                return rtn;
            }
        });
    }

    public void getTables(DBMirror dbMirror, Environment environment) throws SQLException {
        log.info("Fetching tables for DBMirror: {} in environment: {}", dbMirror.getName(), environment);
        Connection conn = null;
        String database = null;
        try {
            ExecuteSession session = executeSessionService.getSession();
            HmsMirrorConfig config = session.getConfig();

            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
            if (conn == null) {
                log.error("Unable to obtain a connection for environment: {}", environment);
                dbMirror.addIssue(environment, "No connection available for environment.");
                return;
            }

            database = (environment == Environment.LEFT)
                    ? dbMirror.getName()
                    : HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config);

            log.info("Loading tables for {}:{}", environment, database);

            List<String> showStatements = buildShowStatements(config, environment);

            try (Statement stmt = conn.createStatement()) { // try-with-resources for Statement
                setDatabaseContext(stmt, database);
                for (String show : showStatements) {
                    log.debug("Executing show statement: {}", show);
                    try (ResultSet rs = stmt.executeQuery(show)) { // try-with-resources for ResultSet
                        while (rs.next()) {
                            String tableName = rs.getString(1);
                            handleTableName(dbMirror, config, database, tableName);
                        }
                    }
                }
            }
            log.debug("Fetched tables for DBMirror: {}, environment: {}", dbMirror, environment);
        } catch (SQLException e) {
            log.error("SQLException while fetching tables for DBMirror: {}, environment: {}", dbMirror.getName(), environment, e);
            dbMirror.addIssue(environment, (database != null ? database : "unknown") + " " + e.getMessage());
            throw e;
        } finally {
            if (conn != null) try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }

    private List<String> buildShowStatements(HmsMirrorConfig config, Environment environment) {
        List<String> shows = new ArrayList<>();
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
        return shows;
    }

    private void setDatabaseContext(Statement stmt, String database) throws SQLException {
        stmt.execute(MessageFormat.format(MirrorConf.USE, database));
        log.debug("Set Hive DB Session Context to {}", database);
    }

    private void handleTableName(DBMirror dbMirror, HmsMirrorConfig config, String database, String tableName) {
        if (tableName == null) return;

        if (startsWithAny(tableName, config.getTransfer().getTransferPrefix(), config.getTransfer().getShadowPrefix())
                || endsWith(tableName, config.getTransfer().getStorageMigrationPostfix())) {
            markTableForRemoval(dbMirror, tableName, database, getRemovalReason(tableName, config));
            return;
        }

        Filter filter = config.getFilter();

        boolean addTable = false;
        if (filter == null || (isBlank(filter.getTblRegEx()) && isBlank(filter.getTblExcludeRegEx()))) {
            addTable = true;
        } else {
            if (!isBlank(filter.getTblRegEx())) {
                Matcher matcher = filter.getTblFilterPattern().matcher(tableName);
                addTable = matcher.matches();
            } else if (!isBlank(filter.getTblExcludeRegEx())) {
                Matcher matcher = filter.getTblExcludeFilterPattern().matcher(tableName);
                addTable = !matcher.matches();
            }
        }

        if (addTable) {
            // Add to DBMirror for processing.
            TableMirror tableMirror = dbMirror.addTable(tableName);
            String uniqueStr = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now());
            tableMirror.setUnique(uniqueStr);
            tableMirror.setMigrationStageMessage("Added to evaluation inventory");
            log.info("{}.{} added to processing list.", database, tableName);
        } else {
            log.info("{}.{} did not match filter and will NOT be added.", database, tableName);
        }
    }

    private static boolean startsWithAny(String value, String... prefixes) {
        for (String prefix : prefixes) {
            if (!isBlank(prefix) && value.startsWith(prefix)) return true;
        }
        return false;
    }

    private static boolean endsWith(String value, String postfix) {
        return !isBlank(postfix) && value.endsWith(postfix);
    }

    private void markTableForRemoval(DBMirror dbMirror, String tableName, String database, String reason) {
        TableMirror tableMirror = dbMirror.addTable(tableName);
        tableMirror.setRemove(true);
        tableMirror.setRemoveReason(reason);
        log.info("{}.{} was NOT added to list. Reason: {}", database, tableName, reason);
    }

    private String getRemovalReason(String tableName, HmsMirrorConfig config) {
        if (tableName.startsWith(config.getTransfer().getTransferPrefix())) {
            return "Table name matches the transfer prefix; likely remnant of a previous event.";
        } else if (tableName.startsWith(config.getTransfer().getShadowPrefix())) {
            return "Table name matches the shadow prefix; likely remnant of a previous event.";
        } else if (tableName.endsWith(config.getTransfer().getStorageMigrationPostfix())) {
            return "Table name matches the storage migration suffix; likely remnant of a previous event.";
        }
        return "Removed for unspecified reason";
    }

    private static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

// Refactored: Extracted helper methods, renamed vars, improved resource handling, modularized, reduced nesting

    private static final String OWNER_PREFIX = "owner";

    public void
    loadSchemaFromCatalog(TableMirror tableMirror, Environment environment) throws SQLException {
        log.info("Loading schema from catalog for table: {} in environment: {}", tableMirror.getName(), environment);
        // ...logic...
        String database = resolveDatabaseName(tableMirror, environment);
        EnvironmentTable environmentTable = tableMirror.getEnvironmentTable(environment);
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        try (Connection connection = getConnectionPoolService().getHS2EnvironmentConnection(environment)) {
            if (connection == null) return;

            try (Statement statement = connection.createStatement()) {
                useDatabase(statement, database);
                List<String> tableDefinition = fetchTableDefinition(statement, tableMirror, database, environment);
                environmentTable.setDefinition(tableDefinition);
                environmentTable.setName(tableMirror.getName());
                environmentTable.setExists(Boolean.TRUE);
                tableMirror.addStep(environment.toString(), "Fetched Schema");

                if (config.getOwnershipTransfer().isTable()) {
                    String owner = fetchTableOwner(statement, tableMirror, database, environment);
                    if (owner != null) {
                        environmentTable.setOwner(owner);
                    }
                }
            }
        }
        log.debug("Loaded schema from catalog for table: {}", tableMirror);
    }

    private String resolveDatabaseName(TableMirror tableMirror, Environment environment) {
        log.trace("Resolving database name for table: {} in environment: {}", tableMirror, environment);
        // ...logic...
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        if (environment == Environment.LEFT) {
            return tableMirror.getParent().getName();
        } else {
            return HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);
        }
    }

    private void useDatabase(Statement statement, String database) throws SQLException {
        log.trace("Executing USE database statement: {}", database);
        // ...logic...
        String useStatement = MessageFormat.format(MirrorConf.USE, database);
        statement.execute(useStatement);
    }

    private List<String> fetchTableDefinition(Statement statement, TableMirror tableMirror, String database, Environment environment) throws SQLException {
        log.debug("Fetching table definition for table: {} from database: {} in environment: {}", tableMirror.getName(), database, environment);
        // ...logic...
        String showStatement = MessageFormat.format(MirrorConf.SHOW_CREATE_TABLE, tableMirror.getName());
        List<String> tableDefinition = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery(showStatement)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            if (metaData.getColumnCount() >= 1) {
                while (resultSet.next()) {
                    try {
                        tableDefinition.add(resultSet.getString(1).trim());
                    } catch (NullPointerException npe) {
                        log.error("Loading Table Definition. Issue with SHOW CREATE TABLE resultset. " +
                                        "ResultSet record(line) is null. Skipping. {}:{}.{}",
                                environment, database, tableMirror.getName());
                    }
                }
            } else {
                log.error("Loading Table Definition. Issue with SHOW CREATE TABLE resultset. No Metadata. {}:{}.{}",
                        environment, database, tableMirror.getName());
            }
        }
        return tableDefinition;
    }

    private String fetchTableOwner(Statement statement, TableMirror tableMirror, String database, Environment environment) {
        log.debug("Fetching owner for table: {} in database: {}", tableMirror, database);
        // ...logic...
        String ownerStatement = MessageFormat.format(MirrorConf.SHOW_TABLE_EXTENDED, tableMirror.getName());
        try (ResultSet resultSet = statement.executeQuery(ownerStatement)) {
            while (resultSet.next()) {
                String value = resultSet.getString(1);
                if (value != null && value.startsWith(OWNER_PREFIX)) {
                    String[] ownerLine = value.split(":");
                    try {
                        return ownerLine[1];
                    } catch (Throwable t) {
                        log.error("Couldn't parse 'owner' value from: {} for table {}:{}.{}",
                                value, environment, database, tableMirror.getName());
                    }
                    break;
                }
            }
        } catch (SQLException ignored) {
            // Failed to gather owner details.
        }
        return null;
    }

    private void handleSqlException(SQLException exception, TableMirror tableMirror, EnvironmentTable environmentTable, Environment environment) {
        log.error("SQL Exception for table: {} in environmentTable: {}, environment: {}", tableMirror, environmentTable, environment, exception);
        // ...logic...
        String message = exception.getMessage();
        if (message.contains("Table not found") || message.contains("Database does not exist")) {
            tableMirror.addStep(environment.toString(), "No Schema");
        } else {
            log.error(message, exception);
            environmentTable.addError(message);
        }
    }

    protected void loadTableOwnership(TableMirror tableMirror, Environment environment) {
        log.info("Loading ownership information for table: {} in environment: {}", tableMirror, environment);
        // ...logic...
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
        log.info("Loading partition metadata for table: {}, environment: {}", tableMirror, environment);
        // ...logic...
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
