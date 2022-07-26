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

package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.hms.mirror.feature.Feature;
import com.cloudera.utils.hadoop.hms.mirror.feature.FeaturesEnum;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class TableMirror {
    private static final Logger LOG = LogManager.getLogger(TableMirror.class);

    private String dbName;
    private String name;
    private Date start = new Date();
    /*
    Use to indicate the tblMirror should be removed from processing, post setup.
     */
    @JsonIgnore
    private boolean remove = Boolean.FALSE;
    @JsonIgnore
    private String removeReason = null;
    @JsonIgnore
    private final String unique = UUID.randomUUID().toString().replaceAll("-", "");

    private final DateFormat tdf = new SimpleDateFormat("HH:mm:ss.SSS");
    @JsonIgnore
    private final List<Marker> steps = new ArrayList<Marker>();

    private DataStrategy strategy = null;

    // An ordinal value that we'll increment at each phase of the process
    private int currentPhase = 0;
    // An ordinal value, assign when we start processing, that indicates how many phase there will be.
    private int totalPhaseCount = 0;

    // Caption to help identify the current phase of the effort.
    @JsonIgnore
    private String migrationStageMessage = null;

    private PhaseState phaseState = PhaseState.INIT;

    @JsonIgnore
    private Long stageDuration = 0l;

    private final Map<Environment, EnvironmentTable> environments = new TreeMap<Environment, EnvironmentTable>();

    public String getName() {
        return name;
    }

    public String getName(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getName();
    }

    public String getUnique() {
        return unique;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public boolean isRemove() {
        return remove;
    }

    public void setRemove(boolean remove) {
        this.remove = remove;
    }

    public String getRemoveReason() {
        return removeReason;
    }

    public void setRemoveReason(String removeReason) {
        this.removeReason = removeReason;
    }

    public PhaseState getPhaseState() {
        return phaseState;
    }

    public void setPhaseState(PhaseState phaseState) {
        this.phaseState = phaseState;
    }

    public Long getStageDuration() {
        return stageDuration;
    }

    public void setStageDuration(Long stageDuration) {
        this.stageDuration = stageDuration;
    }

    public TableMirror(String dbName, String tablename) {
        this.dbName = dbName;
        this.name = tablename;
        addStep("init", null);
    }

    public DataStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(DataStrategy strategy) {
        this.strategy = strategy;
    }

    public void incPhase() {
        currentPhase += 1;
        if (currentPhase >= totalPhaseCount) {
            totalPhaseCount = currentPhase + 1;
        }
    }

    public Map<Environment, EnvironmentTable> getEnvironments() {
        return environments;
    }

    public String getProgressIndicator(int width, int scale) {
        StringBuilder sb = new StringBuilder();
        int progressLength = (width / scale) * currentPhase;
        sb.append("\u001B[32m");
        sb.append(StringUtils.rightPad("=", progressLength - 1, "="));
        sb.append("\u001B[33m");
        sb.append(StringUtils.rightPad("-", width - progressLength, "-"));
        sb.append("\u001B[0m|");
        return sb.toString();
    }

    public void processingDone() {
        totalPhaseCount = currentPhase;
        // Clear message
        setMigrationStageMessage(null);
    }

    public void nextPhase(String msg) {
        incPhase();
        setMigrationStageMessage(msg);
    }

    public String getMigrationStageMessage() {
        return migrationStageMessage;
    }

    public void setMigrationStageMessage(String migrationStageMessage) {
        this.migrationStageMessage = migrationStageMessage;
        incPhase();
    }

    public void addStep(String key, Object value) {
        Date now = new Date();
        Long elapsed = now.getTime() - start.getTime();
        start = now; // reset
        BigDecimal secs = new BigDecimal(elapsed).divide(new BigDecimal(1000));///1000
        DecimalFormat decf = new DecimalFormat("#,###.00");
        String secStr = decf.format(secs);
        steps.add(new Marker(secStr, key, value));
    }

    public List<Marker> getSteps() {
        return steps;
    }

    public EnvironmentTable getEnvironmentTable(Environment environment) {
        EnvironmentTable et = environments.get(environment);
        if (et == null) {
            et = new EnvironmentTable();
            environments.put(environment, et);
        }
        return et;
    }

    @JsonIgnore
    public boolean isThereAnIssue(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getIssues().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean isThereSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getSql().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean isThereCleanupSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getCleanUpSql().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean whereTherePropsAdded(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getAddProperties().size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public List<String> getIssues(Environment environment) {
        return getEnvironmentTable(environment).getIssues();
    }

    public Map<String, String> getPropAdd(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getAddProperties();
    }

    public List<Pair> getCleanUpSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getCleanUpSql();
    }

    public List<Pair> getSql(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getSql();
    }

    public void addIssue(Environment environment, String issue) {
        if (issue != null) {
            String scrubbedIssue = issue.replace("\n", "<br/>");
            getIssues(environment).add(scrubbedIssue);
        }
    }

    public Boolean hasIssues() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : environments.entrySet()) {
            if (entry.getValue().getIssues().size() > 0)
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean hasActions() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : environments.entrySet()) {
            if (entry.getValue().getActions().size() > 0)
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean hasAddedProperties() {
        Boolean rtn = Boolean.FALSE;
        for (Map.Entry<Environment, EnvironmentTable> entry : environments.entrySet()) {
            if (entry.getValue().getAddProperties().size() > 0)
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean buildoutDUMPDefinition(Config config, DBMirror dbMirror) {
//        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout DUMP Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        // Standardize the LEFT def.
        // Remove DB from CREATE
        TableUtils.stripDatabase(let.getName(), let.getDefinition());

        // If not legacy, remove location from ACID tables.
        if (!config.getCluster(Environment.LEFT).getLegacyHive() &&
                TableUtils.isACID(let.getName(), let.getDefinition())) {
            TableUtils.stripLocation(let.getName(), let.getDefinition());
        }
        return Boolean.TRUE;
    }

    public Boolean buildoutSCHEMA_ONLYDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SCHEMA_ONLY Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Swap out the namespace of the LEFT with the RIGHT.
        copySpec.setReplaceLocation(Boolean.TRUE);
        if (config.convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (!config.isReadOnly() || !config.isSync()) {
            if ((TableUtils.isExternal(let) && config.getCluster(Environment.LEFT).getLegacyHive()) ||
                    // Don't add purge for non-legacy environments...
                    // https://github.com/cloudera-labs/hms-mirror/issues/5
                    (TableUtils.isExternal(let) && !config.getCluster(Environment.LEFT).getLegacyHive())) {
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                copySpec.setTakeOwnership(Boolean.TRUE);
            }
        } else if (copySpec.getUpgrade()) {
            ret.addIssue("Ownership (PURGE Option) not set because of either: `sync` or `ro|read-only` was specified in the config.");
        }
        if (config.isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }

        if (config.isSync()) {
            // We assume that the 'definitions' are only there is the
            //     table exists.
            if (!let.getExists() && ret.getExists()) {
                // If left is empty and right is not, DROP RIGHT.
                ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                ret.setCreateStrategy(CreateStrategy.DROP);
            } else if (let.getExists() && !ret.getExists()) {
                // If left is defined and right is not, CREATE RIGHT.
                ret.addIssue("Schema missing, will be CREATED");
                ret.setCreateStrategy(CreateStrategy.CREATE);
            } else if (let.getExists() && ret.getExists()) {
                // If left and right, check schema change and replace if necessary.
                // Compare Schemas.
                if (schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                    ret.addIssue("Schema exists AND matches.  No Actions Necessary.");
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                } else {
                    if (TableUtils.isExternalPurge(ret)) {
                        ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                "We can NOT safely replace the table without compromising the data. No action will be taken.");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                        return Boolean.FALSE;
                    } else {
                        ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                        ret.setCreateStrategy(CreateStrategy.REPLACE);
                    }
                }
            }
            copySpec.setTakeOwnership(Boolean.FALSE);
        } else {
            if (ret.getExists()) {
                if (TableUtils.isView(ret)) {
                    ret.addIssue("View exists already.  Will REPLACE.");
                    ret.setCreateStrategy(CreateStrategy.REPLACE);
                } else {
                    // Already exists, no action.
                    ret.addIssue("Schema exists already, no action. If you wish to rebuild the schema, " +
                            "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                    return Boolean.FALSE;
                }
            } else {
                ret.addIssue("Schema will be created");
                ret.setCreateStrategy(CreateStrategy.CREATE);
            }
        }

        // For ACID tables, we need to remove the location.
        // Hive 3 doesn't allow this to be set via SQL Create.
        if (TableUtils.isACID(let)) {
            copySpec.setStripLocation(Boolean.TRUE);
        }

        // Rebuild Target from Source.
        if (!TableUtils.isACID(let) || (TableUtils.isACID(let) && config.getMigrateACID().isOn())) {
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue(TableUtils.ACID_NOT_ON);
            ret.setCreateStrategy(CreateStrategy.NOTHING);
            rtn = Boolean.FALSE;
        }

        // If not legacy, remove location from ACID tables.
        if (rtn && !config.getCluster(Environment.LEFT).getLegacyHive() &&
                TableUtils.isACID(let.getName(), let.getDefinition())) {
            TableUtils.stripLocation(let.getName(), let.getDefinition());
        }
        return rtn;
    }

    public Boolean buildoutLINKEDDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout LINKED Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
            // Swap out the namespace of the LEFT with the RIGHT.
            copySpec.setReplaceLocation(Boolean.FALSE);
            if (config.convertManaged())
                copySpec.setUpgrade(Boolean.TRUE);
            // LINKED doesn't own the data.
            copySpec.setTakeOwnership(Boolean.FALSE);

            if (config.isSync()) {
                // We assume that the 'definitions' are only there is the
                //     table exists.
                if (!let.getExists() && ret.getExists()) {
                    // If left is empty and right is not, DROP RIGHT.
                    ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else if (let.getExists() && !ret.getExists()) {
                    // If left is defined and right is not, CREATE RIGHT.
                    ret.addIssue("Schema missing, will be CREATED");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else if (let.getExists() && ret.getExists()) {
                    // If left and right, check schema change and replace if necessary.
                    // Compare Schemas.
                    if (schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                        ret.addIssue("Schema exists AND matches.  No Actions Necessary.");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                    } else {
                        if (TableUtils.isExternalPurge(ret)) {
                            ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                    "We can NOT safely replace the table without compromising the data. No action will be taken.");
                            ret.setCreateStrategy(CreateStrategy.LEAVE);
                            return Boolean.FALSE;
                        } else {
                            ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                            ret.setCreateStrategy(CreateStrategy.REPLACE);
                        }
                    }
                }
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                if (ret.getExists()) {
                    // Already exists, no action.
                    ret.addIssue("Schema exists already, no action. If you wish to rebuild the schema, " +
                            "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                    return Boolean.FALSE;
                } else {
                    ret.addIssue("Schema will be created");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                }
            }
            // Rebuild Target from Source.
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue("Can't LINK ACID tables");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
        }
        return rtn;
    }

    public Boolean buildoutLINKEDSql(Config config, DBMirror dbMirror) {
        return buildoutSCHEMA_ONLYSql(config, dbMirror);
    }

    public Boolean buildoutCOMMONDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout COMMON Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Can't LINK ACID tables.
        if (TableUtils.isHiveNative(let) && !TableUtils.isACID(let)) {
            // Swap out the namespace of the LEFT with the RIGHT.
            copySpec.setReplaceLocation(Boolean.FALSE);
            if (config.convertManaged())
                copySpec.setUpgrade(Boolean.TRUE);
            // COMMON owns the data unless readonly specified.
            if (!config.isReadOnly())
                copySpec.setTakeOwnership(Boolean.TRUE);

            if (config.isSync()) {
                // We assume that the 'definitions' are only there if the
                //     table exists.
                if (!let.getExists() && ret.getExists()) {
                    // If left is empty and right is not, DROP RIGHT.
                    ret.addIssue("Schema doesn't exist in 'source'.  Will be DROPPED.");
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else if (let.getExists() && !ret.getExists()) {
                    // If left is defined and right is not, CREATE RIGHT.
                    ret.addIssue("Schema missing, will be CREATED");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else if (let.getExists() && ret.getExists()) {
                    // If left and right, check schema change and replace if necessary.
                    // Compare Schemas.
                    if (schemasEqual(Environment.LEFT, Environment.RIGHT)) {
                        ret.addIssue("Schema exists AND matches.  No Actions Necessary.");
                        ret.setCreateStrategy(CreateStrategy.LEAVE);
                    } else {
                        if (TableUtils.isExternalPurge(ret)) {
                            ret.addIssue("Schema exists AND DOESN'T match.  But the 'RIGHT' table is has a PURGE option set. " +
                                    "We can NOT safely replace the table without compromising the data. No action will be taken.");
                            ret.setCreateStrategy(CreateStrategy.LEAVE);
                            return Boolean.FALSE;
                        } else {
                            ret.addIssue("Schema exists AND DOESN'T match.  It will be REPLACED (DROPPED and RECREATED).");
                            ret.setCreateStrategy(CreateStrategy.REPLACE);
                        }
                    }
                }
                // With sync, don't own data.
                copySpec.setTakeOwnership(Boolean.FALSE);
            } else {
                if (ret.getExists()) {
                    // Already exists, no action.
                    ret.addIssue("Schema exists already, no action. If you wish to rebuild the schema, " +
                            "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                    return Boolean.FALSE;
                } else {
                    ret.addIssue("Schema will be created");
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                }
            }
            // Rebuild Target from Source.
            rtn = buildTableSchema(copySpec);
        } else {
            let.addIssue("Can't use COMMON for ACID tables");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
        }
        return rtn;
    }

    /*
    In this case for the inplace downgrade, the 'left' has already been renamed to an
    'archive' and the 'right' table is the new definition (but will be played on the left).
    We're using the 'right' as a placeholder in this case, since we're not using a RIGHT
    environment.
     */
    public Boolean buildoutSQLACIDDowngradeInplaceSQL(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        // Check to see if there are partitions.
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        // Ensure we're in the right database.
        String database = dbMirror.getName();
        String useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);

        if (let.getPartitioned()) {
            if (false) {
                // TODO: Check for Optimization Settings.
            } else {
                // Prescriptive Optimization.
                String partElement = TableUtils.getPartitionElements(let);
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                        let.getName(), ret.getName(), partElement);
                String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
                let.addSql(new Pair(transferDesc, transferSql));
            }
        } else {
            // Simple FROM .. INSERT OVERWRITE ... SELECT *;
            String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE, let.getName(), ret.getName());
            let.addSql(new Pair(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, transferSql));
        }

        rtn = Boolean.TRUE;

        return rtn;
    }

    public Boolean buildoutSQLACIDDowngradeInplaceDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        // Use db
        String useDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
        let.addSql(TableUtils.USE_DESC, useDb);

        // Build Right (to be used as new table on left).
        CopySpec leftNewTableSpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        leftNewTableSpec.setTakeOwnership(Boolean.TRUE);
        leftNewTableSpec.setMakeExternal(Boolean.TRUE);
        // Location of converted data will got to default location.
        leftNewTableSpec.setStripLocation(Boolean.TRUE);

        rtn = buildTableSchema(leftNewTableSpec);

        String origTableName = let.getName();

        // Rename Original Table
        String newTblName = let.getName() + "_archive";
        String renameSql = MessageFormat.format(MirrorConf.RENAME_TABLE, origTableName, newTblName);
        TableUtils.changeTableName(let, newTblName);
        let.addSql(TableUtils.RENAME_TABLE, renameSql);

        // Check Buckets and Strip.
        int buckets = TableUtils.numOfBuckets(ret);
        if (buckets > 0 && buckets <= config.getMigrateACID().getArtificialBucketThreshold()) {
            // Strip bucket definition.
            if (TableUtils.removeBuckets(ret, config.getMigrateACID().getArtificialBucketThreshold())) {
                let.addIssue("Bucket Definition removed (was " + TableUtils.numOfBuckets(ret) + ") because it was EQUAL TO or BELOW " +
                        "the configured 'artificialBucketThreshold' of " +
                        config.getMigrateACID().getArtificialBucketThreshold());
            }

        }
        // Create New Table.
        String newCreateTable = this.getCreateStatement(Environment.RIGHT);
        let.addSql(TableUtils.CREATE_DESC, newCreateTable);

        rtn = Boolean.TRUE;

        return rtn;
    }

    /*
    The SQL Strategy uses LINKED clusters and is only valid against Legacy Managed and EXTERNAL
    tables.  NO ACID tables.

    - We create the same schema in the 'target' cluster for the TARGET.
    - We need the create and LINKED a shadow table to the LOWER clusters data.

     */
    public Boolean buildoutSQLDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SQL Definition");


        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        EnvironmentTable set = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        // Different transfer technique.  Staging location.
        if (config.getTransfer().getIntermediateStorage() != null ||
                config.getTransfer().getCommonStorage() != null ||
                TableUtils.isACID(let)) {
            return buildoutIntermediateDefinition(config, dbMirror);
        }

        if (config.isSync()) {
            let.addIssue("Sync NOT supported in the scenario.");
            return Boolean.FALSE;
        }

        if (config.getTransfer().getCommonStorage() == null) {
            CopySpec shadowSpec = null;

            // Create a 'shadow' table definition on right cluster pointing to the left data.
            shadowSpec = new CopySpec(config, Environment.LEFT, Environment.SHADOW);

            if (config.convertManaged())
                shadowSpec.setUpgrade(Boolean.TRUE);

            // Don't claim data.  It will be in the LEFT cluster, so the LEFT owns it.
            shadowSpec.setTakeOwnership(Boolean.FALSE);

            // Create table with alter name in RIGHT cluster.
            shadowSpec.setTableNamePrefix(config.getTransfer().getShadowPrefix());

            if (ret.getExists()) {
                // Already exists, no action.
                ret.addIssue("Schema exists already, no action.  If you wish to rebuild the schema, " +
                        "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
                ret.setCreateStrategy(CreateStrategy.LEAVE);
                return Boolean.FALSE;
            } else {
                ret.addIssue("Schema will be created");
                ret.setCreateStrategy(CreateStrategy.CREATE);
            }

            // Build Shadow from Source.
            rtn = buildTableSchema(shadowSpec);
        }

        // Create final table in right.
        CopySpec rightSpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);

        // Swap out the namespace of the LEFT with the RIGHT.
        rightSpec.setReplaceLocation(Boolean.TRUE);
        if (TableUtils.isManaged(let) && config.convertManaged()) {
            rightSpec.setUpgrade(Boolean.TRUE);
        } else {
            rightSpec.setMakeExternal(Boolean.TRUE);
        }
        if (config.isReadOnly()) {
            rightSpec.setTakeOwnership(Boolean.FALSE);
        } else if (TableUtils.isManaged(let)) {
            rightSpec.setTakeOwnership(Boolean.TRUE);
        }

        // Rebuild Target from Source.
        rtn = buildTableSchema(rightSpec);

        return rtn;
    }

    /*
     */
    public Boolean buildoutEXPORT_IMPORTDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout EXPORT_IMPORT Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        if (TableUtils.isACID(let) && !config.getCluster(Environment.LEFT).getLegacyHive().equals(config.getCluster(Environment.RIGHT).getLegacyHive())) {
            let.addIssue("Can't process ACID tables with EXPORT_IMPORT between 'legacy' and 'non-legacy' hive environments.  The processes aren't compatible.");
            return Boolean.FALSE;
        }

        if (!TableUtils.isHiveNative(let)) {
            let.addIssue("Can't process ACID tables, VIEWs, or Non Native Hive Tables with this strategy.");
            return Boolean.FALSE;
        }

        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);
        // Swap out the namespace of the LEFT with the RIGHT.
        copySpec.setReplaceLocation(Boolean.TRUE);
        if (config.convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (!config.isReadOnly() || !config.isSync()) {
            copySpec.setTakeOwnership(Boolean.TRUE);
        }
        if (config.isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }

        if (ret.getExists()) {
            // Already exists, no action.
            ret.addIssue("Schema exists already, no action.  If you wish to rebuild the schema, " +
                    "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
            ret.setCreateStrategy(CreateStrategy.LEAVE);
        } else {
            ret.addIssue("Schema will be created");
            ret.setCreateStrategy(CreateStrategy.CREATE);
            rtn = Boolean.TRUE;
        }

        if (rtn)
            // Build Target from Source.
            rtn = buildTableSchema(copySpec);

        return rtn;
    }

    /*
     */
    public Boolean buildoutIntermediateDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout Intermediate Definition");
        EnvironmentTable let = null;
        EnvironmentTable ret = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        CopySpec rightSpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);

        if (ret.getExists()) {
            // Already exists, no action.
            ret.addIssue("Schema exists already.  Can't do ACID transfer if schema already exists. Drop it and " +
                    "try again.");
            ret.setCreateStrategy(CreateStrategy.NOTHING);
            return Boolean.FALSE;
        } else {
            ret.setCreateStrategy(CreateStrategy.CREATE);
        }

        if (!TableUtils.isACID(let) && TableUtils.isManaged(let)) {
            // Managed to EXTERNAL
            rightSpec.setUpgrade(Boolean.TRUE);
            rightSpec.setReplaceLocation(Boolean.TRUE);
        } else if (TableUtils.isACID(let)) {
            // ACID
            if (config.getMigrateACID().isDowngrade()) {
                if (config.getTransfer().getCommonStorage() == null) {
                    if (config.getTransfer().getStorageMigration().isDistcp()) {
                        rightSpec.setReplaceLocation(Boolean.TRUE);
                    } else {
                        rightSpec.setStripLocation(Boolean.TRUE);
                    }
                } else {
                    rightSpec.setReplaceLocation(Boolean.TRUE);
                }
                rightSpec.setMakeExternal(Boolean.TRUE);
                // Strip the Transactional Elements
                rightSpec.setMakeNonTransactional(Boolean.TRUE);
                // Set Purge Flag
                rightSpec.setTakeOwnership(Boolean.TRUE);
            } else {
                // Use the system default location when converting.
                rightSpec.setStripLocation(Boolean.TRUE);
            }
        } else {
            // External
            rightSpec.setReplaceLocation(Boolean.TRUE);
        }

        // Build Target from Source.
        rtn = buildTableSchema(rightSpec);

        // Build Transfer Spec.
        CopySpec transferSpec = new CopySpec(config, Environment.LEFT, Environment.TRANSFER);
        if (config.getTransfer().getCommonStorage() == null) {
            if (config.getCluster(Environment.LEFT).getLegacyHive() != config.getCluster(Environment.RIGHT).getLegacyHive()) {
                if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                    transferSpec.setMakeNonTransactional(Boolean.TRUE);
                } else {
                    // Don't support non-legacy to legacy conversions.
                    let.addIssue("Don't support Non-Legacy to Legacy conversions.");
                    return Boolean.FALSE;
                }
            } else {
                if (config.getCluster(Environment.LEFT).getLegacyHive()) {
                    // Legacy to Legacy
                    // Non-Transactional, Managed (for ownership)
                    transferSpec.setMakeNonTransactional(Boolean.TRUE);
                } else {
                    // Non Legacy to Non Legacy
                    // External w/ Ownership
                    transferSpec.setMakeExternal(Boolean.TRUE);
                    transferSpec.setTakeOwnership(Boolean.TRUE);
                }
            }
        } else {
            // Storage will be used by right.  So don't let Transfer table own data.
            transferSpec.setMakeNonTransactional(Boolean.TRUE);
            if (config.getCluster(Environment.LEFT).getLegacyHive() != config.getCluster(Environment.RIGHT).getLegacyHive()) {
                if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                    // Don't support non-legacy to legacy conversions.
                    let.addIssue("Don't support Non-Legacy to Legacy conversions.");
                    return Boolean.FALSE;
                } else {
                    if (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade()) {
                        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                            transferSpec.setMakeExternal(Boolean.TRUE);
                            transferSpec.setTakeOwnership(Boolean.FALSE);
                        } else {
                            transferSpec.setMakeExternal(Boolean.TRUE);
                        }
                    } else {
                        transferSpec.setMakeExternal(Boolean.TRUE);
                    }
                }
            } else {
                if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
                    // Non Legacy to Non Legacy
                    // External w/ Ownership
                    transferSpec.setMakeExternal(Boolean.TRUE);
                    if (config.getMigrateACID().isDowngrade()) {
                        // The location will be used by the right cluster, so do not own the data.
                        transferSpec.setTakeOwnership(Boolean.FALSE);
                    } else {
                        // Common Storage is used just like Intermediate Storage in this case.
                        transferSpec.setTakeOwnership(Boolean.TRUE);
                    }
                }
            }
        }

        transferSpec.setTableNamePrefix(config.getTransfer().getTransferPrefix());
        transferSpec.setReplaceLocation(Boolean.TRUE);

        if (rtn)
            // Build transfer table.
            rtn = buildTableSchema(transferSpec);

        // Build Shadow Spec (don't build when using commonStorage)
        // If acid and ma.isOn
        // if not downgrade
        if ((config.getTransfer().getIntermediateStorage() != null && !TableUtils.isACID(let)) ||
                (TableUtils.isACID(let) && config.getMigrateACID().isOn())) {
            if (!config.getMigrateACID().isDowngrade() ||
                    // Is Downgrade but the downgraded location isn't available to the right.
                    (config.getMigrateACID().isDowngrade() && config.getTransfer().getCommonStorage() == null)) {
                if (!config.getTransfer().getStorageMigration().isDistcp()) { // ||
//                (config.getMigrateACID().isOn() && TableUtils.isACID(let)
//                        && !config.getMigrateACID().isDowngrade())) {
                    CopySpec shadowSpec = new CopySpec(config, Environment.LEFT, Environment.SHADOW);
                    shadowSpec.setUpgrade(Boolean.TRUE);
                    shadowSpec.setMakeExternal(Boolean.TRUE);
                    shadowSpec.setTakeOwnership(Boolean.FALSE);
                    shadowSpec.setReplaceLocation(Boolean.TRUE);
                    shadowSpec.setTableNamePrefix(config.getTransfer().getShadowPrefix());

                    if (rtn)
                        rtn = buildTableSchema(shadowSpec);
                }
            }
        }
        return rtn;
    }


    /*
    TODO: buildoutHYBRIDDefinition
     */
    private Boolean buildoutHYBRIDDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout HYBRID Definition");
        EnvironmentTable let = null;

        let = getEnvironmentTable(Environment.LEFT);

        if (TableUtils.isACID(let.getName(), let.getDefinition())) {
            if (config.getMigrateACID().isOn()) {
                rtn = buildoutIntermediateDefinition(config, dbMirror);
            } else {
                let.addIssue(TableUtils.ACID_NOT_ON);
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit()) {
                    rtn = buildoutSQLDefinition(config, dbMirror);
                } else {
                    rtn = buildoutEXPORT_IMPORTDefinition(config, dbMirror);
                }

            }
        }

        return rtn;
    }

    public Boolean buildoutDUMPSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout DUMP SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        let.getSql().clear();
        useDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
        let.addSql(TableUtils.USE_DESC, useDb);

        createTbl = this.getCreateStatement(Environment.LEFT);
        let.addSql(TableUtils.CREATE_DESC, createTbl);

        // If partitioned, !ACID, repair
        if (let.getPartitioned() && !TableUtils.isACID(let) &&
                config.getCluster(Environment.LEFT).getPartitionDiscovery().getInitMSCK()) {
            String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, dbMirror.getName(), let.getName());
            let.addSql(TableUtils.REPAIR_DESC, msckStmt);
        }

        rtn = Boolean.TRUE;

        return rtn;

    }

    public Boolean buildoutSCHEMA_ONLYSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SCHEMA_ONLY SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        //ret.getSql().clear();

        database = config.getResolvedDB(dbMirror.getName());
        useDb = MessageFormat.format(MirrorConf.USE, database);

        switch (ret.getCreateStrategy()) {
            case NOTHING:
            case LEAVE:
                // Do Nothing.
                break;
            case DROP:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                break;
            case REPLACE:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String dropStmt2 = null;
                if (TableUtils.isView(ret)) {
                    dropStmt2 = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                } else {
                    dropStmt2 = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                }
                ret.addSql(TableUtils.DROP_DESC, dropStmt2);
                String createStmt = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                ret.addSql(TableUtils.USE_DESC, useDb);
                String createStmt2 = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                break;
        }

        // If partitioned, !ACID, repair
        if (let.getPartitioned() && !TableUtils.isACID(let) &&
                config.getCluster(Environment.RIGHT).getPartitionDiscovery().getInitMSCK() &&
                (ret.getCreateStrategy() == CreateStrategy.REPLACE || ret.getCreateStrategy() == CreateStrategy.CREATE)) {
            String msckStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, ret.getName());
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                ret.addCleanUpSql(TableUtils.REPAIR_DESC, msckStmt);
            } else {
                ret.addSql(TableUtils.REPAIR_DESC, msckStmt);
            }
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    /*
    We'll only work with the LEFT cluster in this configuration.  But we will use the RIGHT clusters object to store
    the new information.

     */
    public Boolean buildoutSTORAGEMIGRATIONDefinition(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SQL Definition");

        // Different transfer technique.  Staging location.
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        EnvironmentTable set = null;
        CopySpec copySpec = null;

        let = getEnvironmentTable(Environment.LEFT);
        ret = getEnvironmentTable(Environment.RIGHT);

        if (TableUtils.getLocation(let.getName(), let.getDefinition()).startsWith(config.getTransfer().getCommonStorage())) {
            addIssue(Environment.LEFT, "Table namespace already matches target MIGRATION namespace.  Nothing to do.");
            return Boolean.FALSE;
        }

        // Create a 'target' table definition on left cluster with right definition (used only as place holder)
        copySpec = new CopySpec(config, Environment.LEFT, Environment.RIGHT);

        if (TableUtils.isACID(let)) {
            copySpec.setStripLocation(Boolean.TRUE);
            if (config.getMigrateACID().isDowngrade()) {
                copySpec.setMakeExternal(Boolean.TRUE);
                copySpec.setTakeOwnership(Boolean.TRUE);
            }
        } else {
            copySpec.setReplaceLocation(Boolean.TRUE);
        }

        // Build Shadow from Source.
        rtn = buildTableSchema(copySpec);

        return rtn;
    }

    public Boolean buildoutSQLSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout SQL SQL");

        if (config.getTransfer().getIntermediateStorage() != null ||
                config.getTransfer().getCommonStorage() != null) {
            return buildoutIntermediateSql(config, dbMirror);
        }

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        if (TableUtils.isACID(let)) {
            rtn = Boolean.FALSE;
            // TODO: Hum... Not sure this is right.
            addIssue(Environment.LEFT, "Shouldn't get an ACID table here.");
        } else {
//        if (!isACIDDowngradeInPlace(config, let)) {
            database = config.getResolvedDB(dbMirror.getName());
            useDb = MessageFormat.format(MirrorConf.USE, database);

            ret.addSql(TableUtils.USE_DESC, useDb);

            String dropStmt = null;
            // Create RIGHT Shadow Table
            if (set.getDefinition().size() > 0) {
                // Drop any previous SHADOW table, if it exists.
                dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);

                String shadowCreateStmt = getCreateStatement(Environment.SHADOW);
                ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
                // Repair Partitions
//                if (let.getPartitioned()) {
//                    String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
//                    ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
//                }
            }

            // RIGHT Final Table
            switch (ret.getCreateStrategy()) {
                case NOTHING:
                case LEAVE:
                    // Do Nothing.
                    break;
                case DROP:
                    dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, dropStmt);
                    break;
                case REPLACE:
                    dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, dropStmt);
                    String createStmt = getCreateStatement(Environment.RIGHT);
                    ret.addSql(TableUtils.CREATE_DESC, createStmt);
                    break;
                case CREATE:
                    String createStmt2 = getCreateStatement(Environment.RIGHT);
                    ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                    break;
            }

            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean buildoutSTORAGEMIGRATIONSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout STORAGE_MIGRATION SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        let.getSql().clear();
        ret.getSql().clear();


        database = dbMirror.getName();
        useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);

        // Alter the current table and rename.
        let.setName(let.getName() + "_" + getUnique());
        String origAlterRename = MessageFormat.format(MirrorConf.RENAME_TABLE, ret.getName(), let.getName());
        let.addSql(MirrorConf.RENAME_TABLE_DESC, origAlterRename);

        // Create table with New Location
        String createStmt2 = getCreateStatement(Environment.RIGHT);
        let.addSql(TableUtils.CREATE_DESC, createStmt2);

        // Drop Renamed Table.
        String dropOriginalTable = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
        let.addCleanUpSql(MirrorConf.DROP_TABLE_DESC, dropOriginalTable);

        rtn = Boolean.TRUE;
        return rtn;
    }

    public Boolean buildoutIntermediateSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Table: " + dbMirror.getName() + " buildout Intermediate SQL");

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        // LEFT Transfer Table
        database = dbMirror.getName();
        useDb = MessageFormat.format(MirrorConf.USE, database);

        let.addSql(TableUtils.USE_DESC, useDb);
        // Drop any previous TRANSFER table, if it exists.
        String transferDropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, tet.getName());
        let.addSql(TableUtils.DROP_DESC, transferDropStmt);

        // Create Transfer Table
        String transferCreateStmt = getCreateStatement(Environment.TRANSFER);
        let.addSql(TableUtils.CREATE_TRANSFER_DESC, transferCreateStmt);

        database = config.getResolvedDB(dbMirror.getName());
        useDb = MessageFormat.format(MirrorConf.USE, database);
        ret.addSql(TableUtils.USE_DESC, useDb);

        // RIGHT SHADOW Table
        if (set.getDefinition().size() > 0) { //config.getTransfer().getCommonStorage() == null || !config.getMigrateACID().isDowngrade()) {

            // Drop any previous SHADOW table, if it exists.
            String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
            ret.addSql(TableUtils.DROP_DESC, dropStmt);
            // Create Shadow Table
            String shadowCreateStmt = getCreateStatement(Environment.SHADOW);
            ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
            // Repair Partitions
//            if (let.getPartitioned()) {
//                String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
//                ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
//            }
        }

        // RIGHT Final Table
        String rightDrop = null;
        switch (ret.getCreateStrategy()) {
            case NOTHING:
            case LEAVE:
                // Do Nothing.
                break;
            case DROP:
                rightDrop = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, rightDrop);
                break;
            case REPLACE:
                rightDrop = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(TableUtils.DROP_DESC, rightDrop);
                String createStmt = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt);
                break;
            case CREATE:
                String createStmt2 = getCreateStatement(Environment.RIGHT);
                ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                if (let.getPartitioned()) {
                    if (config.getTransfer().getCommonStorage() != null) {
                        if (!TableUtils.isACID(let) || (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade())) {
                            String rightMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, ret.getName());
                            ret.addSql(TableUtils.REPAIR_DESC, rightMSCKStmt);
                        }
                    }
                }

                break;
        }

        rtn = Boolean.TRUE;
        return rtn;
    }

    public Boolean isACIDDowngradeInPlace(Config config, EnvironmentTable tbl) {
        if (TableUtils.isACID(tbl) && config.getMigrateACID().isDowngradeInPlace()) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public Boolean buildoutEXPORT_IMPORTSql(Config config, DBMirror dbMirror) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Database: " + dbMirror.getName() + " buildout EXPORT_IMPORT SQL");

        String database = null;

        database = config.getResolvedDB(dbMirror.getName());

        EnvironmentTable let = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT);

        // LEFT Export to directory
        String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
        let.addSql(TableUtils.USE_DESC, useLeftDb);
        String exportLoc = null;

        if (config.getTransfer().getIntermediateStorage() != null) {
            String isLoc = config.getTransfer().getIntermediateStorage();
            // Deal with extra '/'
            isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
            exportLoc = isLoc + "/" +
                    config.getTransfer().getRemoteWorkingDirectory() + "/" +
                    config.getRunMarker() + "/" +
//                    config.getTransfer().getTransferPrefix() + this.getUnique() + "_" +
                    this.getDbName() + "/" +
                    this.getName();
        } else if (config.getTransfer().getCommonStorage() != null) {
            String isLoc = config.getTransfer().getCommonStorage();
            // Deal with extra '/'
            isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
            exportLoc = isLoc + "/" + config.getTransfer().getRemoteWorkingDirectory() + "/" +
                    config.getRunMarker() + "/" +
//                    config.getTransfer().getTransferPrefix() + this.getUnique() + "_" +
                    this.getDbName() + "/" +
                    this.getName();
        } else {
            exportLoc = config.getTransfer().getExportBaseDirPrefix() + dbMirror.getName() + "/" + let.getName();
        }
        String origTableName = let.getName();
        if (isACIDDowngradeInPlace(config, let)) {
            // Rename original table.
            String newTblName = let.getName() + "_archive";
            String renameSql = MessageFormat.format(MirrorConf.RENAME_TABLE, origTableName, newTblName);
            TableUtils.changeTableName(let, newTblName);
            let.addSql(TableUtils.RENAME_TABLE, renameSql);
        }

        String exportSql = MessageFormat.format(MirrorConf.EXPORT_TABLE, let.getName(), exportLoc);
        let.addSql(TableUtils.EXPORT_TABLE, exportSql);

        // RIGHT IMPORT from Directory
        if (!isACIDDowngradeInPlace(config, let)) {
            String useRightDb = MessageFormat.format(MirrorConf.USE, database);
            ret.addSql(TableUtils.USE_DESC, useRightDb);
        }

        String importLoc = null;
        if (config.getTransfer().getIntermediateStorage() != null || config.getTransfer().getCommonStorage() != null) {
            importLoc = exportLoc;
        } else {
            importLoc = config.getCluster(Environment.LEFT).getHcfsNamespace() + exportLoc;
        }

        String sourceLocation = TableUtils.getLocation(let.getName(), let.getDefinition());
        String targetLocation = config.getTranslator().translateTableLocation(database, let.getName(), sourceLocation, config);
        String importSql;
        if (TableUtils.isACID(let.getName(), let.getDefinition())) {
            if (!config.getMigrateACID().isDowngrade()) {
                importSql = MessageFormat.format(MirrorConf.IMPORT_TABLE, let.getName(), importLoc);
            } else {
                if (config.getMigrateACID().isDowngradeInPlace()) {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, origTableName, importLoc);
                } else {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
                }
            }
        } else {
            if (config.getResetToDefaultLocation()) {
                if (config.getTransfer().getWarehouse().getExternalDirectory() != null) {
                    // Build default location, because in some cases when location isn't specified, it will use the "FROM"
                    // location in the IMPORT statement.
                    targetLocation = config.getCluster(Environment.RIGHT).getHcfsNamespace() + config.getTransfer().getWarehouse().getExternalDirectory() +
                            "/" + config.getResolvedDB(dbMirror.getName()) + ".db/" + getName();
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                } else {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
                }
            } else {
                importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
            }
        }
        if (isACIDDowngradeInPlace(config, let)) {
            let.addSql(TableUtils.IMPORT_TABLE, importSql);
        } else {
            ret.addSql(TableUtils.IMPORT_TABLE, importSql);
        }

        if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit() &&
                config.getHybrid().getExportImportPartitionLimit() > 0) {
            // The partition limit has been exceeded.  The process will need to be done manually.
            let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                    "limit (hybrid->exportImportPartitionLimit) of " + config.getHybrid().getExportImportPartitionLimit() +
                    ".  This value is used to abort migrations that have a high potential for failure.  " +
                    "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ep'.");
            rtn = Boolean.FALSE;
        } else {
            rtn = Boolean.TRUE;
        }

        return rtn;
    }

    public Boolean buildoutCOMMONSql(Config config, DBMirror dbMirror) {
       return buildoutSCHEMA_ONLYSql(config, dbMirror);
    }

    protected Boolean buildSourceToTransferSql(Config config) {
        Boolean rtn = Boolean.TRUE;
        EnvironmentTable source, transfer;
        source = getEnvironmentTable(Environment.LEFT);
        transfer = getEnvironmentTable(Environment.TRANSFER);
        if (TableUtils.isACID(source)) {
            if (source.getPartitions().size() > config.getMigrateACID().getPartitionLimit() && config.getMigrateACID().getPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                source.addIssue("The number of partitions: " + source.getPartitions().size() + " exceeds the configuration " +
                        "limit (migrateACID->partitionLimit) of " + config.getMigrateACID().getPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ap'.");
                rtn = Boolean.FALSE;
            }
        } else {
            if (source.getPartitions().size() > config.getHybrid().getSqlPartitionLimit() &&
                    config.getHybrid().getSqlPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                source.addIssue("The number of partitions: " + source.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->sqlPartitionLimit) of " + config.getHybrid().getSqlPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit. Review commandline option '-sp'.");
                rtn = Boolean.FALSE;
            }
        }

        if (isACIDDowngradeInPlace(config, source)) {
            String dropOriginalTable = MessageFormat.format(MirrorConf.DROP_TABLE,
                    source.getName());
            source.addCleanUpSql(MirrorConf.DROP_TABLE_DESC, dropOriginalTable);
        }

        if (transfer.isDefined()) {
            if (source.getPartitioned()) {
                // TODO: Check Optimizations and Cluster Version
                if (false) {

                } else {
                    String partElement = TableUtils.getPartitionElements(source);
                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                            source.getName(), transfer.getName(), partElement);
                    String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, source.getPartitions().size());
                    source.addSql(new Pair(transferDesc, transferSql));
                }
            } else {
                String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE,
                        source.getName(), transfer.getName());
                String transferDesc = MessageFormat.format(TableUtils.STAGE_TRANSFER_PARTITION_DESC, source.getPartitions().size());
                source.addSql(new Pair(transferDesc, transferSql));
            }
            // Drop Transfer Table
            if (!isACIDDowngradeInPlace(config, source)) {
                String dropTransferSql = MessageFormat.format(MirrorConf.DROP_TABLE, transfer.getName());
                source.getCleanUpSql().add(new Pair(TableUtils.DROP_SHADOW_TABLE, dropTransferSql));
            }
        }

        return rtn;
    }

    protected Boolean buildShadowToFinalSql(Config config) {
        Boolean rtn = Boolean.TRUE;
        // if common storage, skip
        // if inplace, skip
        EnvironmentTable source = getEnvironmentTable(Environment.LEFT);
        EnvironmentTable shadow = getEnvironmentTable(Environment.SHADOW);
        EnvironmentTable target = getEnvironmentTable(Environment.RIGHT);
        if ((!TableUtils.isACID(source) && config.getTransfer().getCommonStorage() != null) ||
                isACIDDowngradeInPlace(config, source)) {
            // Nothing to build.
            return rtn;
        } else {
            if (source.getPartitioned()) {
                // MSCK repair on Shadow
                String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, shadow.getName());
                target.addSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
            }
            if (config.getOptimization().getBuildShadowStatistics()) {
                // Build Shadow Stats.

            }
            // Sql from Shadow to Final
            if (source.getPartitioned()) {
                if (false) {
                    // optimizations
                } else {
                    String partElement = TableUtils.getPartitionElements(source);
                    String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
                            shadow.getName(), target.getName(), partElement);
                    String shadowDesc = MessageFormat.format(TableUtils.LOAD_FROM_PARTITIONED_SHADOW_DESC, source.getPartitions().size());
                    target.addSql(new Pair(shadowDesc, shadowSql));
                }
            } else {
                String shadowSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE,
                        shadow.getName(), target.getName());
                String shadowDesc = MessageFormat.format(TableUtils.LOAD_FROM_SHADOW_DESC, "");
                target.addSql(new Pair(shadowDesc, shadowSql));
            }
            // Drop Shadow Table.
            String dropShadowSql = MessageFormat.format(MirrorConf.DROP_TABLE, shadow.getName());
            target.getSql().add(new Pair(TableUtils.DROP_SHADOW_TABLE, dropShadowSql));

        }
        return rtn;
    }

    public Boolean buildTransferSql(EnvironmentTable source, EnvironmentTable shadow, EnvironmentTable target, Config config) {
        Boolean rtn = Boolean.TRUE;
        // Build Source->Transfer SQL
        rtn = buildSourceToTransferSql(config);

        // Build Shadow->Final SQL
        if (rtn) {
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                getEnvironmentTable(Environment.RIGHT).addSql("distcp specified", "-- Run the Distcp output to migrate data.");
                if (source.getPartitioned()) {
                    String msckTable = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, target.getName());
                    target.addCleanUpSql(new Pair(MirrorConf.MSCK_REPAIR_TABLE_DESC, msckTable));
                }
            } else if (config.getTransfer().getCommonStorage() == null) {
                rtn = buildShadowToFinalSql(config);
            }
        }
        return rtn;
    }

    /*

     */
    public Boolean buildTableSchema(CopySpec copySpec) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Config config = copySpec.getConfig();

        EnvironmentTable source = getEnvironmentTable(copySpec.getSource());
        EnvironmentTable target = getEnvironmentTable(copySpec.getTarget());

        // Set Table Name
        if (source.getExists()) {
            target.setName(source.getName());

            // Clear the target spec.
            target.getDefinition().clear();
            // Reset with Source
            target.getDefinition().addAll(getTableDefinition(copySpec.getSource()));

            if (TableUtils.isHiveNative(source)) {
                // Rules
                // 1. Strip db from create state.  It's broken anyway with the way
                //      the quotes are.  And we're setting the target db in the context anyways.
                TableUtils.stripDatabase(target);

                if (copySpec.getLocation() != null)
                    TableUtils.updateTableLocation(target, copySpec.getLocation());

                // 1. If Managed, convert to EXTERNAL
                // When coming from legacy and going to non-legacy (Hive 3).
                Boolean converted = Boolean.FALSE;
                if (!TableUtils.isACID(source)) {
                    // Non ACID tables.
                    if (copySpec.getUpgrade() && TableUtils.isManaged(source)) {
                        converted = TableUtils.makeExternal(target);
                        if (converted) {
                            target.addIssue("Schema 'converted' from LEGACY managed to EXTERNAL");
                            target.addProperty(MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG, converted.toString());
                            target.addProperty(MirrorConf.HMS_MIRROR_CONVERTED_FLAG, converted.toString());
                            if (copySpec.getTakeOwnership()) {
                                target.addProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                            } else {
                                target.addIssue("Ownership of the data not allowed in this scenario, PURGE flag NOT set.");
                            }
                        }
                    } else {
                        if (copySpec.isMakeExternal()) {
                            converted = TableUtils.makeExternal(target);
                        }
                        if (copySpec.getTakeOwnership()) {
                            if (TableUtils.isACID(source)) {
                                if (config.getMigrateACID().isDowngrade()) {
                                    target.addProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                                }
                            } else {
                                target.addProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                            }
                        }
                    }
                } else {
                    // Handle ACID tables.
                    if (copySpec.isMakeNonTransactional()) {
                        TableUtils.removeTblProperty(MirrorConf.TRANSACTIONAL, target);
                        TableUtils.removeTblProperty(MirrorConf.TRANSACTIONAL_PROPERTIES, target);
                        TableUtils.removeTblProperty(MirrorConf.BUCKETING_VERSION, target);
                    }

                    if (copySpec.isMakeExternal())
                        converted = TableUtils.makeExternal(target);

                    if (copySpec.getTakeOwnership()) {
                        if (TableUtils.isACID(source)) {
                            if (config.getMigrateACID().isDowngrade()) {
                                target.addProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                            }
                            if (copySpec.getTarget() == Environment.TRANSFER) {
                                target.addProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                            }
                        } else {
                            target.addProperty(MirrorConf.EXTERNAL_TABLE_PURGE, "true");
                        }
                    }

                    if (copySpec.getStripLocation()) {
                        if (config.getMigrateACID().isDowngrade()) {
                            target.addIssue("Location Stripped from 'Downgraded' ACID definition.  Location will be the default " +
                                    "external location as configured by the environment.");
                        } else {
                            target.addIssue("Location Stripped from ACID definition.  Location element in 'CREATE' " +
                                    "not allowed in Hive3+");
                        }
                        TableUtils.stripLocation(target);
                    }

                    if (config.getMigrateACID().isDowngrade() && copySpec.isMakeExternal()) {
//                        TableUtils.upsertTblProperty(MirrorConf.DOWNGRADED_FROM_ACID, Boolean.TRUE.toString(), target);
                        converted = TableUtils.makeExternal(target);
                        target.addProperty(MirrorConf.EXTERNAL_TABLE_PURGE, Boolean.TRUE.toString());
                        target.addProperty(MirrorConf.DOWNGRADED_FROM_ACID, Boolean.TRUE.toString());
                    }

                    if (TableUtils.removeBuckets(target, config.getMigrateACID().getArtificialBucketThreshold())) {
                        target.addIssue("Bucket Definition removed (was " + TableUtils.numOfBuckets(source) + ") because it was EQUAL TO or BELOW " +
                                "the configured 'artificialBucketThreshold' of " +
                                config.getMigrateACID().getArtificialBucketThreshold());
                    }
                }

                // 2. Set mirror stage one flag
                if (copySpec.getTarget() == Environment.RIGHT) {
                    target.addProperty(MirrorConf.HMS_MIRROR_METADATA_FLAG, df.format(new Date()));
                }

                // 3. Rename table
                if (copySpec.renameTable()) {
                    TableUtils.changeTableName(target, copySpec.getTableNamePrefix() + getName());
                }

                // 4. identify this table as being converted by hms-mirror
//        TableUtils.upsertTblProperty(MirrorConf.HMS_MIRROR_CONVERTED_FLAG, Boolean.TRUE.toString(), upperTD);

                // 5. Strip stat properties
                TableUtils.removeTblProperty("COLUMN_STATS_ACCURATE", target);
                TableUtils.removeTblProperty("numFiles", target);
                TableUtils.removeTblProperty("numRows", target);
                TableUtils.removeTblProperty("rawDataSize", target);
                TableUtils.removeTblProperty("totalSize", target);
                TableUtils.removeTblProperty("discover.partitions", target);
                TableUtils.removeTblProperty("transient_lastDdlTime", target);
                // This is control by the create, so we don't want any legacy values to
                // interrupt that.
                TableUtils.removeTblProperty("external", target);
                TableUtils.removeTblProperty("last_modified_by", target);
                TableUtils.removeTblProperty("last_modified_time", target);

                // 6. Set 'discover.partitions' if config and non-acid
                if (config.getCluster(copySpec.getTarget()).getPartitionDiscovery().getAuto() && TableUtils.isPartitioned(target.getName(), target.getDefinition())) {
                    if (converted) {
                        target.addProperty(MirrorConf.DISCOVER_PARTITIONS, Boolean.TRUE.toString());
                    } else if (TableUtils.isExternal(target)) {
                        target.addProperty(MirrorConf.DISCOVER_PARTITIONS, Boolean.TRUE.toString());
                    }
                }

                // 5. Location Adjustments
                //    Since we are looking at the same data as the original, we're not changing this now.
                //    Any changes to data location are a part of stage-2 (STORAGE).
                switch (copySpec.getTarget()) {
                    case LEFT:
                    case RIGHT:
                        if (copySpec.getReplaceLocation() && (!TableUtils.isACID(source) || config.getMigrateACID().isDowngrade())) {
                            String sourceLocation = TableUtils.getLocation(getName(), getTableDefinition(copySpec.getSource()));
                            String targetLocation = copySpec.getConfig().getTranslator().
                                    translateTableLocation(this.getDbName(), getName(), sourceLocation, copySpec.getConfig());
                            TableUtils.updateTableLocation(target, targetLocation);
                        }
                        if (copySpec.getStripLocation()) {
                            TableUtils.stripLocation(target);
                        }
                        if (config.getResetToDefaultLocation()) {
                            TableUtils.stripLocation(target);
                            target.addIssue(MessageCode.RESET_TO_DEFAULT_LOCATION_WARNING.getDesc());
                        }

                        break;
                    case SHADOW:
                    case TRANSFER:
                        if (copySpec.getLocation() != null) {
                            TableUtils.updateTableLocation(target, copySpec.getLocation());
                        } else if (copySpec.getReplaceLocation()) {
                            if (config.getTransfer().getIntermediateStorage() != null) {
                                String isLoc = config.getTransfer().getIntermediateStorage();
                                // Deal with extra '/'
                                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                                isLoc = isLoc + "/" + config.getTransfer().getRemoteWorkingDirectory() + "/" +
                                        config.getRunMarker() + "/" +
//                                        config.getTransfer().getTransferPrefix() + this.getUnique() + "_" +
                                        this.getDbName() + "/" +
                                        this.getName();
                                TableUtils.updateTableLocation(target, isLoc);
                            } else if (config.getTransfer().getCommonStorage() != null) {
                                String sourceLocation = TableUtils.getLocation(getName(), getTableDefinition(copySpec.getSource()));
                                String targetLocation = copySpec.getConfig().getTranslator().
                                        translateTableLocation(this.getDbName(), getName(), sourceLocation, copySpec.getConfig());
                                TableUtils.updateTableLocation(target, targetLocation);
                            } else if (copySpec.getStripLocation()) {
                                TableUtils.stripLocation(target);
                            } else if (config.isReplace()) {
                                String sourceLocation = TableUtils.getLocation(getName(), getTableDefinition(copySpec.getSource()));
                                String replacementLocation = sourceLocation + "_replacement";
                                TableUtils.updateTableLocation(target, replacementLocation);
                            } else {
                                // Need to use export location
                                String isLoc = config.getTransfer().getExportBaseDirPrefix();
                                // Deal with extra '/'
                                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                                isLoc = config.getCluster(Environment.LEFT).getHcfsNamespace() +
                                        isLoc + this.getDbName() + "/" + this.getName();
                                TableUtils.updateTableLocation(target, isLoc);
                            }
                        }
                        break;
                }

                switch (copySpec.getTarget()) {
                    case TRANSFER:
                        TableUtils.upsertTblProperty(MirrorConf.HMS_MIRROR_TRANSFER_TABLE, "true", target);
                        break;
                    case SHADOW:
                        TableUtils.upsertTblProperty(MirrorConf.HMS_MIRROR_SHADOW_TABLE, "true", target);
                        break;
                }
                // 6. Go through the features, if any.
                if (!config.getSkipFeatures()) {
                    for (FeaturesEnum features : FeaturesEnum.values()) {
                        Feature feature = features.getFeature();
                        LOG.debug("Table: " + getName() + " - Checking Feature: " + features);
                        if (feature.fixSchema(target)) {
                            LOG.debug("Table: " + getName() + " - Feature Applicable: " + features);
                            target.addIssue("Feature (" + features + ") was found applicable and adjustments applied. " +
                                    feature.getDescription());
                        } else {
                            LOG.debug("Table: " + getName() + " - Feature NOT Applicable: " + features);
                        }
                    }
                } else {
                    LOG.debug("Table: " + getName() + " - Skipping Features Check...");
                }

                // Add props to definition.
                if (whereTherePropsAdded(copySpec.getTarget())) {
                    Set<String> keys = target.getAddProperties().keySet();
                    for (String key : keys) {
                        TableUtils.upsertTblProperty(key, target.getAddProperties().get(key), target);
                    }
                }

                if (!copySpec.getTakeOwnership() && config.getDataStrategy() != DataStrategy.STORAGE_MIGRATION) {
                    TableUtils.removeTblProperty(MirrorConf.EXTERNAL_TABLE_PURGE, target);
                }

                if (config.getCluster(copySpec.getTarget()).getLegacyHive() && config.getDataStrategy() != DataStrategy.STORAGE_MIGRATION) {
                    // remove newer flags;
                    TableUtils.removeTblProperty(MirrorConf.EXTERNAL_TABLE_PURGE, target);
                    TableUtils.removeTblProperty(MirrorConf.DISCOVER_PARTITIONS, target);
                    TableUtils.removeTblProperty(MirrorConf.BUCKETING_VERSION, target);
                }

            } else if (TableUtils.isView(target)) {
                source.addIssue("This is a VIEW.  It will be translated AS-IS.  View transitions will NOT honor " +
                        "target db name changes For example: `-dbp`.  VIEW creation depends on the referenced tables existing FIRST. " +
                        "VIEW creation failures may mean that all referenced tables don't exist yet.");
            } else {
                // This is a connector table.  IE: HBase, Kafka, JDBC, etc.  We just past it through.
                source.addIssue("This is not a NATIVE Hive table.  It will be translated 'AS-IS'.  If the libraries or dependencies required for this table definition are not available on the target cluster, the 'create' statement may fail.");
            }

            TableUtils.fixTableDefinition(target);
        }
        return Boolean.TRUE;
    }

    public String getCreateStatement(Environment environment) {
        StringBuilder createStatement = new StringBuilder();
        List<String> tblDef = this.getTableDefinition(environment);
        if (tblDef != null) {
            Iterator<String> iter = tblDef.iterator();
            while (iter.hasNext()) {
                String line = iter.next();
                createStatement.append(line);
                if (iter.hasNext()) {
                    createStatement.append("\n");
                }
            }
        } else {
            throw new RuntimeException("Couldn't location definition for table: " + getName() +
                    " in environment: " + environment.toString());
        }
        return createStatement.toString();
    }

    public Boolean isPartitioned(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getPartitioned();
    }

    public List<String> getTableDefinition(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getDefinition();
    }

    public boolean schemasEqual(Environment one, Environment two) {
        List<String> schemaOne = getTableDefinition(one);
        List<String> schemaTwo = getTableDefinition(two);
        if (schemaOne != null && schemaTwo != null) {
            String fpOne = TableUtils.tableFieldsFingerPrint(schemaOne);
            String fpTwo = TableUtils.tableFieldsFingerPrint(schemaTwo);
            if (fpOne.equals(fpTwo)) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        } else {
            return Boolean.FALSE;
        }
    }

    public void setTableDefinition(Environment environment, List<String> tableDefList) {
        EnvironmentTable et = getEnvironmentTable(environment);
        et.setDefinition(tableDefList);
    }

    public List<String> getPartitionDefinition(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getPartitions();
    }

    public void addTableAction(Environment environment, String action) {
        List<String> tableActions = getTableActions(environment);
        tableActions.add(action);
    }

    public List<String> getTableActions(Environment environment) {
        EnvironmentTable et = getEnvironmentTable(environment);
        return et.getActions();
    }

}
